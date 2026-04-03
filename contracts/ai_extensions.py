#!/usr/bin/env python3
"""
Phase 4 — AI contract extensions: embedding drift, prompt input validation, output schema violation rate.

- **Embedding drift**: OpenAI when ``OPENAI_API_KEY`` is set, else deterministic **local**
  character-distribution vectors (real data, no API required).
- **Prompt schema**: JSON Schema validation on reconstructed prompt payloads from extractions.
- **Output schema rate**: ``recommendation`` enum when present; else **event_type** against an
  allow-list learned on first baseline (detects new event types on later runs).

Rising output violation rate or drift/prompt WARN/FAIL appends **WARN** rows to
``violation_log/violations.jsonl`` when ``--violation-log`` is set (default: repo path).

Usage:
  python contracts/ai_extensions.py \\
    --mode all \\
    --extractions outputs/migrate/migrated_extractions.jsonl \\
    --verdicts outputs/migrate/migrated_events.jsonl \\
    --output validation_reports/ai_extensions.json

Drift demo (clean verdicts → establish baseline, then dirty file):
  python contracts/ai_extensions.py --mode output_rate --verdicts outputs/ai_demo/verdicts_clean.jsonl
  python contracts/ai_extensions.py --mode output_rate --verdicts outputs/ai_demo/verdicts_dirty.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent

EMBEDDING_BASELINE_DIR_DEFAULT = "schema_snapshots/embedding_baselines"
LATEST_EMBEDDING_NPZ = "latest.npz"
LEGACY_EMBEDDING_NPZ = REPO_ROOT / "schema_snapshots" / "embedding_baselines.npz"

OUTPUT_RATE_BASELINE = "schema_snapshots/ai_output_rate_baseline.json"

RECOMMENDATION_ALLOWED = ("APPROVE", "REFER", "DECLINE")

# Relaxed doc_id for legacy Week 3 hex ids and migrated UUIDs (prompt-input envelope).
WEEK3_PROMPT_INPUT_SCHEMA: Dict[str, Any] = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "required": ["doc_id", "source_path", "content_preview"],
    "properties": {
        "doc_id": {"type": "string", "minLength": 8, "maxLength": 64},
        "source_path": {"type": "string", "minLength": 1},
        "content_preview": {"type": "string", "maxLength": 8000},
    },
    "additionalProperties": False,
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _resolve_path(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else REPO_ROOT / p


def _fetch_openai_embeddings(texts: List[str]) -> np.ndarray:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set; cannot call embeddings API")
    resp = requests.post(
        "https://api.openai.com/v1/embeddings",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={"model": "text-embedding-3-small", "input": texts},
        timeout=120,
    )
    resp.raise_for_status()
    data = resp.json()
    items = sorted(data.get("data") or [], key=lambda x: x.get("index", 0))
    vectors = [item["embedding"] for item in items]
    return np.array(vectors, dtype=np.float64)


def _local_text_embedding_matrix(texts: List[str]) -> np.ndarray:
    """
    Fixed 256-D bag-of-bytes histogram per text, L2-normalized (no network; reproducible drift).
    """
    rows: List[np.ndarray] = []
    for t in texts:
        v = np.zeros(256, dtype=np.float64)
        b = t.encode("utf-8", errors="ignore")[:50_000]
        for c in b:
            v[c % 256] += 1.0
        n = float(np.linalg.norm(v))
        rows.append(v / (n + 1e-9))
    return np.array(rows, dtype=np.float64)


def _embed_texts(texts: List[str], backend: str) -> np.ndarray:
    b = (backend or "auto").lower()
    if b == "openai":
        return _fetch_openai_embeddings(texts)
    if b == "local":
        return _local_text_embedding_matrix(texts)
    # auto
    if os.environ.get("OPENAI_API_KEY"):
        return _fetch_openai_embeddings(texts)
    return _local_text_embedding_matrix(texts)


def check_embedding_drift(
    texts: List[str],
    baseline_dir: str = EMBEDDING_BASELINE_DIR_DEFAULT,
    threshold: float = 0.15,
    min_sample_for_baseline: int = 10,
    *,
    backend: str = "auto",
) -> dict:
    """
    Compare embedding centroid of a text sample to the latest saved baseline (cosine drift).

    ``backend``: ``auto`` (OpenAI if key, else local), ``openai``, or ``local``.
    """
    if len(texts) < min_sample_for_baseline:
        return {
            "status": "INSUFFICIENT_DATA",
            "drift_score": None,
            "embedding_backend": backend,
            "message": f"Need >= {min_sample_for_baseline} text samples, got {len(texts)}",
        }

    random.seed(42)
    k = min(200, len(texts))
    sample = random.sample(texts, k)

    d = _resolve_path(baseline_dir)
    d.mkdir(parents=True, exist_ok=True)
    latest = d / LATEST_EMBEDDING_NPZ
    if LEGACY_EMBEDDING_NPZ.is_file() and not latest.is_file():
        shutil.copy2(LEGACY_EMBEDDING_NPZ, latest)

    used_backend = "openai" if backend == "openai" else ("openai" if backend == "auto" and os.environ.get("OPENAI_API_KEY") else "local")
    try:
        embeddings = _embed_texts(sample, backend)
    except Exception as exc:
        if backend == "auto" and os.environ.get("OPENAI_API_KEY"):
            embeddings = _local_text_embedding_matrix(sample)
            used_backend = "local_fallback"
        else:
            return {
                "status": "ERROR",
                "embedding_backend": backend,
                "message": str(exc),
            }

    centroid = np.mean(embeddings, axis=0)
    dim_note = int(centroid.shape[0]) if centroid.ndim else 0

    if not latest.is_file():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        archive = d / f"centroid_{stamp}.npz"
        np.savez(
            str(archive),
            centroid=centroid,
            established_at=utc_now_iso(),
            embedding_backend=used_backend,
            dim=dim_note,
        )
        shutil.copy2(archive, latest)
        return {
            "status": "BASELINE_SET",
            "drift_score": 0.0,
            "embedding_backend": used_backend,
            "vector_dim": dim_note,
            "samples_used": len(sample),
            "baseline_archive": str(archive.relative_to(REPO_ROOT)),
            "baseline_latest": str(latest.relative_to(REPO_ROOT)),
            "message": "Baseline established. Run again on new data to measure drift.",
        }

    loaded = np.load(str(latest), allow_pickle=False)
    baseline_centroid = np.asarray(loaded["centroid"], dtype=np.float64)
    cur = np.asarray(centroid, dtype=np.float64)
    if cur.shape != baseline_centroid.shape:
        return {
            "status": "WARN",
            "drift_score": None,
            "embedding_backend": used_backend,
            "message": (
                f"Baseline dim {baseline_centroid.shape} != current {cur.shape}; "
                "delete latest.npz or use a fresh baseline_dir."
            ),
        }
    denom = float(np.linalg.norm(cur) * np.linalg.norm(baseline_centroid) + 1e-9)
    cosine_sim = float(np.dot(cur, baseline_centroid) / denom)
    drift = 1.0 - cosine_sim
    fail = drift > threshold
    return {
        "status": "FAIL" if fail else "PASS",
        "drift_score": round(float(drift), 4),
        "threshold": threshold,
        "embedding_backend": used_backend,
        "vector_dim": dim_note,
        "samples_used": len(sample),
        "baseline_latest": str(latest.relative_to(REPO_ROOT)),
        "interpretation": (
            "semantic / lexical distribution shifted vs baseline — investigate drift"
            if fail
            else "stable within threshold"
        ),
    }


def validate_prompt_inputs(
    records: List[dict],
    schema: dict,
    quarantine_path: str = "outputs/quarantine/",
) -> dict:
    """
    Validate each record with jsonschema; append failures to quarantine JSONL.
    """
    try:
        from jsonschema import Draft7Validator, ValidationError
    except ImportError as e:
        return {
            "valid_count": 0,
            "quarantined_count": len(records),
            "quarantine_path": None,
            "status": "ERROR",
            "message": f"jsonschema not available: {e}",
        }

    validator = Draft7Validator(schema)
    valid: List[dict] = []
    quarantined: List[dict] = []

    for r in records:
        if not isinstance(r, dict):
            quarantined.append(
                {
                    "record": r,
                    "error": "Record is not a JSON object",
                    "path": [],
                }
            )
            continue
        try:
            validator.validate(r)
            valid.append(r)
        except ValidationError as e:
            quarantined.append(
                {
                    "record": r,
                    "error": e.message,
                    "path": list(e.path),
                }
            )

    qdir = _resolve_path(quarantine_path)
    out_path: Optional[str] = None
    if quarantined:
        qdir.mkdir(parents=True, exist_ok=True)
        qfile = qdir / "quarantine.jsonl"
        with qfile.open("a", encoding="utf-8") as fq:
            for entry in quarantined:
                fq.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        out_path = str(qfile.relative_to(REPO_ROOT))

    return {
        "valid_count": len(valid),
        "quarantined_count": len(quarantined),
        "quarantine_path": out_path,
        "status": "PASS" if len(quarantined) == 0 else "WARN",
    }


def _metric_field_and_values(outputs: List[dict]) -> Tuple[str, List[Any]]:
    """Prefer LLM ``recommendation`` when populated; else Week-5-style ``event_type``."""
    n = len(outputs)
    if n == 0:
        return "recommendation", []
    rec_n = sum(
        1 for r in outputs if isinstance(r, dict) and r.get("recommendation") is not None
    )
    if rec_n >= max(3, min(n, n // 4 or 1)):
        return "recommendation", [r.get("recommendation") if isinstance(r, dict) else None for r in outputs]
    return "event_type", [r.get("event_type") if isinstance(r, dict) else None for r in outputs]


def check_output_violation_rate(
    outputs: List[dict],
    baseline_path: str = OUTPUT_RATE_BASELINE,
    warn_threshold: float = 0.05,
) -> dict:
    """
    Tracks **violation_rate** metric: share of rows whose field value is outside the allow-list.

    - ``recommendation``: closed world {APPROVE, REFER, DECLINE}.
    - ``event_type``: allow-list is **frozen** on first baseline write from that batch; later
      files with *new* types count as violations (real drift demo across two JSONL exports).
    """
    total = len(outputs)
    field, values = _metric_field_and_values(outputs)
    path = _resolve_path(baseline_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    stored: Dict[str, Any] = {}
    if path.is_file():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                stored = raw
        except (OSError, json.JSONDecodeError):
            stored = {}

    if stored.get("metric_field") and stored.get("metric_field") != field:
        stored = {}

    is_first = not path.is_file() or stored.get("baseline_rate") is None

    if field == "recommendation":
        allowed_set = set(RECOMMENDATION_ALLOWED)
    elif is_first and total > 0:
        allowed_set = {str(v).strip() for v in values if v is not None and str(v).strip()}
    else:
        allowed_set = {str(x).strip() for x in (stored.get("allowed_event_types") or []) if str(x).strip()}

    violations = sum(1 for v in values if v is None or str(v).strip() not in allowed_set)

    rate = violations / max(total, 1)
    history: List[dict] = list(stored.get("rate_history") or [])
    history.append(
        {
            "run_at": utc_now_iso(),
            "metric_field": field,
            "violation_rate": round(rate, 6),
            "violations": violations,
            "total": total,
        }
    )
    history = history[-24:]

    if is_first:
        payload = {
            "baseline_rate": round(rate, 6),
            "metric_field": field,
            "allowed_event_types": sorted(allowed_set) if field == "event_type" else list(RECOMMENDATION_ALLOWED),
            "rate_history": history,
            "updated_at": utc_now_iso(),
        }
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return {
            "metric_field": field,
            "total_outputs": total,
            "schema_violations": violations,
            "violation_rate": round(rate, 4),
            "violation_rate_metric": round(rate, 4),
            "baseline_rate": round(rate, 4),
            "allowed_values_count": len(allowed_set),
            "trend": "baseline_initialized",
            "status": "BASELINE_SET",
            "warn_threshold": warn_threshold,
            "message": "Baseline saved. Re-run on a later batch; new event_type values or bad verdicts increase the metric.",
        }

    try:
        eff_baseline_f = float(stored.get("baseline_rate"))
    except (TypeError, ValueError):
        eff_baseline_f = 0.0

    if field == "recommendation":
        allowed_set = set(RECOMMENDATION_ALLOWED)
    else:
        allowed_set = {str(x) for x in (stored.get("allowed_event_types") or [])}

    violations = sum(
        1
        for v in values
        if v is None or str(v).strip() not in allowed_set
    )
    rate = violations / max(total, 1)

    if eff_baseline_f <= 1e-9 and rate > warn_threshold:
        trend = "rising"
    elif eff_baseline_f > 1e-9 and rate > eff_baseline_f * 1.5:
        trend = "rising"
    elif eff_baseline_f > 1e-9 and rate < eff_baseline_f * 0.5:
        trend = "falling"
    else:
        trend = "stable"

    status = "WARN" if (trend == "rising" or rate > warn_threshold) else "PASS"

    payload = {
        "baseline_rate": eff_baseline_f,
        "metric_field": field,
        "allowed_event_types": sorted(allowed_set) if field == "event_type" else list(RECOMMENDATION_ALLOWED),
        "last_rate": round(rate, 6),
        "rate_history": history,
        "updated_at": utc_now_iso(),
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return {
        "metric_field": field,
        "total_outputs": total,
        "schema_violations": violations,
        "violation_rate": round(rate, 4),
        "violation_rate_metric": round(rate, 4),
        "baseline_rate": round(eff_baseline_f, 4),
        "trend": trend,
        "status": status,
        "warn_threshold": warn_threshold,
        "message": (
            "Output schema violation rate elevated or trending up vs baseline — review LLM post-processing or producers."
            if status == "WARN"
            else "Violation rate within expectations."
        ),
    }


def load_jsonl(path: Path) -> List[dict]:
    if not path.is_file():
        return []
    out: List[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                out.append(obj)
        except json.JSONDecodeError:
            continue
    return out


def extract_embedding_texts(records: List[dict]) -> List[str]:
    texts: List[str] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        for fact in r.get("extracted_facts") or []:
            if isinstance(fact, dict):
                t = fact.get("text")
                if isinstance(t, str) and t.strip():
                    texts.append(t.strip())
        if not any(
            isinstance(fact, dict) and (fact.get("text") or "").strip()
            for fact in (r.get("extracted_facts") or [])
        ):
            for key in ("document_name", "source_path", "notes", "strategy_sequence"):
                v = r.get(key)
                if isinstance(v, str) and v.strip():
                    texts.append(v.strip())
                elif isinstance(v, list) and v:
                    texts.append(json.dumps(v, sort_keys=True)[:4000])
    return texts


def build_prompt_inputs(records: List[dict]) -> List[dict]:
    out: List[dict] = []
    for r in records:
        if not isinstance(r, dict):
            continue
        doc_id = r.get("doc_id", "")
        src = r.get("source_path", "") or ""
        name = r.get("document_name", "") or ""
        if not isinstance(doc_id, str):
            doc_id = str(doc_id)
        if not isinstance(src, str):
            src = str(src)
        if not isinstance(name, str):
            name = str(name)
        prev = name[:8000] if name else (src[:8000] if src else json.dumps(r, default=str)[:8000])
        out.append(
            {
                "doc_id": doc_id,
                "source_path": src or "(unknown)",
                "content_preview": prev,
            }
        )
    return out


def run_embedding_check(extractions_path: Path, backend: str = "auto") -> dict:
    records = load_jsonl(extractions_path)
    texts = extract_embedding_texts(records)
    return check_embedding_drift(texts, backend=backend)


def run_prompt_check(extractions_path: Path) -> dict:
    records = load_jsonl(extractions_path)
    prompt_inputs = build_prompt_inputs(records)
    return validate_prompt_inputs(prompt_inputs, WEEK3_PROMPT_INPUT_SCHEMA)


def run_output_rate_check(
    verdicts_path: Path,
    baseline_path: str = OUTPUT_RATE_BASELINE,
) -> dict:
    records = load_jsonl(verdicts_path)
    return check_output_violation_rate(records, baseline_path=baseline_path)


def _should_log_ai_violation(section: dict) -> bool:
    if not isinstance(section, dict):
        return False
    st = str(section.get("status") or "").upper()
    if st in ("FAIL", "WARN", "WARNING", "ERROR"):
        return True
    if section.get("trend") == "rising":
        return True
    return False


def append_ai_violations_to_log(
    result: Dict[str, Any],
    violation_log: Path,
) -> int:
    """
    Append structured WARN/FAIL rows for rubric: rising output rate → violation log.
    """
    violation_log.parent.mkdir(parents=True, exist_ok=True)
    ts = str(result.get("run_at") or utc_now_iso())
    n = 0

    def _line(check_id: str, message: str, metric: dict, records_failing: int) -> dict:
        return {
            "violation_id": str(uuid.uuid4()),
            "source": "ai_extensions",
            "check_id": check_id,
            "detected_at": ts,
            "status": "WARN",
            "severity": "MEDIUM",
            "message": message,
            "metric_snapshot": metric,
            "blast_radius": {
                "source": "ai_pipeline",
                "affected_pipelines": ["llm-postprocess", "event-producers"],
                "estimated_records": records_failing,
            },
            "blame_chain": [
                {
                    "rank": 1,
                    "file_path": str(Path("contracts/ai_extensions.py").as_posix()),
                    "commit_hash": "",
                    "author": "",
                    "commit_timestamp": "",
                    "commit_message": "Inspect ai_extensions report and baseline files under schema_snapshots/",
                    "confidence_score": 0.55,
                }
            ],
            "records_failing": records_failing,
        }

    od = result.get("output_violation_rate")
    if isinstance(od, dict) and (
        od.get("trend") == "rising" or str(od.get("status")).upper() == "WARN"
    ):
        vf = int(od.get("schema_violations") or 0)
        msg = (
            f"AI output schema violation rate WARN: trend={od.get('trend')!r} "
            f"rate={od.get('violation_rate')} baseline={od.get('baseline_rate')} "
            f"field={od.get('metric_field')}"
        )
        with violation_log.open("a", encoding="utf-8") as fq:
            fq.write(
                json.dumps(
                    _line("ai_extensions.output_violation_rate", msg, od, vf),
                    ensure_ascii=False,
                )
                + "\n"
            )
        n += 1

    for key, check_id in (
        ("embedding_drift", "ai_extensions.embedding_drift"),
        ("prompt_validation", "ai_extensions.prompt_input_schema"),
    ):
        sec = result.get(key)
        if isinstance(sec, dict) and _should_log_ai_violation(sec):
            msg = f"{key}: {sec.get('status')} — {sec.get('message') or sec.get('interpretation') or ''}"
            fails = int(sec.get("quarantined_count") or sec.get("samples_used") or 0)
            with violation_log.open("a", encoding="utf-8") as fq:
                fq.write(
                    json.dumps(
                        _line(check_id, msg[:500], sec, fails),
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            n += 1

    return n


def main() -> None:
    parser = argparse.ArgumentParser(description="AI contract extensions (Phase 4)")
    parser.add_argument(
        "--mode",
        choices=["all", "embedding", "prompt", "output_rate"],
        default="all",
        help="Which checks to run",
    )
    parser.add_argument(
        "--extractions",
        type=str,
        default=None,
        help="Week 3 extractions JSONL (embedding + prompt checks)",
    )
    parser.add_argument(
        "--verdicts",
        type=str,
        default=None,
        help="Events or LLM verdicts JSONL (output violation rate)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="validation_reports/ai_extensions.json",
        help="Combined JSON report path",
    )
    parser.add_argument(
        "--embedding-backend",
        choices=["auto", "openai", "local"],
        default="auto",
        help="auto uses OpenAI if OPENAI_API_KEY is set, else local histogram embeddings",
    )
    parser.add_argument(
        "--violation-log",
        type=str,
        default=str(REPO_ROOT / "violation_log" / "violations.jsonl"),
        help="Append WARN rows for rising output rate / extension failures (JSONL)",
    )
    parser.add_argument(
        "--no-violation-log",
        action="store_true",
        help="Do not append to violation_log/violations.jsonl",
    )
    parser.add_argument(
        "--output-rate-baseline",
        type=str,
        default=OUTPUT_RATE_BASELINE,
        help="JSON state for output violation-rate baseline (use a separate path for demos)",
    )
    args = parser.parse_args()

    mode = args.mode
    out_path = _resolve_path(args.output)
    vlog = None if args.no_violation_log else _resolve_path(args.violation_log)

    need_ext = mode in ("all", "embedding", "prompt")
    need_ver = mode in ("all", "output_rate")

    if need_ext and not args.extractions:
        print("ERROR: --extractions is required for this --mode", file=sys.stderr)
        sys.exit(2)
    if need_ver and not args.verdicts:
        print("ERROR: --verdicts is required for this --mode", file=sys.stderr)
        sys.exit(2)

    ext_path = _resolve_path(args.extractions) if args.extractions else None
    ver_path = _resolve_path(args.verdicts) if args.verdicts else None

    result: Dict[str, Any] = {
        "run_at": utc_now_iso(),
        "embedding_drift": None,
        "prompt_validation": None,
        "output_violation_rate": None,
        "embedding_backend_requested": args.embedding_backend,
    }

    if mode in ("all", "embedding"):
        try:
            assert ext_path is not None
            result["embedding_drift"] = run_embedding_check(ext_path, backend=args.embedding_backend)
        except Exception as e:
            result["embedding_drift"] = {"status": "ERROR", "message": str(e)}

    if mode in ("all", "prompt"):
        try:
            assert ext_path is not None
            result["prompt_validation"] = run_prompt_check(ext_path)
        except Exception as e:
            result["prompt_validation"] = {"status": "ERROR", "message": str(e)}

    if mode in ("all", "output_rate"):
        try:
            assert ver_path is not None
            result["output_violation_rate"] = run_output_rate_check(
                ver_path, baseline_path=args.output_rate_baseline
            )
        except Exception as e:
            result["output_violation_rate"] = {"status": "ERROR", "message": str(e)}

    ov = result.get("output_violation_rate")
    vr_metric = None
    if isinstance(ov, dict) and ov.get("violation_rate_metric") is not None:
        try:
            vr_metric = float(ov["violation_rate_metric"])
        except (TypeError, ValueError):
            vr_metric = None
    elif isinstance(ov, dict) and ov.get("violation_rate") is not None:
        try:
            vr_metric = float(ov["violation_rate"])
        except (TypeError, ValueError):
            vr_metric = None

    result["summary"] = {
        "extensions_run": [k for k in ("embedding_drift", "prompt_validation", "output_violation_rate") if result.get(k) is not None],
        "all_three_in_all_mode": mode == "all",
        "violation_rate_metric": vr_metric,
        "output_trend": (ov or {}).get("trend") if isinstance(ov, dict) else None,
    }

    log_n = 0
    if vlog is not None:
        log_n = append_ai_violations_to_log(result, vlog)
    result["summary"]["violation_log_events_appended"] = log_n

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {out_path}", file=sys.stderr)
    if vlog and log_n:
        print(f"Appended {log_n} AI extension row(s) to {vlog}", file=sys.stderr)


if __name__ == "__main__":
    main()
