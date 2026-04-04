#!/usr/bin/env python3
"""
ContractGenerator (Phase 1) — entry point.

Reads **migrated** JSONL under outputs/migrate/ (Week 3, 4, 5, LangSmith traces)
plus the Week 4 lineage snapshot for downstream context injection.

Pipeline:
  1. Structural profiling (ydata-profiling + privacy-preserving column shapes)
  2. Statistical profiling (numeric summaries; confidence 0–1 distribution flags; malformed rows
     logged and excluded from baselines where coerced)
  3. Lineage context from latest migrated lineage snapshot
  4. Optional LLM annotations via OpenRouter — **schema metadata only** (no raw row values)
  5. Parallel dbt schema files (Challenge Week 7 Phase 1 Step 5: generated_contracts/{name}_dbt.yml)
  6. Timestamped schema snapshots for Phase 3 (schema_snapshots/{contract_id}/{timestamp}.yaml)

Environment:
  OPENROUTER_API_KEY — enable step 4
  OPENROUTER_MODEL   — default openai/gpt-4o-mini
  OPENROUTER_HTTP_REFERER — optional, for OpenRouter rankings

Usage:
  python contracts/generator.py
  python contracts/generator.py --no-llm
  python contracts/generator.py \\
    --source outputs/week3/extractions.jsonl \\
    --contract-id week3-document-refinery-extractions \\
    --lineage outputs/week4/lineage_snapshots.jsonl \\
    --registry contract_registry/subscriptions.yaml \\
    --output generated_contracts/
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import logging
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import yaml

from schema_evolution import write_contract_schema_snapshot
from validation_checks import flatten_extractions_for_profile

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from contracts.registry_loader import load_registry

logger = logging.getLogger(__name__)

# Log profiling/coercion issues to stderr when generator runs as __main__ (see main()).

GENERATED = REPO_ROOT / "generated_contracts"

_REGISTRY_LINEAGE_NOTE_OK = (
    "registry_subscribers is the authoritative blast radius source. "
    "downstream_nodes is lineage enrichment only."
)
_REGISTRY_LINEAGE_NOTE_MISSING = "registry not provided"

# Authoritative inputs: migrated artifacts (post data-contract migration).
MIGRATED = {
    "week3": REPO_ROOT / "outputs/migrate/migrated_extractions.jsonl",
    "week4": REPO_ROOT / "outputs/migrate/migrated_lineage_snapshots.jsonl",
    "week5": REPO_ROOT / "outputs/migrate/migrated_events.jsonl",
    "traces": REPO_ROOT / "outputs/migrate/migrated_runs.jsonl",
}

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------


def load_jsonl_records(path: Path, *, lenient: bool = False) -> List[dict]:
    """
    Load records from JSONL, or a single JSON array/object, or Python-literal lines
    (e.g. single-quoted keys) after strict JSON fails — common for notebook exports.

    With ``lenient=True``, malformed lines are skipped and logged instead of raising;
    use for profiling so one bad row does not abort the whole generator run.
    """
    records, _issues = load_jsonl_records_with_issues(path, lenient=lenient)
    return records


def load_jsonl_records_with_issues(
    path: Path, *, lenient: bool = False
) -> Tuple[List[dict], List[Dict[str, Any]]]:
    """
    Load JSONL-like inputs; return ``(records, issues)``.

    Each issue: ``{"line": int|None, "kind": str, "detail": str}``.
    In strict mode, raises ``ValueError`` on first unrecoverable line (after logging).
    """
    issues: List[Dict[str, Any]] = []
    if not path.is_file():
        return [], issues

    raw = path.read_text(encoding="utf-8")
    if raw.startswith("\ufeff"):
        raw = raw[1:]
    text = raw.strip()
    if not text:
        return [], issues

    def _record_issue(line_no: Optional[int], kind: str, detail: str) -> None:
        rec = {"line": line_no, "kind": kind, "detail": detail[:500]}
        issues.append(rec)
        msg = f"{path}"
        if line_no is not None:
            msg += f":{line_no}"
        msg += f" [{kind}] {detail[:300]}"
        logger.warning("ContractGenerator JSONL: %s", msg)

    out: List[dict] = []

    # One JSON document: array of objects or a single object (incl. pretty-printed).
    try:
        data = json.loads(text)
        if isinstance(data, list):
            for i, x in enumerate(data):
                if isinstance(x, dict):
                    out.append(x)
                else:
                    _record_issue(
                        None,
                        "skip_non_object_in_array",
                        f"index {i}: expected object, got {type(x).__name__}",
                    )
            return out, issues
        if isinstance(data, dict):
            return [data], issues
    except json.JSONDecodeError:
        pass

    for lineno, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            val = json.loads(line)
        except json.JSONDecodeError:
            try:
                val = ast.literal_eval(line)
            except (ValueError, SyntaxError) as e:
                detail = f"not valid JSON/JSONL (or Python dict literal): {e}"
                if lenient:
                    _record_issue(lineno, "malformed_line", detail)
                    continue
                raise ValueError(f"{path}: line {lineno}: {detail}") from e
        if isinstance(val, dict):
            out.append(val)
        elif isinstance(val, list):
            for j, x in enumerate(val):
                if isinstance(x, dict):
                    out.append(x)
                else:
                    _record_issue(
                        lineno,
                        "skip_non_object_in_line_array",
                        f"element {j}: {type(x).__name__}",
                    )
        else:
            detail = f"expected object or array of objects, got {type(val).__name__}"
            if lenient:
                _record_issue(lineno, "unexpected_line_type", detail)
                continue
            raise ValueError(f"{path}: line {lineno}: {detail}")

    return out, issues


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_optional_registry_path(registry: Optional[str]) -> Optional[Path]:
    """Return absolute path to registry file if ``registry`` is set and the file exists."""
    if registry is None or not str(registry).strip():
        return None
    p = Path(registry)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p if p.is_file() else None


def enrich_contract_lineage_from_registry(
    contract: dict,
    contract_id: str,
    registry_path: Optional[Path],
) -> None:
    """
    After the contract dict is built (and lineage downstream/upstream set), add
    ``registry_subscribers`` and ``note`` under ``lineage`` before YAML write.
    """
    contract.setdefault("lineage", {})
    lin = contract["lineage"]
    if registry_path is not None and registry_path.is_file():
        registry = load_registry(str(registry_path))
        subs = registry.get("subscriptions") or []
        matching = [
            s
            for s in subs
            if isinstance(s, dict) and str(s.get("contract_id", "")) == contract_id
        ]
        out_ids: List[str] = []
        for s in matching:
            sid = s.get("subscriber_id")
            if sid is None:
                continue
            t = str(sid).strip()
            if t:
                out_ids.append(t)
        lin["registry_subscribers"] = out_ids
        lin["note"] = _REGISTRY_LINEAGE_NOTE_OK
    else:
        lin["registry_subscribers"] = []
        lin["note"] = _REGISTRY_LINEAGE_NOTE_MISSING


# ---------------------------------------------------------------------------
# Privacy-preserving value shapes (safe for YAML; never sent to LLM as raw PII)
# ---------------------------------------------------------------------------


def value_shape_signature(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        h = hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]
        return f"string(len={len(value)},sha8={h})"
    if isinstance(value, (list, dict)):
        raw = json.dumps(value, sort_keys=True, default=str)
        h = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
        return f"{type(value).__name__}(items≈{len(value)},sha8={h})"
    return type(value).__name__


def distinct_shape_samples(series: pd.Series, k: int = 5) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in series.dropna().unique():
        sig = value_shape_signature(v if not hasattr(v, "item") else v)
        if sig not in seen:
            seen.add(sig)
            out.append(sig)
        if len(out) >= k:
            break
    return out


def dominant_string_pattern(series: pd.Series) -> str:
    s = series.dropna().astype(str)
    if s.empty:
        return "n/a"
    labels: List[str] = []
    for v in s.head(800):
        if re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", v, re.I):
            labels.append("uuid_like")
        elif re.fullmatch(r"[0-9a-f]{64}", v, re.I):
            labels.append("hex64_like")
        elif re.fullmatch(r"\d+", v):
            labels.append("numeric_string")
        elif re.fullmatch(r"\d{4}-\d{2}-\d{2}T", v[:11]):
            labels.append("iso8601_prefix")
        else:
            labels.append("free_text")
    if not labels:
        return "n/a"
    return Counter(labels).most_common(1)[0][0]


# ---------------------------------------------------------------------------
# Step 1–2: profiling
# ---------------------------------------------------------------------------


def ydata_profile_summary(df: pd.DataFrame, dataset_id: str) -> Dict[str, Any]:
    """Run ydata-profiling (minimal) and return a compact, reviewable summary."""
    if df.empty:
        return {"status": "skipped_empty_dataframe", "dataset": dataset_id}
    try:
        from ydata_profiling import ProfileReport

        report = ProfileReport(df, minimal=True, title=dataset_id, progress_bar=False)
        desc_obj = report.get_description()
    except Exception as exc:  # pragma: no cover - optional dependency edge cases
        return {"status": "ydata_failed", "error": str(exc), "dataset": dataset_id}

    if isinstance(desc_obj, dict):
        desc = desc_obj
    elif hasattr(desc_obj, "model_dump"):
        desc = desc_obj.model_dump()
    elif hasattr(desc_obj, "dict"):
        desc = desc_obj.dict()
    else:
        desc = {}

    variables = desc.get("variables") or {}
    alerts = desc.get("alerts")
    if not isinstance(alerts, list):
        alerts = []
    return {
        "status": "ok",
        "dataset": dataset_id,
        "alerts": alerts[:20],
        "variable_count": len(variables),
    }


def structural_column_profiles(df: pd.DataFrame) -> List[Dict[str, Any]]:
    cols: List[Dict[str, Any]] = []
    for name in df.columns:
        s = df[name]
        null_frac = float(s.isna().mean()) if len(df) else 1.0
        card = int(s.nunique(dropna=True))
        prof: Dict[str, Any] = {
            "name": name,
            "dtype": str(s.dtype),
            "null_fraction": round(null_frac, 6),
            "cardinality": card,
            "distinct_shape_samples": distinct_shape_samples(s, 5),
        }
        if s.dtype == object or pd.api.types.is_string_dtype(s):
            prof["dominant_character_pattern"] = dominant_string_pattern(s)
        cols.append(prof)
    return cols


def numeric_stats(series: pd.Series, *, column_label: str = "") -> Optional[Dict[str, Any]]:
    """
    Baseline-friendly numeric summaries. Non-numeric cells are coerced with ``errors='coerce'``;
    coercions are counted and logged so malformed strings cannot silently skew stats.
    """
    label = column_label or series.name or "column"
    sn_all = pd.to_numeric(series, errors="coerce")
    # Non-null input that became NaN after coerce = unparsable "numeric" garbage
    mask_junk = series.notna() & sn_all.isna()
    n_junk = int(mask_junk.sum())
    if n_junk > 0:
        logger.warning(
            "ContractGenerator numeric_stats(%s): dropped %d non-coercible non-null cells before stats",
            label,
            n_junk,
        )
    sn = sn_all.dropna()
    inf_mask = (sn == float("inf")) | (sn == float("-inf"))
    inf_ct = int(inf_mask.sum()) if len(sn) else 0
    if inf_ct:
        logger.warning(
            "ContractGenerator numeric_stats(%s): filtering %d inf/-inf values before quantiles",
            label,
            inf_ct,
        )
        sn = sn.mask(inf_mask).dropna()
    if sn.empty:
        return None
    stddev = float(sn.std(ddof=0)) if len(sn) > 1 else 0.0
    out: Dict[str, Any] = {
        "min": float(sn.min()),
        "max": float(sn.max()),
        "mean": float(sn.mean()),
        "p25": float(sn.quantile(0.25)),
        "p50": float(sn.quantile(0.50)),
        "p75": float(sn.quantile(0.75)),
        "p95": float(sn.quantile(0.95)),
        "p99": float(sn.quantile(0.99)),
        "stddev": stddev,
    }
    if n_junk or inf_ct:
        out["non_numeric_or_non_finite_dropped"] = n_junk + inf_ct
    return out


def dataframe_numeric_profile(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Per-column numeric_stats for a frame (coercion-safe; logs junk cells)."""
    out: Dict[str, Dict[str, Any]] = {}
    for c in df.columns:
        ns = numeric_stats(df[c], column_label=str(c))
        if ns:
            out[str(c)] = ns
    return out


def confidence_distribution_flags(
    series: pd.Series, *, column_label: str = "primary_fact_confidence"
) -> Dict[str, Any]:
    n_in = int(series.notna().sum())
    sn = pd.to_numeric(series, errors="coerce")
    dropped = int((series.notna() & sn.isna()).sum())
    if dropped:
        logger.warning(
            "ContractGenerator confidence_distribution_flags(%s): %d non-null values failed numeric coerce "
            "(excluded from min/max/mean)",
            column_label,
            dropped,
        )
    sn = sn.dropna()
    if sn.empty:
        return {
            "status": "no_numeric_confidence_samples",
            "non_numeric_non_null_dropped": dropped,
            "non_null_input_cells": n_in,
        }
    mn, mx, mean = float(sn.min()), float(sn.max()), float(sn.mean())
    flags: List[str] = []
    if mn < 0.0 or mx > 1.0:
        flags.append("OUTSIDE_0_1_RANGE")
    if mean > 0.99:
        flags.append("MEAN_GT_0_99_CLAMP_SUSPECT")
    if mean < 0.01:
        flags.append("MEAN_LT_0_01_BROKEN_SUSPECT")
    return {
        "status": "ok",
        "min": mn,
        "max": mx,
        "mean": mean,
        "flags": flags,
        "non_numeric_non_null_dropped": dropped,
        "numeric_sample_count": int(len(sn)),
    }


# ---------------------------------------------------------------------------
# Step 3: lineage
# ---------------------------------------------------------------------------


def load_lineage_snapshot() -> Optional[dict]:
    recs, issues = load_jsonl_records_with_issues(MIGRATED["week4"], lenient=True)
    if issues:
        logger.warning(
            "ContractGenerator lineage load: %d issue(s) in %s (malformed lines skipped; using last valid record).",
            len(issues),
            MIGRATED["week4"],
        )
    if not recs:
        return None
    return recs[-1]


def downstream_table_consumers(snapshot: dict) -> List[Dict[str, Any]]:
    edges = snapshot.get("edges") or []
    out: List[Dict[str, Any]] = []
    seen = set()
    for e in edges:
        if not isinstance(e, dict):
            continue
        if e.get("relationship") != "CONSUMES":
            continue
        src = e.get("source") or ""
        tgt = e.get("target") or ""
        if not str(src).startswith("table::"):
            continue
        key = (src, tgt)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "consumer_node_id": tgt,
                "source_table_node_id": src,
                "relationship": "CONSUMES",
            }
        )
    return out


def unique_pipeline_targets(consumers: List[Dict[str, Any]]) -> List[str]:
    ids = []
    for c in consumers:
        tid = c.get("consumer_node_id")
        if isinstance(tid, str) and tid.startswith("pipeline::"):
            ids.append(tid)
    return sorted(set(ids))


# ---------------------------------------------------------------------------
# Step 4: OpenRouter — schema only
# ---------------------------------------------------------------------------


def columns_for_llm_schema_only(df: pd.DataFrame, structural_cols: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build LLM payload: metadata + adjacency only — no raw cell values."""
    names = list(df.columns)
    payload: List[Dict[str, Any]] = []
    for prof in structural_cols:
        name = prof["name"]
        adj = [n for n in names if n != name][:8]
        hints: List[str] = []
        if name.endswith("_id"):
            hints.append("likely_identifier")
        if "confidence" in name.lower():
            hints.append("likely_probability_score")
        if prof.get("null_fraction", 0) > 0.5:
            hints.append("sparse_column")
        payload.append(
            {
                "column_name": name,
                "pandas_dtype": prof["dtype"],
                "null_fraction": prof["null_fraction"],
                "cardinality": prof["cardinality"],
                "dominant_character_pattern": prof.get("dominant_character_pattern"),
                "distinct_shape_signatures": prof.get("distinct_shape_samples", []),
                "adjacent_columns": adj,
                "semantic_hints": hints,
            }
        )
    return payload


def select_ambiguous_columns(schema_columns: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for c in schema_columns:
        score = 0.0
        if c.get("null_fraction", 0) > 0.4:
            score += 1.0
        if c.get("cardinality", 0) <= 1:
            score += 0.5
        name = c["column_name"]
        if name in ("payload", "metadata", "outputs", "inputs") or name.startswith("_migration"):
            score += 2.0
        if len(name) <= 2:
            score += 1.5
        scored.append((score, c))
    scored.sort(key=lambda x: -x[0])
    out: List[Dict[str, Any]] = []
    for sc, col in scored[:limit]:
        if sc > 0:
            out.append(col)
    return out


def openrouter_annotate_schema(
    dataset_title: str, schema_columns_subset: List[Dict[str, Any]], model: str
) -> Dict[str, Any]:
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key or not schema_columns_subset:
        return {}

    system = (
        "You assist with data contracts. You receive ONLY schema metadata (no raw row values). "
        "Respond with a single JSON object whose keys are column names. "
        "Each value must be an object with keys: description (string), "
        "business_rule_expression (string, machine-oriented), "
        "cross_column_relationships (array of short strings). "
        "Do not invent literal values from a dataset; stay generic where unknown."
    )
    user_payload = {
        "dataset": dataset_title,
        "columns_schema_only": schema_columns_subset,
    }
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(user_payload, default=str)},
        ],
        "temperature": 0.2,
        "max_tokens": 4096,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": os.environ.get("OPENROUTER_HTTP_REFERER", "https://localhost"),
        "X-Title": "data-contract-enforcer-contract-generator",
    }
    r = requests.post(OPENROUTER_URL, headers=headers, json=body, timeout=120)
    r.raise_for_status()
    data = r.json()
    content = (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z]*\n", "", content)
        content = re.sub(r"\n```$", "", content)
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return {"_parse_error": "non_json_llm_response", "_raw": content[:2000]}


# ---------------------------------------------------------------------------
# DataFrame builders per dataset
# ---------------------------------------------------------------------------


def _trim_profiling_issues(issues: List[Dict[str, Any]], cap: int = 32) -> Dict[str, Any]:
    if not issues:
        return {"total": 0, "sample": [], "truncated": False}
    return {"total": len(issues), "sample": issues[:cap], "truncated": len(issues) > cap}


def profiling_caveats_week3_records(records: List[dict]) -> List[str]:
    """Detect row shapes that skew nullability / numeric profiling without crashing."""
    caveats: List[str] = []
    if not records:
        return ["no_records_loaded"]
    non_list_facts = 0
    empty_facts = 0
    bad_ptm = 0
    for r in records:
        if not isinstance(r, dict):
            continue
        ef = r.get("extracted_facts")
        if ef is None:
            empty_facts += 1
        elif not isinstance(ef, list):
            non_list_facts += 1
        ptm = r.get("processing_time_ms")
        if ptm is not None and not isinstance(ptm, (int, float)):
            bad_ptm += 1
    if non_list_facts:
        msg = (
            f"{non_list_facts} row(s) have extracted_facts that is not a list "
            "(primary_fact_confidence falls back to null for those rows)"
        )
        logger.warning("ContractGenerator week3 profiling: %s", msg)
        caveats.append(msg)
    if empty_facts:
        caveats.append(f"{empty_facts} row(s) have null/missing extracted_facts")
    if bad_ptm:
        msg = f"{bad_ptm} row(s) have processing_time_ms not int/float (object dtype / coerce risk)"
        logger.warning("ContractGenerator week3 profiling: %s", msg)
        caveats.append(msg)
    return caveats


def df_week3_extractions(records: List[dict]) -> pd.DataFrame:
    rows = []
    skipped = 0
    for r in records:
        if not isinstance(r, dict):
            skipped += 1
            continue
        facts = r.get("extracted_facts") or []
        conf = None
        if facts and isinstance(facts[0], dict):
            conf = facts[0].get("confidence")
        tc = r.get("token_count") or {}
        rows.append(
            {
                "doc_id": r.get("doc_id"),
                "source_path": r.get("source_path"),
                "source_hash": r.get("source_hash"),
                "extraction_model": r.get("extraction_model"),
                "processing_time_ms": r.get("processing_time_ms"),
                "token_count_input": tc.get("input") if isinstance(tc, dict) else None,
                "token_count_output": tc.get("output") if isinstance(tc, dict) else None,
                "extracted_at": r.get("extracted_at"),
                "entities_len": len(r.get("entities") or []),
                "extracted_facts_len": len(facts),
                "primary_fact_confidence": conf,
            }
        )
    if skipped:
        logger.warning(
            "ContractGenerator df_week3_extractions: skipped %d non-dict row(s) (not represented in profile frame)",
            skipped,
        )
    return pd.DataFrame(rows)


def df_week4_lineage(snapshot: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    node_rows = []
    for n in snapshot.get("nodes") or []:
        if not isinstance(n, dict):
            continue
        md = n.get("metadata") or {}
        node_rows.append(
            {
                "node_id": n.get("node_id"),
                "type": n.get("type"),
                "label": n.get("label"),
                "metadata_path": md.get("path") if isinstance(md, dict) else None,
                "metadata_language": md.get("language") if isinstance(md, dict) else None,
            }
        )
    edge_rows = []
    for e in snapshot.get("edges") or []:
        if not isinstance(e, dict):
            continue
        edge_rows.append(
            {
                "source": e.get("source"),
                "target": e.get("target"),
                "relationship": e.get("relationship"),
                "confidence": e.get("confidence"),
            }
        )
    return pd.DataFrame(node_rows), pd.DataFrame(edge_rows)


def df_week5_events(records: List[dict]) -> pd.DataFrame:
    rows = []
    skipped = 0
    for r in records:
        if not isinstance(r, dict):
            skipped += 1
            continue
        p = r.get("payload") if isinstance(r.get("payload"), dict) else {}
        m = r.get("metadata") if isinstance(r.get("metadata"), dict) else {}
        rows.append(
            {
                "event_id": r.get("event_id"),
                "event_type": r.get("event_type"),
                "aggregate_id": r.get("aggregate_id"),
                "aggregate_type": r.get("aggregate_type"),
                "sequence_number": r.get("sequence_number"),
                "schema_version": r.get("schema_version"),
                "occurred_at": r.get("occurred_at"),
                "recorded_at": r.get("recorded_at"),
                "metadata_correlation_id": m.get("correlation_id"),
                "metadata_source_service": m.get("source_service"),
                "payload_event_types": r.get("event_type"),
                "payload_key_count": len(p),
            }
        )
    if skipped:
        logger.warning(
            "ContractGenerator df_week5_events: skipped %d non-dict row(s)",
            skipped,
        )
    return pd.DataFrame(rows)


def df_langsmith_traces(records: List[dict]) -> pd.DataFrame:
    rows = []
    skipped = 0
    for r in records:
        if not isinstance(r, dict):
            skipped += 1
            continue
        inp = r.get("inputs") if isinstance(r.get("inputs"), dict) else {}
        out = r.get("outputs") if isinstance(r.get("outputs"), dict) else {}
        rows.append(
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "run_type": r.get("run_type"),
                "session_id": r.get("session_id"),
                "error": r.get("error"),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "total_tokens": r.get("total_tokens"),
                "prompt_tokens": r.get("prompt_tokens"),
                "completion_tokens": r.get("completion_tokens"),
                "total_cost": r.get("total_cost"),
                "tags_repr": json.dumps(r.get("tags") or [], sort_keys=True),
                "inputs_key_count": len(inp),
                "outputs_key_count": len(out),
            }
        )
    if skipped:
        logger.warning(
            "ContractGenerator df_langsmith_traces: skipped %d non-dict row(s)",
            skipped,
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Contract + dbt emission
# ---------------------------------------------------------------------------


def write_yaml(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(
            data,
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
            width=110,
        )


LINEAGE_NODE_TYPES = ["FILE", "TABLE", "SERVICE", "MODEL", "PIPELINE", "EXTERNAL"]
LINEAGE_EDGE_RELATIONSHIPS = ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"]
TRACE_RUN_TYPES = ["llm", "chain", "tool", "retriever", "embedding"]


def dbt_column_tests(col: str, required: bool, accepted: Optional[List[str]] = None) -> Dict[str, Any]:
    return dbt_column_entry(col, not_null=required, accepted=accepted)


def dbt_column_entry(
    col: str,
    *,
    not_null: bool = False,
    unique: bool = False,
    accepted: Optional[List[str]] = None,
    relationships: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Build one dbt schema.yml column entry with core generic tests (Step 5)."""
    entry: Dict[str, Any] = {"name": col}
    tests: List[Any] = []
    if not_null:
        tests.append("not_null")
    if unique:
        tests.append("unique")
    if accepted:
        tests.append({"accepted_values": {"values": accepted}})
    if relationships:
        tests.append({"relationships": relationships})
    if tests:
        entry["tests"] = tests
    return entry


def write_dbt_schema(
    path: Path,
    source_name: str,
    table: str,
    columns: List[Dict[str, Any]],
) -> None:
    write_dbt_schema_multi(path, source_name, [(table, columns, None)])


def write_dbt_schema_multi(
    path: Path,
    source_name: str,
    tables: List[Tuple[str, List[Dict[str, Any]], Optional[str]]],
) -> None:
    """
    Write dbt sources with one or more tables (e.g. lineage nodes + edges).
    Third tuple element is an optional table-level description.
    """
    tbl_docs: List[Dict[str, Any]] = []
    for tname, cols, tdesc in tables:
        row: Dict[str, Any] = {"name": tname, "columns": cols}
        if tdesc:
            row["description"] = tdesc
        tbl_docs.append(row)

    doc = {
        "version": 2,
        "sources": [
            {
                "name": source_name,
                "meta": {
                    "generated_by": "ContractGenerator",
                    "migrated_inputs": True,
                    "note": "Logical sources: align staging models to these column tests (Week 7 Step 5).",
                },
                "tables": tbl_docs,
            }
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        yaml.dump(doc, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=100)


def dbt_week3_extraction_columns() -> List[Dict[str, Any]]:
    """Equivalent tests to Bitol week3 extraction_record (migrated JSONL row shape)."""
    return [
        dbt_column_entry("doc_id", not_null=True, unique=True),
        dbt_column_entry("source_path", not_null=True),
        dbt_column_entry("extracted_at", not_null=True),
        dbt_column_entry("processing_time_ms", not_null=True),
        dbt_column_entry("extracted_facts", not_null=True),
        dbt_column_entry("entities", not_null=True),
    ]


def dbt_week4_lineage_nodes_columns() -> List[Dict[str, Any]]:
    return [
        dbt_column_entry("node_id", not_null=True, unique=True),
        dbt_column_entry("type", not_null=True, accepted=LINEAGE_NODE_TYPES),
        dbt_column_entry("label", not_null=False),
        dbt_column_entry("metadata_path", not_null=False),
        dbt_column_entry("metadata_language", not_null=False),
    ]


def dbt_week4_lineage_edges_columns(source_name: str) -> List[Dict[str, Any]]:
    rel_to_nodes = {"to": f"source('{source_name}', 'week4_lineage_nodes')", "field": "node_id"}
    return [
        dbt_column_entry("source", not_null=True, relationships=rel_to_nodes),
        dbt_column_entry("target", not_null=True, relationships=rel_to_nodes),
        dbt_column_entry("relationship", not_null=True, accepted=LINEAGE_EDGE_RELATIONSHIPS),
        dbt_column_entry("confidence", not_null=False),
    ]


def dbt_week5_event_columns() -> List[Dict[str, Any]]:
    return [
        dbt_column_entry("event_id", not_null=True, unique=True),
        dbt_column_entry("event_type", not_null=True),
        dbt_column_entry("aggregate_id", not_null=True),
        dbt_column_entry("aggregate_type", not_null=True),
        dbt_column_entry("sequence_number", not_null=True),
        dbt_column_entry("payload", not_null=True),
        dbt_column_entry("metadata", not_null=True),
        dbt_column_entry("schema_version", not_null=True),
        dbt_column_entry("occurred_at", not_null=True),
        dbt_column_entry("recorded_at", not_null=True),
    ]


def dbt_langsmith_trace_columns() -> List[Dict[str, Any]]:
    return [
        dbt_column_entry("id", not_null=True, unique=True),
        dbt_column_entry("name", not_null=True),
        dbt_column_entry("run_type", not_null=True, accepted=TRACE_RUN_TYPES),
        dbt_column_entry("session_id", not_null=True),
        dbt_column_entry("inputs", not_null=True),
        dbt_column_entry("outputs", not_null=True),
    ]


def build_contract_common(
    *,
    contract_id: str,
    title: str,
    description: str,
    owner: str,
    relative_data_path: str,
    schema: dict,
    quality_checks: List[str],
    lineage_upstream: List[Any],
    lineage_downstream: List[Any],
    profiling: dict,
    llm_annotations: dict,
) -> dict:
    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": title,
            "version": "1.0.0",
            "owner": owner,
            "description": description,
        },
        "servers": {
            "local": {
                "type": "local",
                "path": relative_data_path,
                "format": "jsonl",
                "note": "Uses post-migration contract-aligned JSONL under outputs/migrate/.",
            }
        },
        "terms": {
            "usage": "Internal inter-system data contract. Do not publish raw exports.",
            "limitations": "Profiling reflects migrated snapshot only; regenerate after migrations.",
        },
        "schema": schema,
        "profiling": profiling,
        "llm_annotations": llm_annotations,
        "quality": {
            "type": "SodaChecks",
            "specification": {"checks": quality_checks},
        },
        "lineage": {"upstream": lineage_upstream, "downstream": lineage_downstream},
    }


def placeholder_contract(
    contract_id: str,
    title: str,
    path_relative: str,
    reason: str,
) -> dict:
    return {
        "kind": "DataContract",
        "apiVersion": "v3.0.0",
        "id": contract_id,
        "info": {
            "title": title,
            "version": "0.0.0",
            "owner": "not-deployed",
            "description": reason,
        },
        "servers": {
            "local": {
                "type": "local",
                "path": path_relative,
                "format": "jsonl",
                "availability": "not_deployed_in_this_repo",
            }
        },
        "terms": {"usage": "Placeholder only.", "limitations": "No dataset in this repository scope."},
        "schema": {},
        "profiling": {"status": "skipped", "reason": reason},
        "llm_annotations": {},
        "quality": {
            "type": "SodaChecks",
            "specification": {"checks": ["row_count >= 0"]},
        },
        "lineage": {"upstream": [], "downstream": []},
    }


def profiling_to_llm_columns(
    profiles: List[Dict[str, Any]], prefix: str, adjacent: List[str]
) -> List[Dict[str, Any]]:
    """Turn structural profiles into schema-only LLM payloads (no raw values)."""
    out: List[Dict[str, Any]] = []
    for prof in profiles:
        cname = f"{prefix}.{prof['name']}"
        hints: List[str] = []
        if prof["name"].endswith("_id"):
            hints.append("likely_identifier")
        if prof.get("null_fraction", 0) > 0.5:
            hints.append("sparse_column")
        out.append(
            {
                "column_name": cname,
                "pandas_dtype": prof["dtype"],
                "null_fraction": prof["null_fraction"],
                "cardinality": prof["cardinality"],
                "dominant_character_pattern": prof.get("dominant_character_pattern"),
                "distinct_shape_signatures": prof.get("distinct_shape_samples", []),
                "adjacent_columns": [a for a in adjacent if a != cname][:8],
                "semantic_hints": hints,
            }
        )
    return out


def downstream_for_week3(snapshot: Optional[dict]) -> List[dict]:
    if not snapshot:
        return []
    consumers = downstream_table_consumers(snapshot)
    pipes = unique_pipeline_targets(consumers)
    return [
        {
            "id": pid,
            "description": "Week 4 lineage: SQL migration/pipeline consumes database tables (blast-radius context for adjacent systems).",
            "fields_consumed": ["logical: relational schema under migration"],
            "breaking_if_changed": ["migration SQL altering consumed tables"],
        }
        for pid in pipes[:25]
    ]


def downstream_generic_lineage(snapshot: Optional[dict]) -> List[dict]:
    return downstream_for_week3(snapshot)[:15]


# ---------------------------------------------------------------------------
# Targeted CLI: profile → Bitol clauses → lineage (instruction-aligned)
# ---------------------------------------------------------------------------


def flatten_for_profile(records: List[dict]) -> pd.DataFrame:
    """Explode extracted_facts to one row per item (same shape as ValidationRunner profiling)."""
    return flatten_extractions_for_profile(records)


def profile_column(series: pd.Series, col_name: str) -> Dict[str, Any]:
    sn = series.dropna()
    try:
        card = int(sn.nunique())
        uniques = sn.unique()
    except TypeError:
        str_vals = sn.map(lambda x: json.dumps(x, sort_keys=True, default=str) if isinstance(x, (list, dict)) else str(x))
        card = int(str_vals.nunique())
        uniques = str_vals.unique()
    result: Dict[str, Any] = {
        "name": col_name,
        "dtype": str(series.dtype),
        "null_fraction": float(series.isna().mean()),
        "cardinality_estimate": card,
        "sample_values": [str(v) for v in uniques[:5]],
    }
    if pd.api.types.is_numeric_dtype(series):
        sn2 = series.dropna()
        if len(sn2):
            result["stats"] = {
                "min": float(sn2.min()),
                "max": float(sn2.max()),
                "mean": float(sn2.mean()),
                "p25": float(sn2.quantile(0.25)),
                "p50": float(sn2.quantile(0.50)),
                "p75": float(sn2.quantile(0.75)),
                "p95": float(sn2.quantile(0.95)),
                "p99": float(sn2.quantile(0.99)),
                "stddev": float(sn2.std()),
            }
    elif str(series.dtype) == "object" and (
        "confidence" in col_name.lower()
        or col_name.endswith("_ms")
        or "sequence" in col_name.lower()
        or col_name.endswith("_tokens")
    ):
        coerced = pd.to_numeric(series, errors="coerce")
        n_bad = int((series.notna() & coerced.isna()).sum())
        if n_bad > 0:
            logger.warning(
                "ContractGenerator profile_column(%s): %d non-null object cells not numeric-coercible",
                col_name,
                n_bad,
            )
            result["non_numeric_coerced_away"] = n_bad
        cn = coerced.dropna()
        if len(cn):
            result["stats"] = {
                "min": float(cn.min()),
                "max": float(cn.max()),
                "mean": float(cn.mean()),
                "p25": float(cn.quantile(0.25)),
                "p50": float(cn.quantile(0.50)),
                "p75": float(cn.quantile(0.75)),
                "p95": float(cn.quantile(0.95)),
                "p99": float(cn.quantile(0.99)),
                "stddev": float(cn.std()),
            }
    return result


def infer_type(dtype_str: str) -> str:
    mapping = {
        "float64": "number",
        "float32": "number",
        "int64": "integer",
        "Int64": "integer",
        "bool": "boolean",
        "boolean": "boolean",
        "object": "string",
    }
    return mapping.get(dtype_str, "string")


def column_to_clause(profile: Dict[str, Any]) -> Dict[str, Any]:
    null_frac = float(profile.get("null_fraction") or 0.0)
    uncertain = bool(profile.get("non_numeric_coerced_away"))
    # Junk strings in an object column can inflate null_fraction after coerce in profile_column stats
    # but leave raw null_fraction 0; keep required=True only when truly no nulls in the raw series.
    clause: Dict[str, Any] = {
        "type": infer_type(profile["dtype"]),
        "required": null_frac == 0.0,
    }
    name = profile["name"]
    if uncertain:
        clause["required"] = False
        clause["description"] = (
            f"Profiling marked optional because {profile['non_numeric_coerced_away']} cell(s) were not "
            "numeric-coercible; review upstream types before setting required: true."
        )
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        conf_desc = (
            "Confidence score. Must remain 0.0–1.0 float. BREAKING if changed to 0–100."
        )
        clause["description"] = (
            f"{clause.get('description', '')} {conf_desc}".strip() if clause.get("description") else conf_desc
        )
    if name.endswith("_id"):
        clause["format"] = "uuid"
        clause["pattern"] = r"^[0-9a-f-]{36}$"
    if name.endswith("_at"):
        clause["format"] = "date-time"
    ce = profile.get("cardinality_estimate", 999)
    if ce <= 10 and clause["type"] == "string" and profile.get("dtype") == "object":
        series_vals = profile.get("_distinct_non_null")
        if isinstance(series_vals, list) and len(series_vals) == ce and ce > 0:
            clause["enum"] = [str(v) for v in sorted(series_vals, key=str)]
    return clause


def _enrich_profile_for_enum(df: pd.DataFrame, col: str, profile: Dict[str, Any]) -> None:
    s = df[col].dropna()
    if s.map(lambda x: isinstance(x, (list, dict))).any():
        profile["_distinct_non_null"] = None
        return
    try:
        profile["_distinct_non_null"] = list(s.unique())
    except TypeError:
        profile["_distinct_non_null"] = None


def inject_lineage_instruction(contract: dict, lineage_path: Path) -> dict:
    if not lineage_path.is_file():
        contract.setdefault("lineage", {})
        contract["lineage"].setdefault("upstream", [])
        contract["lineage"].setdefault("downstream", [])
        return contract
    text = lineage_path.read_text(encoding="utf-8").strip()
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    snapshot: dict = {}
    if lines:
        try:
            snapshot = json.loads(lines[-1])
        except json.JSONDecodeError:
            try:
                snapshot = json.loads(text)
            except json.JSONDecodeError:
                snapshot = {}
    consumers: List[str] = []
    for e in snapshot.get("edges") or []:
        if not isinstance(e, dict):
            continue
        src = str(e.get("source", "")).lower()
        if "week3" in src or "extraction" in src:
            t = e.get("target")
            if t:
                consumers.append(str(t))
    contract.setdefault("lineage", {})
    contract["lineage"]["upstream"] = []
    contract["lineage"]["downstream"] = [
        {"id": c, "fields_consumed": ["doc_id", "extracted_facts"]} for c in sorted(set(consumers))
    ]
    return contract


def run_targeted_extractions_contract(
    *,
    source: Path,
    contract_id: str,
    lineage_path: Path,
    output_dir: Path,
    repo_root: Path,
    use_llm: bool,
    openrouter_model: str,
    registry_path: Optional[Path],
) -> None:
    records, cli_load_issues = load_jsonl_records_with_issues(source, lenient=True)
    if cli_load_issues:
        logger.warning(
            "ContractGenerator CLI profile: skipped %d malformed JSONL line(s) from %s",
            len(cli_load_issues),
            source,
        )
    cli_row_caveats = profiling_caveats_week3_records(records)
    try:
        df = flatten_for_profile(records)
    except Exception as exc:
        logger.exception("ContractGenerator flatten_for_profile failed (%s); empty DataFrame", exc)
        df = pd.DataFrame()
    contract_violations: List[Dict[str, Any]] = []
    for col in df.columns:
        if "confidence" in col.lower() and str(df[col].dtype) == "object":
            contract_violations.append(
                {
                    "column": col,
                    "issue": "mixed_types_object_dtype",
                    "detail": "Confidence-like column is object, not numeric; fix upstream before relying on range checks.",
                }
            )

    column_profiles: Dict[str, Dict[str, Any]] = {}
    for col in df.columns:
        prof = profile_column(df[col], col)
        if prof.get("cardinality_estimate", 999) <= 10 and prof.get("dtype") == "object":
            _enrich_profile_for_enum(df, col, prof)
        column_profiles[col] = prof

    schema: Dict[str, Any] = {}
    items: Dict[str, Any] = {}
    for col, prof in column_profiles.items():
        clause = column_to_clause(prof)
        if col.startswith("fact_"):
            inner = col[5:]
            items[inner] = clause
        else:
            schema[col] = clause
    if items:
        schema["extracted_facts"] = {"type": "array", "items": items}
    if "entities" not in schema:
        schema["entities"] = {"type": "array", "required": False}
    if "token_count" not in schema:
        schema["token_count"] = {
            "type": "object",
            "required": False,
            "properties": {
                "input": {"type": "integer", "required": False},
                "output": {"type": "integer", "required": False},
            },
        }

    try:
        rel_data = str(source.resolve().relative_to(repo_root.resolve()))
    except ValueError:
        rel_data = str(source)

    yprof = ydata_profile_summary(df, "week3_extractions_cli")
    llm_annotations: Dict[str, Any] = {}
    if use_llm:
        struct = structural_column_profiles(df)
        amb = select_ambiguous_columns(columns_for_llm_schema_only(df, struct))
        llm_annotations = openrouter_annotate_schema(
            "Week 3 Document Refinery — Extractions (CLI)", amb, openrouter_model
        )
    else:
        llm_annotations = {"status": "skipped", "reason": "--no-llm or OPENROUTER_API_KEY unset"}

    qc: List[str] = ["row_count >= 1"]
    if "doc_id" in df.columns:
        qc.extend(["missing_count(doc_id) = 0", "duplicate_count(doc_id) = 0"])
    if "primary_fact_confidence" in df.columns:
        qc.extend(
            [
                "min(primary_fact_confidence) >= 0.0",
                "max(primary_fact_confidence) <= 1.0",
            ]
        )

    title = f"Generated contract — {contract_id}"
    description = (
        "Profiled from JSONL via ContractGenerator; clauses map nullability, types, "
        "and detected constraints."
    )
    owner = "data-contracts"

    contract = build_contract_common(
        contract_id=contract_id,
        title=title,
        description=description,
        owner=owner,
        relative_data_path=rel_data,
        schema=schema,
        quality_checks=qc,
        lineage_upstream=[],
        lineage_downstream=[],
        profiling={
            "engine": "ydata-profiling",
            "generated_at": utc_now_iso(),
            "structural": {
                "ydata_profiling": yprof,
                "column_profiles": list(column_profiles.values()),
                "contract_violations": contract_violations,
                "input_quality": {
                    "jsonl_load": _trim_profiling_issues(cli_load_issues),
                    "row_shape_caveats": cli_row_caveats,
                },
            },
        },
        llm_annotations=llm_annotations,
    )
    inject_lineage_instruction(contract, lineage_path)
    enrich_contract_lineage_from_registry(contract, contract_id, registry_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    primary_out = output_dir / f"{contract_id}.yaml"
    write_yaml(primary_out, contract)
    if contract_id == "week3-document-refinery-extractions":
        write_yaml(output_dir / "week3_extractions.yaml", contract)
    write_contract_schema_snapshot(
        repo_root,
        contract_id,
        schema,
        registry_subscribers=list(contract.get("lineage", {}).get("registry_subscribers") or []),
    )
    if contract_id == "week3-document-refinery-extractions":
        write_dbt_schema(
            output_dir / "week3_extractions_dbt.yml",
            "contract_sources",
            "week3_extractions",
            dbt_week3_extraction_columns(),
        )
    print(f"Wrote {primary_out}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="ContractGenerator — Phase 1")
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="JSONL path for single-contract generation (Week 3 extractions)",
    )
    parser.add_argument(
        "--contract-id",
        type=str,
        default=None,
        help="Bitol contract id (required with --source), e.g. week3-document-refinery-extractions",
    )
    parser.add_argument(
        "--lineage",
        type=str,
        default=None,
        help="Week 4 lineage_snapshots.jsonl (latest line used). Default: outputs/week4/lineage_snapshots.jsonl",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory for YAML/dbt. Default: generated_contracts",
    )
    parser.add_argument(
        "--registry",
        type=str,
        default=None,
        help="Optional contract_registry YAML (e.g. contract_registry/subscriptions.yaml) to enrich lineage.registry_subscribers",
    )
    parser.add_argument("--no-llm", action="store_true", help="Skip OpenRouter schema annotations")
    parser.add_argument(
        "--openrouter-model",
        default=os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
        help="OpenRouter model id",
    )
    parser.add_argument(
        "--repo-root",
        type=str,
        default=None,
        help="Repository root for snapshots and relative paths (default: enforcer repo root).",
    )
    args = parser.parse_args()
    use_llm = not args.no_llm and bool(os.environ.get("OPENROUTER_API_KEY"))

    registry_resolved = resolve_optional_registry_path(args.registry)
    if registry_resolved is None:
        print(
            "INFO: No registry provided — blast radius will use lineage graph only",
            file=sys.stderr,
        )

    if args.source is not None:
        if not args.contract_id:
            parser.error("--contract-id is required when using --source")
        src = Path(args.source)
        proj_root = Path(args.repo_root) if args.repo_root else REPO_ROOT
        if not proj_root.is_absolute():
            proj_root = REPO_ROOT / proj_root
        if not src.is_absolute():
            src = proj_root / src
        lin = Path(args.lineage or "outputs/week4/lineage_snapshots.jsonl")
        if not lin.is_absolute():
            lin = proj_root / lin
        out = Path(args.output or "generated_contracts")
        if not out.is_absolute():
            out = proj_root / out
        run_targeted_extractions_contract(
            source=src,
            contract_id=args.contract_id,
            lineage_path=lin,
            output_dir=out,
            repo_root=proj_root,
            use_llm=use_llm,
            openrouter_model=args.openrouter_model,
            registry_path=registry_resolved,
        )
        return

    GENERATED.mkdir(parents=True, exist_ok=True)
    snapshot = load_lineage_snapshot()

    # --- Week 3 ---
    w3_records, w3_load_issues = load_jsonl_records_with_issues(MIGRATED["week3"], lenient=True)
    if w3_load_issues:
        logger.warning(
            "ContractGenerator week3: skipped %d malformed JSONL line(s) / fragments at %s",
            len(w3_load_issues),
            MIGRATED["week3"],
        )
    w3_row_caveats = profiling_caveats_week3_records(w3_records)
    df3 = df_week3_extractions(w3_records)
    structural3 = structural_column_profiles(df3)
    y3 = ydata_profile_summary(df3, "week3_extractions")
    num3 = dataframe_numeric_profile(df3)
    conf3 = (
        confidence_distribution_flags(df3["primary_fact_confidence"], column_label="primary_fact_confidence")
        if not df3.empty and "primary_fact_confidence" in df3.columns
        else {}
    )
    w3_input_quality = {
        "jsonl_load": _trim_profiling_issues(w3_load_issues),
        "row_shape_caveats": w3_row_caveats,
    }

    llm3 = {}
    if use_llm:
        amb = select_ambiguous_columns(columns_for_llm_schema_only(df3, structural3))
        llm3 = openrouter_annotate_schema("Week 3 Document Refinery — Extractions", amb, args.openrouter_model)
    else:
        llm3 = {"status": "skipped", "reason": "--no-llm or OPENROUTER_API_KEY unset"}

    schema3 = {
        "doc_id": {
            "type": "string",
            "format": "uuid",
            "pattern": "^[0-9a-f-]{36}$",
            "required": True,
            "unique": True,
            "description": "Primary key (deterministic UUID post-migration).",
        },
        "source_path": {"type": "string", "required": True, "description": "Source document path or filename."},
        "source_hash": {
            "type": "string",
            "pattern": "^[a-f0-9]{64}$",
            "required": False,
            "description": "SHA-256 when present; often null in legacy migrated export.",
        },
        "extracted_facts": {
            "type": "array",
            "description": "Facts array; migrated export uses legacy summary fact with confidence.",
            "items": {
                "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0, "required": True},
                "fact_id": {
                    "type": "string",
                    "format": "uuid",
                    "pattern": "^[0-9a-f-]{36}$",
                    "required": True,
                },
            },
        },
        "entities": {"type": "array", "required": False},
        "extraction_model": {
            "type": "string",
            "required": False,
            "description": "Model id when present.",
        },
        "processing_time_ms": {"type": "integer", "minimum": 1, "required": True},
        "token_count": {
            "type": "object",
            "properties": {
                "input": {"type": "integer", "required": False},
                "output": {"type": "integer", "required": False},
            },
        },
        "extracted_at": {"type": "string", "format": "date-time", "required": True},
    }

    checks3 = [
        "missing_count(doc_id) = 0",
        "duplicate_count(doc_id) = 0",
        "min(primary_fact_confidence) >= 0.0",
        "max(primary_fact_confidence) <= 1.0",
        "row_count >= 1",
    ]

    contract3 = build_contract_common(
        contract_id="week3-document-refinery-extractions",
        title="Week 3 Document Refinery — Extraction Records (migrated)",
        description="One record per processed document. Inputs read from outputs/migrate/migrated_extractions.jsonl.",
        owner="week3-team",
        relative_data_path="outputs/migrate/migrated_extractions.jsonl",
        schema=schema3,
        quality_checks=checks3,
        lineage_upstream=[],
        lineage_downstream=downstream_for_week3(snapshot),
        profiling={
            "engine": "ydata-profiling",
            "generated_at": utc_now_iso(),
            "structural": {
                "ydata_profiling": y3,
                "columns": structural3,
                "numeric": num3,
                "confidence_distribution": conf3,
                "input_quality": w3_input_quality,
            },
        },
        llm_annotations=llm3,
    )
    enrich_contract_lineage_from_registry(contract3, contract3["id"], registry_resolved)
    write_yaml(GENERATED / "week3_extractions.yaml", contract3)
    write_contract_schema_snapshot(
        REPO_ROOT,
        contract3["id"],
        contract3.get("schema") or {},
        registry_subscribers=list(contract3.get("lineage", {}).get("registry_subscribers") or []),
    )
    write_dbt_schema(
        GENERATED / "week3_extractions_dbt.yml",
        "contract_sources",
        "week3_extractions",
        dbt_week3_extraction_columns(),
    )

    # --- Week 4 ---
    if snapshot:
        df4n, df4e = df_week4_lineage(snapshot)
        st4n = structural_column_profiles(df4n)
        st4e = structural_column_profiles(df4e)
        y4n = ydata_profile_summary(df4n, "week4_lineage_nodes")
        y4e = ydata_profile_summary(df4e, "week4_lineage_edges")
        llm4 = {}
        if use_llm:
            adj = ["lineage_nodes.node_id", "lineage_edges.source", "lineage_edges.target"]
            llm_cols4 = profiling_to_llm_columns(st4n, "lineage_nodes", adj) + profiling_to_llm_columns(
                st4e, "lineage_edges", adj
            )
            amb4 = select_ambiguous_columns(llm_cols4)
            if not amb4 and llm_cols4:
                amb4 = llm_cols4[:10]
            llm4 = openrouter_annotate_schema("Week 4 Lineage — nodes & edges", amb4, args.openrouter_model)
        else:
            llm4 = {"status": "skipped", "reason": "--no-llm or OPENROUTER_API_KEY unset"}

        schema4 = {
            "snapshot_id": {"type": "string", "format": "uuid", "required": True},
            "codebase_root": {"type": "string", "required": True},
            "git_commit": {"type": "string", "pattern": "^[a-f0-9]{40}$", "required": True},
            "nodes": {
                "type": "array",
                "items": {
                    "node_id": {"type": "string", "required": True},
                    "type": {"type": "string", "enum": ["FILE", "TABLE", "SERVICE", "MODEL", "PIPELINE", "EXTERNAL"]},
                    "label": {"type": "string"},
                    "metadata": {"type": "object"},
                },
            },
            "edges": {
                "type": "array",
                "items": {
                    "source": {"type": "string", "required": True},
                    "target": {"type": "string", "required": True},
                    "relationship": {
                        "type": "string",
                        "enum": ["IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"],
                    },
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                },
            },
            "captured_at": {"type": "string", "format": "date-time", "required": True},
        }
        checks4 = [
            "row_count(nodes) >= 1",
            "edge_endpoints_resolve_to_nodes = 1",
            "invalid_relationship_count = 0",
        ]
        contract4 = build_contract_common(
            contract_id="week4-brownfield-lineage-snapshot",
            title="Week 4 Brownfield Cartographer — Lineage Snapshot (migrated)",
            description="Single snapshot JSONL line; migrated relative pipeline node_ids.",
            owner="week4-team",
            relative_data_path="outputs/migrate/migrated_lineage_snapshots.jsonl",
            schema=schema4,
            quality_checks=checks4,
            lineage_upstream=[],
            lineage_downstream=[],
            profiling={
                "engine": "ydata-profiling",
                "generated_at": utc_now_iso(),
                "structural": {
                    "nodes_table": {"ydata_profiling": y4n, "columns": st4n},
                    "edges_table": {"ydata_profiling": y4e, "columns": st4e},
                },
            },
            llm_annotations=llm4,
        )
        enrich_contract_lineage_from_registry(contract4, contract4["id"], registry_resolved)
        write_yaml(GENERATED / "week4_lineage.yaml", contract4)
        write_contract_schema_snapshot(
            REPO_ROOT,
            contract4["id"],
            contract4.get("schema") or {},
            registry_subscribers=list(contract4.get("lineage", {}).get("registry_subscribers") or []),
        )
        _src = "contract_sources"
        write_dbt_schema_multi(
            GENERATED / "week4_lineage_dbt.yml",
            _src,
            [
                (
                    "week4_lineage_nodes",
                    dbt_week4_lineage_nodes_columns(),
                    "Logical table: one row per snapshot.nodes[] element after parsing migrated_lineage_snapshots.jsonl.",
                ),
                (
                    "week4_lineage_edges",
                    dbt_week4_lineage_edges_columns(_src),
                    "Logical table: one row per snapshot.edges[]; source/target FK to week4_lineage_nodes.node_id.",
                ),
            ],
        )
    else:
        _ph4 = placeholder_contract(
            "week4-lineage-missing",
            "Week 4 Lineage (missing migrated file)",
            "outputs/migrate/migrated_lineage_snapshots.jsonl",
            "Migrated lineage file not found; run migrations first.",
        )
        enrich_contract_lineage_from_registry(_ph4, _ph4["id"], registry_resolved)
        write_yaml(GENERATED / "week4_lineage.yaml", _ph4)
        write_contract_schema_snapshot(
            REPO_ROOT,
            _ph4["id"],
            _ph4.get("schema") or {},
            registry_subscribers=list(_ph4.get("lineage", {}).get("registry_subscribers") or []),
        )
        _src = "contract_sources"
        write_dbt_schema_multi(
            GENERATED / "week4_lineage_dbt.yml",
            _src,
            [
                ("week4_lineage_nodes", dbt_week4_lineage_nodes_columns(), None),
                ("week4_lineage_edges", dbt_week4_lineage_edges_columns(_src), None),
            ],
        )

    # --- Week 5 ---
    w5, w5_load_issues = load_jsonl_records_with_issues(MIGRATED["week5"], lenient=True)
    if w5_load_issues:
        logger.warning(
            "ContractGenerator week5: skipped %d malformed line(s) at %s",
            len(w5_load_issues),
            MIGRATED["week5"],
        )
    df5 = df_week5_events(w5)
    st5 = structural_column_profiles(df5)
    y5 = ydata_profile_summary(df5, "week5_events")
    num5 = dataframe_numeric_profile(df5)
    w5_input_quality = {"jsonl_load": _trim_profiling_issues(w5_load_issues)}
    llm5 = {}
    if use_llm:
        amb5 = select_ambiguous_columns(columns_for_llm_schema_only(df5, st5))
        llm5 = openrouter_annotate_schema("Week 5 Event Sourcing — Events", amb5, args.openrouter_model)
    else:
        llm5 = {"status": "skipped", "reason": "--no-llm or OPENROUTER_API_KEY unset"}

    schema5 = {
        "event_id": {"type": "string", "format": "uuid", "required": True},
        "event_type": {"type": "string", "required": True, "description": "PascalCase event name."},
        "aggregate_id": {"type": "string", "format": "uuid", "required": True},
        "aggregate_type": {"type": "string", "required": True},
        "sequence_number": {"type": "integer", "minimum": 1, "required": True},
        "payload": {"type": "object", "required": True},
        "metadata": {
            "type": "object",
            "required": True,
            "properties": {
                "causation_id": {"type": "string", "required": False},
                "correlation_id": {"type": "string", "format": "uuid", "required": True},
                "user_id": {"type": "string", "required": False},
                "source_service": {"type": "string", "required": True},
            },
        },
        "schema_version": {"type": "string", "required": True},
        "occurred_at": {"type": "string", "format": "date-time", "required": True},
        "recorded_at": {"type": "string", "format": "date-time", "required": True},
    }
    checks5 = [
        "missing_count(event_id) = 0",
        "duplicate_count(event_id) = 0",
        "recorded_gte_occurred = 1",
    ]
    contract5 = build_contract_common(
        contract_id="week5-event-sourcing-events",
        title="Week 5 Event Sourcing — Event Records (migrated)",
        description="Canonical envelope events; inputs from outputs/migrate/migrated_events.jsonl.",
        owner="week5-team",
        relative_data_path="outputs/migrate/migrated_events.jsonl",
        schema=schema5,
        quality_checks=checks5,
        lineage_upstream=[],
        lineage_downstream=downstream_generic_lineage(snapshot),
        profiling={
            "engine": "ydata-profiling",
            "generated_at": utc_now_iso(),
            "structural": {
                "ydata_profiling": y5,
                "columns": st5,
                "numeric": num5,
                "input_quality": w5_input_quality,
            },
        },
        llm_annotations=llm5,
    )
    enrich_contract_lineage_from_registry(contract5, contract5["id"], registry_resolved)
    write_yaml(GENERATED / "week5_events.yaml", contract5)
    write_contract_schema_snapshot(
        REPO_ROOT,
        contract5["id"],
        contract5.get("schema") or {},
        registry_subscribers=list(contract5.get("lineage", {}).get("registry_subscribers") or []),
    )
    write_dbt_schema(
        GENERATED / "week5_events_dbt.yml",
        "contract_sources",
        "week5_events",
        dbt_week5_event_columns(),
    )

    # --- LangSmith traces (migrated) ---
    tr, tr_load_issues = load_jsonl_records_with_issues(MIGRATED["traces"], lenient=True)
    if tr_load_issues:
        logger.warning(
            "ContractGenerator traces: skipped %d malformed line(s) at %s",
            len(tr_load_issues),
            MIGRATED["traces"],
        )
    dft = df_langsmith_traces(tr)
    stt = structural_column_profiles(dft)
    yt = ydata_profile_summary(dft, "langsmith_traces")
    numt = dataframe_numeric_profile(dft)
    trace_numeric_note = {
        "outputs_key_count_stats": numeric_stats(dft["outputs_key_count"], column_label="outputs_key_count")
        if "outputs_key_count" in dft.columns
        else None,
    }
    tr_input_quality = {"jsonl_load": _trim_profiling_issues(tr_load_issues)}
    llmt = {}
    if use_llm:
        amb_t = select_ambiguous_columns(columns_for_llm_schema_only(dft, stt))
        llmt = openrouter_annotate_schema("LangSmith trace_record (migrated)", amb_t, args.openrouter_model)
    else:
        llmt = {"status": "skipped", "reason": "--no-llm or OPENROUTER_API_KEY unset"}

    schemat = {
        "id": {"type": "string", "format": "uuid", "required": True},
        "name": {"type": "string", "required": True},
        "run_type": {
            "type": "string",
            "enum": ["llm", "chain", "tool", "retriever", "embedding"],
            "required": True,
        },
        "inputs": {"type": "object", "required": True},
        "outputs": {"type": "object", "required": True},
        "error": {"type": "string", "required": False},
        "start_time": {"type": "string", "format": "date-time", "required": False},
        "end_time": {"type": "string", "format": "date-time", "required": False},
        "total_tokens": {"type": "integer", "minimum": 0, "required": False},
        "prompt_tokens": {"type": "integer", "minimum": 0, "required": False},
        "completion_tokens": {"type": "integer", "minimum": 0, "required": False},
        "total_cost": {"type": "number", "minimum": 0.0, "required": False},
        "tags": {"type": "array", "items": {"type": "string"}},
        "parent_run_id": {"type": "string", "format": "uuid", "required": False},
        "session_id": {"type": "string", "format": "uuid", "required": True},
    }
    checkst = [
        "row_count >= 1",
        "run_type in ('llm','chain','tool','retriever','embedding')",
        "coalesce(total_tokens, prompt_tokens+completion_tokens) consistent when all non-null",
    ]
    contract_t = build_contract_common(
        contract_id="langsmith-trace-record-migrated",
        title="LangSmith Trace Export — trace_record (migrated)",
        description="Migrated canonical trace rows from outputs/migrate/migrated_runs.jsonl.",
        owner="week7-team",
        relative_data_path="outputs/migrate/migrated_runs.jsonl",
        schema=schemat,
        quality_checks=checkst,
        lineage_upstream=[],
        lineage_downstream=downstream_generic_lineage(snapshot),
        profiling={
            "engine": "ydata-profiling",
            "generated_at": utc_now_iso(),
            "structural": {
                "ydata_profiling": yt,
                "columns": stt,
                "numeric": numt,
                "trace_shape_notes": trace_numeric_note,
                "token_fields_nullable_note": "Migrated export may omit timings/tokens pending LangSmith backfill.",
                "input_quality": tr_input_quality,
            },
        },
        llm_annotations=llmt,
    )
    enrich_contract_lineage_from_registry(contract_t, contract_t["id"], registry_resolved)
    write_yaml(GENERATED / "langsmith_traces.yaml", contract_t)
    write_contract_schema_snapshot(
        REPO_ROOT,
        contract_t["id"],
        contract_t.get("schema") or {},
        registry_subscribers=list(contract_t.get("lineage", {}).get("registry_subscribers") or []),
    )
    write_dbt_schema(
        GENERATED / "langsmith_traces_dbt.yml",
        "contract_sources",
        "langsmith_traces",
        dbt_langsmith_trace_columns(),
    )

    print("ContractGenerator finished. Wrote YAML + dbt under generated_contracts/", file=sys.stderr)
    if not use_llm:
        print(
            "LLM annotations skipped (set OPENROUTER_API_KEY or omit --no-llm to enable).",
            file=sys.stderr,
        )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(message)s",
        stream=sys.stderr,
        force=True,
    )
    main()
