#!/usr/bin/env python3
"""
ValidationRunner (Phase 2A) — executes every contract clause against a JSONL snapshot
and emits a single structured report (PASS / FAIL / WARN / ERROR per check).

Usage (from repository root):

  python contracts/runner.py \\
    --contract generated_contracts/week3_extractions.yaml \\
    --data outputs/week3/extractions.jsonl \\
    --output validation_reports/week3_$(date +%Y%m%d_%H%M).json

Default data path: contract ``servers.local.path`` when ``--data`` is omitted.

The public entry point for imports is ``ValidationRunner`` (class below); the CLI
delegates to ``ValidationRunner.run()``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import statistics
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import yaml

from contracts.validation_checks import (
    CheckResult,
    check_datetime_isoformat_week3,
    check_enum_conformance_week3,
    check_extracted_facts_confidence,
    check_int_minimum,
    check_pattern_optional,
    check_pandas_type_match_week3,
    check_required_top_level,
    check_unique,
    check_uuid_format,
    contract_prefix_from_id,
    flatten_extractions_for_profile,
    parse_quality_soda_line,
    primary_fact_confidence,
    result_to_dict,
    check_numeric_drift,
)

BASELINES_PATH = REPO_ROOT / "schema_snapshots" / "baselines.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def load_contract(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_jsonl(path: Path) -> List[dict]:
    """JSONL (one object per line) or a single pretty-printed JSON object/array (e.g. Week 4 snapshot)."""
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return [parsed]
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except json.JSONDecodeError:
            pass
    out = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def resolve_data_path(contract: dict, data_arg: Optional[str]) -> Path:
    if data_arg:
        p = Path(data_arg)
        if not p.is_absolute():
            p = REPO_ROOT / p
        return p
    servers = contract.get("servers") or {}
    local = servers.get("local") or {}
    rel = local.get("path")
    if not rel:
        raise ValueError("No --data and contract has no servers.local.path")
    p = Path(rel)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return p


# --- baseline persistence ---


def load_baselines() -> dict:
    if not BASELINES_PATH.is_file():
        return {"_meta": {"documentation": "Numeric drift baselines per contract column."}, "by_contract": {}}
    with BASELINES_PATH.open(encoding="utf-8") as f:
        data = json.load(f)
    if "by_contract" not in data:
        data["by_contract"] = {}
    return data


def save_baselines(data: dict) -> None:
    BASELINES_PATH.parent.mkdir(parents=True, exist_ok=True)
    data["written_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with BASELINES_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def col_numeric_stats(records: List[dict], extractor: Callable[[dict], Any]) -> Optional[Tuple[float, float, int]]:
    vals: List[float] = []
    for r in records:
        v = extractor(r)
        if v is None:
            continue
        try:
            vals.append(float(v))
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    m = statistics.mean(vals)
    s = statistics.pstdev(vals) if len(vals) > 1 else 0.0
    return m, s, len(vals)


DRIFT_EXTRACTORS: Dict[str, List[Tuple[str, str, Callable[[dict], Any]]]] = {
    "week3-document-refinery-extractions": [
        ("processing_time_ms", "processing_time_ms", lambda r: r.get("processing_time_ms")),
        ("primary_fact_confidence", "primary_fact_confidence", primary_fact_confidence),
    ],
    "week5-event-sourcing-events": [
        ("sequence_number", "sequence_number", lambda r: r.get("sequence_number")),
    ],
    "langsmith-trace-record-migrated": [
        ("inputs_key_count", "inputs_key_count", lambda r: len((r.get("inputs") or {})) if isinstance(r.get("inputs"), dict) else None),
    ],
}


def _drift_extractor_map(contract_id: str, baselines_cols: Dict[str, Any]) -> Dict[str, Tuple[str, Callable[[dict], Any]]]:
    """Registered extractors plus any column already in baselines.json (top-level numeric fields)."""
    m: Dict[str, Tuple[str, Callable[[dict], Any]]] = {}
    for key, label, fn in DRIFT_EXTRACTORS.get(contract_id, []):
        m[key] = (label, fn)
    for key in baselines_cols:
        if key not in m:

            def _get(rec: dict, k: str = key) -> Any:
                return rec.get(k)

            m[key] = (key, _get)
    return m


def apply_drift_checks(
    contract_id: str,
    prefix: str,
    records: List[dict],
    baselines: dict,
    reset_baselines: bool,
) -> List[CheckResult]:
    """Statistical drift vs ``schema_snapshots/baselines.json`` (2σ WARN, 3σ FAIL)."""
    out: List[CheckResult] = []
    spec = baselines.setdefault("by_contract", {}).setdefault(contract_id, {"columns": {}})
    if reset_baselines:
        spec["columns"] = {}
    cols = spec.setdefault("columns", {})
    extractor_map = _drift_extractor_map(contract_id, cols)
    all_keys = sorted(set(extractor_map.keys()) | set(cols.keys()))

    for key in all_keys:
        if key not in extractor_map:
            continue
        label, fn = extractor_map[key]
        stats = col_numeric_stats(records, fn)
        if stats is None:
            continue
        mean_c, std_c, n = stats
        if reset_baselines or key not in cols:
            sd = max(std_c, 1e-9)
            cols[key] = {
                "mean": mean_c,
                "std": sd,
                "stddev": sd,
                "n": n,
                "established_at": utc_now_iso(),
            }
            continue
        b = cols[key]
        bm = float(b["mean"])
        raw_sd = b.get("stddev")
        if raw_sd is None:
            raw_sd = b.get("std")
        bs = max(float(raw_sd or 1e-9), 1e-9)
        out.append(
            check_numeric_drift(
                contract_id,
                key,
                label,
                mean_c,
                std_c,
                bm,
                bs,
                prefix,
            )
        )
    return out


# --- contract-specific check builders ---


def checks_week3(contract: dict, records: List[dict], prefix: str) -> List[CheckResult]:
    """Structural checks first (required, type, enum, uuid, date-time), then statistical (range), then Soda."""
    results: List[CheckResult] = []
    schema = contract.get("schema") or {}
    df = flatten_extractions_for_profile(records)

    for field, spec in schema.items():
        if field == "extracted_facts" or field == "token_count":
            continue
        if not isinstance(spec, dict):
            continue
        req = spec.get("required", False)
        r = check_required_top_level(records, field, prefix, req)
        if r:
            results.append(r)

    results.extend(check_pandas_type_match_week3(df, schema, prefix))
    results.extend(check_enum_conformance_week3(df, schema, prefix))

    if "doc_id" in schema:
        doc_spec = schema.get("doc_id") or {}
        if doc_spec.get("format") == "uuid":
            results.append(
                check_uuid_format(
                    records,
                    "doc_id",
                    prefix,
                    pattern=doc_spec.get("pattern"),
                )
            )

    results.extend(check_datetime_isoformat_week3(records, schema, prefix))

    if "fact_confidence" in df.columns and str(df["fact_confidence"].dtype) == "object":
        results.append(
            CheckResult(
                check_id=f"{prefix}.fact_confidence.dtype_mixed",
                column_name="fact_confidence",
                check_type="type",
                status="WARN",
                actual_value="dtype=object",
                expected="float64 for confidence",
                severity="WARNING",
                records_failing=0,
                sample_failing=[],
                message="fact_confidence is object dtype (mixed types). Treat as contract violation risk before generating or enforcing the contract.",
            )
        )

    if "doc_id" in schema:
        results.append(check_unique(records, "doc_id", prefix))

    sh = schema.get("source_hash") or {}
    pat = sh.get("pattern")
    if pat:
        pr = check_pattern_optional(records, "source_hash", pat, prefix, sh.get("required", False))
        if pr:
            results.append(pr)

    pms = schema.get("processing_time_ms") or {}
    if pms.get("minimum") is not None:
        results.append(check_int_minimum(records, "processing_time_ms", int(pms["minimum"]), prefix))

    ef = schema.get("extracted_facts") or {}
    items = ef.get("items") or {}
    conf = items.get("confidence") or {}
    if conf:
        results.append(
            check_extracted_facts_confidence(
                records,
                prefix,
                float(conf.get("minimum", 0.0)),
                float(conf.get("maximum", 1.0)),
            )
        )

    qual = ((contract.get("quality") or {}).get("specification") or {}).get("checks") or []
    if isinstance(qual, list):
        for line in qual:
            if isinstance(line, str):
                results.append(parse_quality_soda_line(line, records, prefix))

    return results


def _get_nested(rec: dict, path: str) -> Any:
    cur: Any = rec
    for p in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def checks_week5(contract: dict, records: List[dict], prefix: str) -> List[CheckResult]:
    results: List[CheckResult] = []
    schema = contract.get("schema") or {}

    for field, spec in schema.items():
        if field == "metadata" or field == "payload":
            continue
        if not isinstance(spec, dict):
            continue
        req = spec.get("required", False)
        r = check_required_top_level(records, field, prefix, req)
        if r:
            results.append(r)

    results.append(check_unique(records, "event_id", prefix))
    results.append(check_uuid_format(records, "event_id", prefix))
    results.append(check_uuid_format(records, "aggregate_id", prefix))

    meta = schema.get("metadata") or {}
    props = meta.get("properties") or {}
    for sub, sspec in props.items():
        if not isinstance(sspec, dict):
            continue
        path = f"metadata.{sub}"
        if sspec.get("required"):
            missing = sum(1 for r in records if _get_nested(r, path) is None)
            cid = f"{prefix}.{path}.required"
            if missing:
                results.append(
                    CheckResult(
                        check_id=cid,
                        column_name=path,
                        check_type="not_null",
                        status="FAIL",
                        actual_value=f"missing={missing}",
                        expected="required",
                        severity="CRITICAL",
                        records_failing=missing,
                        sample_failing=[],
                        message=f"Required nested field {path}",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        check_id=cid,
                        column_name=path,
                        check_type="not_null",
                        status="PASS",
                        actual_value="missing=0",
                        expected="required",
                        severity="LOW",
                        records_failing=0,
                        message="",
                    )
                )
        if sub == "correlation_id" and sspec.get("format") == "uuid":

            def corr(r):
                return _get_nested(r, "metadata.correlation_id")

            bad = 0
            samples: List[str] = []
            uu = re.compile(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
                re.I,
            )
            for r in records:
                v = corr(r)
                if v is None:
                    continue
                if not isinstance(v, str) or not uu.match(v):
                    bad += 1
                    if len(samples) < 3:
                        samples.append(str(v)[:40])
            cid = f"{prefix}.metadata.correlation_id.format"
            if bad:
                results.append(
                    CheckResult(
                        check_id=cid,
                        column_name="metadata.correlation_id",
                        check_type="format",
                        status="FAIL",
                        actual_value=f"invalid={bad}",
                        expected="uuid",
                        severity="CRITICAL",
                        records_failing=bad,
                        sample_failing=samples,
                        message="correlation_id must be UUID-shaped string.",
                    )
                )
            else:
                results.append(
                    CheckResult(
                        check_id=cid,
                        column_name="metadata.correlation_id",
                        check_type="format",
                        status="PASS",
                        actual_value="valid",
                        expected="uuid",
                        severity="LOW",
                        records_failing=0,
                        message="",
                    )
                )

    # recorded_at >= occurred_at (string ISO compare via parsing)
    from datetime import datetime as dt

    def parse_ts(s: Any) -> Optional[dt]:
        if not isinstance(s, str):
            return None
        try:
            if s.endswith("Z"):
                return dt.fromisoformat(s.replace("Z", "+00:00"))
            return dt.fromisoformat(s)
        except ValueError:
            return None

    bad_ts = 0
    samples_ts: List[str] = []
    for r in records:
        o = parse_ts(r.get("occurred_at"))
        rec = parse_ts(r.get("recorded_at"))
        if o is None or rec is None:
            continue
        if rec < o:
            bad_ts += 1
            if len(samples_ts) < 5:
                samples_ts.append(str(r.get("event_id", "")))
    cid = f"{prefix}.recorded_gte_occurred"
    if bad_ts:
        results.append(
            CheckResult(
                check_id=cid,
                column_name="recorded_at,occurred_at",
                check_type="temporal",
                status="FAIL",
                actual_value=f"violations={bad_ts}",
                expected="recorded_at >= occurred_at",
                severity="CRITICAL",
                records_failing=bad_ts,
                sample_failing=samples_ts,
                message="Event ordering: recorded_at before occurred_at.",
            )
        )
    else:
        results.append(
            CheckResult(
                check_id=cid,
                column_name="recorded_at,occurred_at",
                check_type="temporal",
                status="PASS",
                actual_value="ok",
                expected="recorded_at >= occurred_at",
                severity="LOW",
                records_failing=0,
                message="",
            )
        )

    # Monotonic sequence per aggregate_id
    by_agg: Dict[str, List[Tuple[int, str]]] = {}
    for r in records:
        aid = r.get("aggregate_id")
        seq = r.get("sequence_number")
        eid = r.get("event_id")
        if aid is None or seq is None:
            continue
        try:
            by_agg.setdefault(str(aid), []).append((int(seq), str(eid or "")))
        except (TypeError, ValueError):
            continue
    bad_seq = 0
    sample_seq: List[str] = []
    for _aid, lst in by_agg.items():
        lst.sort(key=lambda x: x[0])
        for i in range(1, len(lst)):
            prev_s, cur_s = lst[i - 1][0], lst[i][0]
            if cur_s != prev_s + 1:
                bad_seq += 1
                if len(sample_seq) < 5:
                    sample_seq.append(lst[i][1])
    cid = f"{prefix}.sequence_number.monotonic"
    if bad_seq:
        results.append(
            CheckResult(
                check_id=cid,
                column_name="sequence_number",
                check_type="sequence",
                status="FAIL",
                actual_value=f"non_monotonic_instances={bad_seq}",
                expected="strict +1 per aggregate_id",
                severity="CRITICAL",
                records_failing=bad_seq,
                sample_failing=sample_seq,
                message="sequence_number must increase by 1 per aggregate with no gaps.",
            )
        )
    else:
        results.append(
            CheckResult(
                check_id=cid,
                column_name="sequence_number",
                check_type="sequence",
                status="PASS",
                actual_value="monotonic",
                expected="+1 per aggregate",
                severity="LOW",
                records_failing=0,
                message="",
            )
        )

    return results


def checks_week4_lineage(contract: dict, records: List[dict], prefix: str) -> List[CheckResult]:
    results: List[CheckResult] = []
    if not records:
        results.append(
            CheckResult(
                check_id=f"{prefix}.snapshot.exists",
                column_name="*",
                check_type="volume",
                status="FAIL",
                actual_value="row_count=0",
                expected=">=1 snapshot",
                severity="CRITICAL",
                records_failing=0,
                message="No lineage snapshot rows.",
            )
        )
        return results
    snap = records[-1]
    sid = snap.get("snapshot_id")
    gc = snap.get("git_commit")
    nodes = snap.get("nodes") or []
    edges = snap.get("edges") or []
    node_ids = {n.get("node_id") for n in nodes if isinstance(n, dict)}

    if not sid:
        results.append(
            CheckResult(
                check_id=f"{prefix}.snapshot_id.required",
                column_name="snapshot_id",
                check_type="not_null",
                status="FAIL",
                actual_value="null",
                expected="uuid",
                severity="CRITICAL",
                records_failing=1,
                message="snapshot_id missing",
            )
        )
    else:
        results.append(
            CheckResult(
                check_id=f"{prefix}.snapshot_id.required",
                column_name="snapshot_id",
                check_type="not_null",
                status="PASS",
                actual_value="present",
                expected="uuid",
                severity="LOW",
                records_failing=0,
                message="",
            )
        )

    if not gc or not re.fullmatch(r"[a-f0-9]{40}", str(gc), re.I):
        results.append(
            CheckResult(
                check_id=f"{prefix}.git_commit.format",
                column_name="git_commit",
                check_type="pattern",
                status="FAIL",
                actual_value=str(gc)[:20],
                expected="40 hex chars",
                severity="CRITICAL",
                records_failing=1,
                message="git_commit must be 40-char SHA",
            )
        )
    else:
        results.append(
            CheckResult(
                check_id=f"{prefix}.git_commit.format",
                column_name="git_commit",
                check_type="pattern",
                status="PASS",
                actual_value="40 hex",
                expected="40 hex",
                severity="LOW",
                records_failing=0,
                message="",
            )
        )

    bad_edges = 0
    samples: List[str] = []
    rel_ok = {"IMPORTS", "CALLS", "READS", "WRITES", "PRODUCES", "CONSUMES"}
    for e in edges:
        if not isinstance(e, dict):
            continue
        s, t, rel = e.get("source"), e.get("target"), e.get("relationship")
        if s not in node_ids or t not in node_ids:
            bad_edges += 1
            if len(samples) < 5:
                samples.append(f"{s}->{t}")
        if rel not in rel_ok:
            bad_edges += 1
    cid = f"{prefix}.edges.endpoints"
    if bad_edges:
        results.append(
            CheckResult(
                check_id=cid,
                column_name="edges[*].source,target",
                check_type="referential",
                status="FAIL",
                actual_value=f"invalid_edges={bad_edges}",
                expected="source,target in nodes",
                severity="CRITICAL",
                records_failing=bad_edges,
                sample_failing=samples,
                message="Edge endpoints must resolve to node_id set.",
            )
        )
    else:
        results.append(
            CheckResult(
                check_id=cid,
                column_name="edges[*].source,target",
                check_type="referential",
                status="PASS",
                actual_value="all resolve",
                expected="nodes",
                severity="LOW",
                records_failing=0,
                message="",
            )
        )

    return results


def checks_langsmith(contract: dict, records: List[dict], prefix: str) -> List[CheckResult]:
    results: List[CheckResult] = []
    allowed = {"llm", "chain", "tool", "retriever", "embedding"}
    cid_rt = f"{prefix}.run_type.enum"
    bad_rt = 0
    samples_rt: List[str] = []
    for r in records:
        rt = r.get("run_type")
        if rt not in allowed:
            bad_rt += 1
            if len(samples_rt) < 5:
                samples_rt.append(str(r.get("id", "")))
    if bad_rt:
        results.append(
            CheckResult(
                check_id=cid_rt,
                column_name="run_type",
                check_type="accepted_values",
                status="FAIL",
                actual_value=f"invalid_count={bad_rt}",
                expected=str(sorted(allowed)),
                severity="CRITICAL",
                records_failing=bad_rt,
                sample_failing=samples_rt,
                message="run_type must be enum value.",
            )
        )
    elif records:
        results.append(
            CheckResult(
                check_id=cid_rt,
                column_name="run_type",
                check_type="accepted_values",
                status="PASS",
                actual_value="valid",
                expected=str(sorted(allowed)),
                severity="LOW",
                records_failing=0,
                message="",
            )
        )

    for r in records:
        st, en = r.get("start_time"), r.get("end_time")
        if st is None or en is None:
            continue
        # loose string compare if parse fails
        if str(st) >= str(en) and st and en:
            results.append(
                CheckResult(
                    check_id=f"{prefix}.timing.end_after_start",
                    column_name="end_time",
                    check_type="temporal",
                    status="FAIL",
                    actual_value=f"start={st}, end={en}",
                    expected="end_time > start_time",
                    severity="CRITICAL",
                    records_failing=1,
                    sample_failing=[str(r.get("id", ""))],
                    message="end_time must be after start_time when both set.",
                )
            )
            break
    else:
        if any(r.get("start_time") and r.get("end_time") for r in records):
            results.append(
                CheckResult(
                    check_id=f"{prefix}.timing.end_after_start",
                    column_name="end_time",
                    check_type="temporal",
                    status="PASS",
                    actual_value="ok",
                    expected="end > start",
                    severity="LOW",
                    records_failing=0,
                    message="",
                )
            )

    for r in records:
        tt, pt, ct = r.get("total_tokens"), r.get("prompt_tokens"), r.get("completion_tokens")
        if tt is None or pt is None or ct is None:
            continue
        try:
            if int(tt) != int(pt) + int(ct):
                results.append(
                    CheckResult(
                        check_id=f"{prefix}.tokens.sum",
                        column_name="total_tokens",
                        check_type="arithmetic",
                        status="FAIL",
                        actual_value=f"total={tt}, prompt+completion={int(pt)+int(ct)}",
                        expected="total_tokens = prompt_tokens + completion_tokens",
                        severity="CRITICAL",
                        records_failing=1,
                        sample_failing=[str(r.get("id", ""))],
                        message="Token identity check failed.",
                    )
                )
                break
        except (TypeError, ValueError):
            pass
    else:
        if any(
            r.get("total_tokens") is not None
            and r.get("prompt_tokens") is not None
            and r.get("completion_tokens") is not None
            for r in records
        ):
            results.append(
                CheckResult(
                    check_id=f"{prefix}.tokens.sum",
                    column_name="total_tokens",
                    check_type="arithmetic",
                    status="PASS",
                    actual_value="consistent",
                    expected="sum",
                    severity="LOW",
                    records_failing=0,
                    message="",
                )
            )

    return results


def run_checks(contract: dict, records: List[dict]) -> List[CheckResult]:
    cid = contract.get("id") or "unknown-contract"
    prefix = contract_prefix_from_id(cid)

    if cid == "week3-document-refinery-extractions":
        return checks_week3(contract, records, prefix)
    if cid == "week5-event-sourcing-events":
        return checks_week5(contract, records, prefix)
    if cid == "week4-brownfield-lineage-snapshot":
        return checks_week4_lineage(contract, records, prefix)
    if cid == "langsmith-trace-record-migrated":
        return checks_langsmith(contract, records, prefix)
    if "lineage-missing" in cid or cid.endswith("-placeholder"):
        return [
            CheckResult(
                check_id=f"{prefix}.contract.placeholder",
                column_name="*",
                check_type="config",
                status="ERROR",
                actual_value=cid,
                expected="generated snapshot contract",
                severity="WARNING",
                records_failing=0,
                message="Placeholder or missing-data contract; run ContractGenerator after migrations.",
            )
        ]

    return [
        CheckResult(
            check_id=f"{prefix}.contract.unsupported",
            column_name="*",
            check_type="config",
            status="ERROR",
            actual_value=cid,
            expected="registered contract id",
            severity="WARNING",
            records_failing=0,
            message="No validator bundle for this contract id; extend contracts/runner.run_checks.",
        )
    ]


class ValidationRunner:
    """
    Phase 2A ValidationRunner: load Bitol YAML + JSONL, evaluate checks, persist report.

    Use from code::

        runner = ValidationRunner(
            contract_path=Path("generated_contracts/week3_extractions.yaml"),
            output_path=Path("validation_reports/out.json"),
            data_path="outputs/week3/extractions.jsonl",
        )
        report = runner.run()
    """

    def __init__(
        self,
        contract_path: Path,
        output_path: Path,
        data_path: Optional[str] = None,
        *,
        reset_baselines: bool = False,
        injection_note: bool = False,
        no_attributor: bool = False,
        exit_zero: bool = False,
        lineage_path: Optional[Path] = None,
    ) -> None:
        self.contract_path = contract_path if contract_path.is_absolute() else REPO_ROOT / contract_path
        self.output_path = output_path if output_path.is_absolute() else REPO_ROOT / output_path
        self.data_path = data_path
        self.reset_baselines = reset_baselines
        self.injection_note = injection_note
        self.no_attributor = no_attributor
        self.exit_zero = exit_zero
        self.lineage_path = lineage_path

    def run(self) -> dict:
        contract = load_contract(self.contract_path)
        contract_id = contract.get("id") or "unknown-contract"
        prefix = contract_prefix_from_id(contract_id)

        try:
            data_path = resolve_data_path(contract, self.data_path)
        except ValueError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(2)

        if not data_path.is_file():
            rep = aggregate_report(contract_id, "", [], injection_note=self.injection_note)
            rep["snapshot_id"] = ""
            rep["results"] = [
                {
                    "check_id": f"{contract_prefix_from_id(contract_id)}.data.file",
                    "column_name": "*",
                    "check_type": "io",
                    "status": "ERROR",
                    "actual_value": str(data_path),
                    "expected": "readable jsonl file",
                    "severity": "CRITICAL",
                    "records_failing": 0,
                    "sample_failing": [],
                    "message": f"Data file not found: {data_path}",
                }
            ]
            rep["total_checks"] = 1
            rep["passed"] = 0
            rep["failed"] = 0
            rep["warned"] = 0
            rep["errored"] = 1
            self._write_report(rep)
            print(f"Wrote error report to {self.output_path}", file=sys.stderr)
            if not self.exit_zero:
                sys.exit(1)
            return rep

        snapshot_id = sha256_file(data_path)
        records = load_jsonl(data_path)

        results: List[CheckResult] = []
        try:
            results.extend(run_checks(contract, records))
        except Exception as exc:
            results.append(
                CheckResult(
                    check_id=f"{prefix}.runner.exception",
                    column_name="*",
                    check_type="system",
                    status="ERROR",
                    actual_value=str(exc),
                    expected="clean run",
                    severity="CRITICAL",
                    records_failing=0,
                    sample_failing=[],
                    message="Unexpected exception during checks; partial results.",
                )
            )

        baselines = load_baselines()
        try:
            results.extend(
                apply_drift_checks(
                    contract_id,
                    prefix,
                    records,
                    baselines,
                    self.reset_baselines,
                )
            )
        except Exception as exc:
            results.append(
                CheckResult(
                    check_id=f"{prefix}.drift.baseline",
                    column_name="*",
                    check_type="drift",
                    status="ERROR",
                    actual_value=str(exc),
                    expected="baseline load/save",
                    severity="WARNING",
                    records_failing=0,
                    sample_failing=[],
                    message="Baseline drift subsystem error.",
                )
            )

        save_baselines(baselines)

        report = aggregate_report(contract_id, snapshot_id, results, injection_note=self.injection_note)
        self._write_report(report)
        print(f"Wrote report to {self.output_path}", file=sys.stderr)

        if not self.no_attributor and any(r["status"] == "FAIL" for r in report["results"]):
            try:
                from contracts import attributor as attr_mod

                attr_mod.run_attribution(
                    report_path=self.output_path,
                    contract_path=self.contract_path,
                    lineage_path=self.lineage_path,
                    violation_log=REPO_ROOT / "violation_log" / "violations.jsonl",
                )
            except Exception as exc:
                print(f"ViolationAttributor failed (non-fatal): {exc}", file=sys.stderr)

        if not self.exit_zero and report.get("failed", 0) > 0:
            sys.exit(1)
        return report

    def _write_report(self, report: dict) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def aggregate_report(
    contract_id: str,
    snapshot_id: str,
    results: List[CheckResult],
    *,
    injection_note: bool = False,
) -> dict:
    passed = failed = warned = errored = 0
    for r in results:
        if r.status == "PASS":
            passed += 1
        elif r.status == "FAIL":
            failed += 1
        elif r.status == "WARN":
            warned += 1
        elif r.status == "ERROR":
            errored += 1
    rep: Dict[str, Any] = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "snapshot_id": snapshot_id,
        "run_timestamp": utc_now_iso(),
        "total_checks": len(results),
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": [result_to_dict(x) for x in results],
    }
    if injection_note:
        rep["injection_note"] = True
    return rep


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ValidationRunner (Phase 2A): contract clauses → structured JSON report.",
    )
    parser.add_argument("--contract", "-c", required=True, type=Path, help="Path to Bitol YAML contract")
    parser.add_argument("--data", "-d", type=str, default=None, help="JSONL snapshot (default: contract servers.local.path)")
    parser.add_argument("--output", "-o", required=True, type=Path, help="Validation report JSON path")
    parser.add_argument(
        "--lineage",
        type=Path,
        default=None,
        help="Week 4 lineage snapshot (JSON/JSONL). Default: outputs/week4/lineage_snapshots.jsonl or migrated_lineage.",
    )
    parser.add_argument(
        "--reset-baselines",
        action="store_true",
        help="Re-establish numeric drift baselines for this contract (ignores prior anchors).",
    )
    parser.add_argument(
        "--no-attributor",
        action="store_true",
        help="Do not invoke ViolationAttributor on FAIL results.",
    )
    parser.add_argument(
        "--injection-note",
        action="store_true",
        help="Set injection_note on the report (rubric flag for injected violations).",
    )
    parser.add_argument(
        "--exit-zero",
        action="store_true",
        help="Always exit 0 if a report was written (still exits 2 on CLI/config errors).",
    )
    args = parser.parse_args()

    lineage = args.lineage
    if lineage is not None and not lineage.is_absolute():
        lineage = REPO_ROOT / lineage

    runner = ValidationRunner(
        contract_path=args.contract,
        output_path=args.output,
        data_path=args.data,
        reset_baselines=args.reset_baselines,
        injection_note=args.injection_note,
        no_attributor=args.no_attributor,
        exit_zero=args.exit_zero,
        lineage_path=lineage,
    )
    runner.run()


if __name__ == "__main__":
    main()
