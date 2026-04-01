"""
Shared check execution helpers for ValidationRunner (Phase 2A).
Pure functions where possible; no I/O.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field, replace
from datetime import datetime as dt
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
_HEX40 = re.compile(r"^[a-f0-9]{40}$", re.I)
_SHA256 = re.compile(r"^[a-f0-9]{64}$", re.I)


@dataclass
class CheckResult:
    check_id: str
    column_name: str
    check_type: str
    status: str  # PASS | FAIL | WARN | ERROR
    actual_value: str
    expected: str
    severity: str
    records_failing: int
    sample_failing: List[str] = field(default_factory=list)
    message: str = ""


def _iso_z(ts: str) -> bool:
    if not isinstance(ts, str) or not ts:
        return False
    return bool(re.search(r"Z$|[+-]\d{2}:\d{2}$", ts))


def get_nested(obj: Any, path: str) -> Any:
    """path like 'metadata.correlation_id' or single key."""
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def primary_fact_confidence(rec: dict) -> Optional[float]:
    facts = rec.get("extracted_facts")
    if not isinstance(facts, list) or not facts:
        return None
    f0 = facts[0]
    if not isinstance(f0, dict):
        return None
    c = f0.get("confidence")
    if c is None:
        return None
    try:
        return float(c)
    except (TypeError, ValueError):
        return None


def check_required_top_level(
    records: List[dict],
    field: str,
    contract_prefix: str,
    required: bool,
) -> Optional[CheckResult]:
    if not required:
        return None
    cid = f"{contract_prefix}.{field}.required"
    missing = 0
    samples: List[str] = []
    for rec in records:
        if field not in rec or rec.get(field) is None:
            missing += 1
            if len(samples) < 5:
                samples.append(str(rec.get("doc_id") or rec.get("event_id") or rec.get("id") or "row"))
    if missing:
        return CheckResult(
            check_id=cid,
            column_name=field,
            check_type="not_null",
            status="FAIL",
            actual_value=f"missing_count={missing}",
            expected="all non-null",
            severity="CRITICAL",
            records_failing=missing,
            sample_failing=samples,
            message=f"Required field {field!r} is null or absent on {missing} record(s).",
        )
    return CheckResult(
        check_id=cid,
        column_name=field,
        check_type="not_null",
        status="PASS",
        actual_value="missing_count=0",
        expected="all non-null",
        severity="LOW",
        records_failing=0,
        message="",
    )


def check_unique(
    records: List[dict],
    field: str,
    contract_prefix: str,
) -> CheckResult:
    cid = f"{contract_prefix}.{field}.unique"
    seen: Dict[Any, int] = {}
    dups = 0
    dup_vals: List[str] = []
    for rec in records:
        v = rec.get(field)
        if v is None:
            continue
        seen[v] = seen.get(v, 0) + 1
    for v, c in seen.items():
        if c > 1:
            dups += c
            if len(dup_vals) < 5:
                dup_vals.append(str(v))
    if dup_vals:
        return CheckResult(
            check_id=cid,
            column_name=field,
            check_type="unique",
            status="FAIL",
            actual_value=f"duplicate_values={dup_vals[:3]}",
            expected="unique",
            severity="CRITICAL",
            records_failing=dups,
            sample_failing=dup_vals,
            message=f"Duplicate values for {field!r}.",
        )
    return CheckResult(
        check_id=cid,
        column_name=field,
        check_type="unique",
        status="PASS",
        actual_value="duplicate_count=0",
        expected="unique",
        severity="LOW",
        records_failing=0,
        message="",
    )


def check_uuid_format(
    records: List[dict],
    field: str,
    contract_prefix: str,
    pattern: Optional[str] = None,
) -> CheckResult:
    """If contract supplies pattern (e.g. ^[0-9a-f-]{36}$), use it; else RFC 4122."""
    cid = f"{contract_prefix}.{field}.format.uuid"
    regex = re.compile(pattern, re.I) if pattern else _UUID_RE
    expected_label = pattern or "RFC 4122 UUID string"
    bad = 0
    samples: List[str] = []
    if len(records) > 10_000:
        import random

        rng = random.Random(42)
        idxs = rng.sample(range(len(records)), min(100, len(records)))
        subset = [records[i] for i in idxs]
        sampled = True
    else:
        subset = records
        sampled = False
    for rec in subset:
        v = rec.get(field)
        if v is None:
            continue
        if not isinstance(v, str) or not regex.match(v):
            bad += 1
            if len(samples) < 5:
                samples.append(str(v)[:64])
    if bad:
        return CheckResult(
            check_id=cid,
            column_name=field,
            check_type="format",
            status="FAIL",
            actual_value=f"invalid_uuid_count={bad}" + (" (100-row sample)" if sampled else ""),
            expected=expected_label,
            severity="CRITICAL",
            records_failing=bad,
            sample_failing=samples,
            message=f"Field {field!r} does not match UUID pattern.",
        )
    return CheckResult(
        check_id=cid,
        column_name=field,
        check_type="format",
        status="PASS",
        actual_value="all valid uuid shape" + (" (100-row sample)" if sampled else ""),
        expected="uuid",
        severity="LOW",
        records_failing=0,
        message="",
    )


def flatten_extractions_for_profile(records: List[dict]) -> pd.DataFrame:
    """One row per extracted_fact item; aligns with ContractGenerator profiling."""
    rows: List[dict] = []
    for r in records:
        base = {k: v for k, v in r.items() if not isinstance(v, (list, dict))}
        facts = r.get("extracted_facts")
        if not facts:
            facts = [{}]
        elif not isinstance(facts, list):
            facts = [{}]
        for fact in facts:
            if not isinstance(fact, dict):
                fact = {}
            rows.append({**base, **{f"fact_{k}": v for k, v in fact.items()}})
    return pd.DataFrame(rows)


def _parse_iso8601_value(v: Any) -> Optional[dt]:
    if not isinstance(v, str) or not v.strip():
        return None
    s = v.strip()
    try:
        if s.endswith("Z"):
            return dt.fromisoformat(s.replace("Z", "+00:00"))
        return dt.fromisoformat(s)
    except ValueError:
        return None


def check_pandas_type_match_week3(df: pd.DataFrame, schema: dict, contract_prefix: str) -> List[CheckResult]:
    """Contract type number → numeric dtype; integer → integer-like in flattened profile frame."""
    skip = frozenset({"extracted_facts", "entities", "token_count"})
    out: List[CheckResult] = []
    for field, spec in schema.items():
        if field in skip or not isinstance(spec, dict):
            continue
        typ = spec.get("type")
        if typ not in ("number", "integer"):
            continue
        cid = f"{contract_prefix}.{field}.type_match"
        if field not in df.columns:
            out.append(
                CheckResult(
                    check_id=cid,
                    column_name=field,
                    check_type="type",
                    status="ERROR",
                    actual_value="column_absent",
                    expected=f"profiled column for schema.{field} ({typ})",
                    severity="WARNING",
                    records_failing=0,
                    sample_failing=[],
                    message=f"Cannot run type check: flattened profile has no column {field!r}.",
                )
            )
            continue
        s = df[field]
        if typ == "number":
            if not pd.api.types.is_numeric_dtype(s):
                out.append(
                    CheckResult(
                        check_id=cid,
                        column_name=field,
                        check_type="type",
                        status="FAIL",
                        actual_value=f"dtype={s.dtype}",
                        expected="float64 or int64 (numeric)",
                        severity="CRITICAL",
                        records_failing=int(s.notna().sum()),
                        sample_failing=[],
                        message=f"Contract type number but column {field!r} is not numeric in profiled DataFrame.",
                    )
                )
            else:
                out.append(
                    CheckResult(
                        check_id=cid,
                        column_name=field,
                        check_type="type",
                        status="PASS",
                        actual_value=str(s.dtype),
                        expected="numeric",
                        severity="LOW",
                        records_failing=0,
                        message="",
                    )
                )
        elif typ == "integer":
            ok = False
            if pd.api.types.is_integer_dtype(s):
                ok = True
            elif pd.api.types.is_float_dtype(s):
                sn = s.dropna()
                if len(sn) and np.allclose(sn.values, np.round(sn.values), rtol=0, atol=0):
                    ok = True
            if not ok:
                out.append(
                    CheckResult(
                        check_id=cid,
                        column_name=field,
                        check_type="type",
                        status="FAIL",
                        actual_value=f"dtype={s.dtype}",
                        expected="integer",
                        severity="CRITICAL",
                        records_failing=int(s.notna().sum()),
                        sample_failing=[],
                        message=f"Contract type integer but column {field!r} is not integer-like.",
                    )
                )
            else:
                out.append(
                    CheckResult(
                        check_id=cid,
                        column_name=field,
                        check_type="type",
                        status="PASS",
                        actual_value=str(s.dtype),
                        expected="integer",
                        severity="LOW",
                        records_failing=0,
                        message="",
                    )
                )
    return out


def check_enum_conformance_week3(df: pd.DataFrame, schema: dict, contract_prefix: str) -> List[CheckResult]:
    out: List[CheckResult] = []
    for field, spec in schema.items():
        if not isinstance(spec, dict):
            continue
        enum = spec.get("enum")
        if not isinstance(enum, list):
            continue
        cid = f"{contract_prefix}.{field}.enum"
        if field not in df.columns:
            out.append(
                CheckResult(
                    check_id=cid,
                    column_name=field,
                    check_type="enum",
                    status="ERROR",
                    actual_value="column_absent",
                    expected=str(enum),
                    severity="WARNING",
                    records_failing=0,
                    sample_failing=[],
                    message=f"Cannot run enum check: no column {field!r} in profiled data.",
                )
            )
            continue
        allowed = {str(x) for x in enum}
        s = df[field].dropna()
        bad_vals: List[str] = []
        bad_count = 0
        for v in s.unique():
            if str(v) not in allowed and v not in enum:
                bad_count += int((s == v).sum())
                if len(bad_vals) < 5:
                    bad_vals.append(str(v)[:80])
        if bad_count:
            out.append(
                CheckResult(
                    check_id=cid,
                    column_name=field,
                    check_type="enum",
                    status="FAIL",
                    actual_value=f"non_conforming_count={bad_count}",
                    expected=str(enum),
                    severity="CRITICAL",
                    records_failing=bad_count,
                    sample_failing=bad_vals,
                    message="Values outside contract enum.",
                )
            )
        else:
            out.append(
                CheckResult(
                    check_id=cid,
                    column_name=field,
                    check_type="enum",
                    status="PASS",
                    actual_value="all values in enum",
                    expected=str(enum),
                    severity="LOW",
                    records_failing=0,
                    message="",
                )
            )
    return out


def check_datetime_isoformat_week3(records: List[dict], schema: dict, contract_prefix: str) -> List[CheckResult]:
    out: List[CheckResult] = []
    for field, spec in schema.items():
        if not isinstance(spec, dict) or spec.get("format") != "date-time":
            continue
        cid = f"{contract_prefix}.{field}.format.date-time"
        bad = 0
        samples: List[str] = []
        for rec in records:
            v = rec.get(field)
            if v is None:
                continue
            if _parse_iso8601_value(v) is None:
                bad += 1
                if len(samples) < 5:
                    samples.append(str(v)[:64])
        if bad:
            out.append(
                CheckResult(
                    check_id=cid,
                    column_name=field,
                    check_type="format",
                    status="FAIL",
                    actual_value=f"unparseable_count={bad}",
                    expected="datetime.fromisoformat compatible",
                    severity="CRITICAL",
                    records_failing=bad,
                    sample_failing=samples,
                    message="date-time field has values that do not parse as ISO-8601.",
                )
            )
        else:
            out.append(
                CheckResult(
                    check_id=cid,
                    column_name=field,
                    check_type="format",
                    status="PASS",
                    actual_value="all parseable",
                    expected="ISO-8601",
                    severity="LOW",
                    records_failing=0,
                    message="",
                )
            )
    return out


def check_extracted_facts_confidence(
    records: List[dict],
    contract_prefix: str,
    minimum: float = 0.0,
    maximum: float = 1.0,
) -> CheckResult:
    cid = f"{contract_prefix}.extracted_facts.confidence.range"
    bad = 0
    samples: List[str] = []
    all_vals: List[float] = []
    out_of_range_vals: List[float] = []
    for rec in records:
        facts = rec.get("extracted_facts")
        if not isinstance(facts, list):
            continue
        for fact in facts:
            if not isinstance(fact, dict):
                continue
            cid_fact = fact.get("fact_id")
            try:
                c = (
                    float(fact.get("confidence"))
                    if fact.get("confidence") is not None
                    else None
                )
            except (TypeError, ValueError):
                c = None
            if c is None:
                continue
            all_vals.append(c)
            if c < minimum or c > maximum:
                bad += 1
                out_of_range_vals.append(c)
                if len(samples) < 5:
                    samples.append(str(cid_fact or "unknown_fact"))
    if bad:
        mx = max(out_of_range_vals) if out_of_range_vals else 0.0
        mn = min(out_of_range_vals) if out_of_range_vals else 0.0
        mean_all = sum(all_vals) / len(all_vals) if all_vals else 0.0
        max_all = max(all_vals) if all_vals else 0.0
        return CheckResult(
            check_id=cid,
            column_name="extracted_facts[*].confidence",
            check_type="range",
            status="FAIL",
            actual_value=f"max={max_all:.4g}, mean={mean_all:.4g} (out_of_range={bad}, max_bad={mx:.4g}, min_bad={mn:.4g})",
            expected=f"max<={maximum}, min>={minimum}",
            severity="CRITICAL",
            records_failing=bad,
            sample_failing=samples,
            message="confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."
            if max_all > 1.0
            else "One or more fact confidence values violate the contract range.",
        )
    return CheckResult(
        check_id=cid,
        column_name="extracted_facts[*].confidence",
        check_type="range",
        status="PASS",
        actual_value=f"all in [{minimum},{maximum}]",
        expected=f"min>={minimum}, max<={maximum}",
        severity="LOW",
        records_failing=0,
        message="",
    )


def check_pattern_optional(
    records: List[dict],
    field: str,
    pattern: str,
    contract_prefix: str,
    required: bool,
) -> Optional[CheckResult]:
    if not pattern:
        return None
    rx = re.compile(pattern)
    cid = f"{contract_prefix}.{field}.pattern"
    bad = 0
    samples: List[str] = []
    for rec in records:
        v = rec.get(field)
        if v is None:
            if required:
                pass  # handled by required
            continue
        if not isinstance(v, str) or not rx.match(v):
            bad += 1
            if len(samples) < 5:
                samples.append(str(v)[:40])
    if bad:
        return CheckResult(
            check_id=cid,
            column_name=field,
            check_type="pattern",
            status="FAIL",
            actual_value=f"pattern_mismatch={bad}",
            expected=pattern,
            severity="CRITICAL",
            records_failing=bad,
            sample_failing=samples,
            message=f"Field {field!r} does not match pattern.",
        )
    return CheckResult(
        check_id=cid,
        column_name=field,
        check_type="pattern",
        status="PASS",
        actual_value="all match or null",
        expected=pattern,
        severity="LOW",
        records_failing=0,
        message="",
    )


def check_int_minimum(
    records: List[dict],
    field: str,
    minimum: int,
    contract_prefix: str,
) -> CheckResult:
    cid = f"{contract_prefix}.{field}.minimum"
    bad = 0
    samples: List[str] = []
    for rec in records:
        v = rec.get(field)
        if v is None:
            continue
        try:
            iv = int(v)
        except (TypeError, ValueError):
            bad += 1
            if len(samples) < 5:
                samples.append(str(rec.get("doc_id") or ""))
            continue
        if iv < minimum:
            bad += 1
            if len(samples) < 5:
                samples.append(str(rec.get("doc_id") or ""))
    if bad:
        return CheckResult(
            check_id=cid,
            column_name=field,
            check_type="range",
            status="FAIL",
            actual_value=f"below_minimum_count={bad}",
            expected=f">= {minimum}",
            severity="CRITICAL",
            records_failing=bad,
            sample_failing=samples,
            message=f"Integer {field!r} must be >= {minimum}.",
        )
    return CheckResult(
        check_id=cid,
        column_name=field,
        check_type="range",
        status="PASS",
        actual_value=f"all >= {minimum}",
        expected=f">= {minimum}",
        severity="LOW",
        records_failing=0,
        message="",
    )


def check_numeric_drift(
    contract_id: str,
    column_key: str,
    column_label: str,
    current_mean: float,
    current_std: float,
    baseline_mean: float,
    baseline_std: float,
    contract_prefix: str,
) -> CheckResult:
    """2σ -> WARN, 3σ -> FAIL on mean shift (baseline_std floor for division)."""
    cid = f"{contract_prefix}.{column_key}.drift.mean"
    if baseline_std < 1e-9:
        baseline_std = 1e-9
    delta = abs(current_mean - baseline_mean)
    z = delta / baseline_std
    actual = f"current_mean={current_mean:.6g}, baseline_mean={baseline_mean:.6g}, z={z:.3f}_std"
    expected = "within 3 std of baseline mean (FAIL beyond); WARN beyond 2 std"
    if z > 3:
        return CheckResult(
            check_id=cid,
            column_name=column_label,
            check_type="drift",
            status="FAIL",
            actual_value=actual,
            expected=expected,
            severity="HIGH",
            records_failing=0,
            message=f"{column_label} mean drifted {z:.1f} stddev from baseline",
        )
    if z > 2:
        return CheckResult(
            check_id=cid,
            column_name=column_label,
            check_type="drift",
            status="WARN",
            actual_value=actual,
            expected=expected,
            severity="MEDIUM",
            records_failing=0,
            message=f"{column_label} mean within warning range ({z:.1f} stddev)",
        )
    return CheckResult(
        check_id=cid,
        column_name=column_label,
        check_type="drift",
        status="PASS",
        actual_value=actual,
        expected=expected,
        severity="LOW",
        records_failing=0,
        message="",
    )


def parse_quality_soda_line(
    line: str,
    records: List[dict],
    contract_prefix: str,
) -> Optional[CheckResult]:
    line = line.strip()
    m = re.match(r"missing_count\((\w+)\)\s*=\s*0", line)
    if m:
        col = m.group(1)
        if col == "primary_fact_confidence":
            missing = sum(1 for r in records if primary_fact_confidence(r) is None)
        else:
            missing = sum(1 for r in records if r.get(col) is None)
        cid = f"{contract_prefix}.quality.{col}.missing_count"
        if missing:
            return CheckResult(
                check_id=cid,
                column_name=col,
                check_type="completeness",
                status="FAIL",
                actual_value=f"missing_count={missing}",
                expected="0",
                severity="CRITICAL",
                records_failing=missing,
                sample_failing=[],
                message=line,
            )
        return CheckResult(
            check_id=cid,
            column_name=col,
            check_type="completeness",
            status="PASS",
            actual_value="missing_count=0",
            expected="0",
            severity="LOW",
            records_failing=0,
            message=line,
        )
    m = re.match(r"duplicate_count\((\w+)\)\s*=\s*0", line)
    if m:
        col = m.group(1)
        base = check_unique(records, col, contract_prefix)
        return replace(
            base,
            check_id=f"{contract_prefix}.quality.{col}.duplicate_count",
            message=line,
        )
    m = re.match(r"min\(primary_fact_confidence\)\s*>=\s*([\d.]+)", line)
    if m:
        bound = float(m.group(1))
        vals = [primary_fact_confidence(r) for r in records]
        vals = [v for v in vals if v is not None]
        if not vals:
            return CheckResult(
                check_id=f"{contract_prefix}.quality.primary_fact_confidence.min",
                column_name="primary_fact_confidence",
                check_type="range",
                status="ERROR",
                actual_value="no samples",
                expected=f">= {bound}",
                severity="WARNING",
                records_failing=0,
                message="Cannot compute min(primary_fact_confidence): no values.",
            )
        mn = min(vals)
        cid = f"{contract_prefix}.quality.primary_fact_confidence.min"
        if mn < bound:
            return CheckResult(
                check_id=cid,
                column_name="primary_fact_confidence",
                check_type="range",
                status="FAIL",
                actual_value=f"min={mn}",
                expected=f">= {bound}",
                severity="CRITICAL",
                records_failing=sum(1 for v in vals if v < bound),
                sample_failing=[],
                message=line,
            )
        return CheckResult(
            check_id=cid,
            column_name="primary_fact_confidence",
            check_type="range",
            status="PASS",
            actual_value=f"min={mn}",
            expected=f">= {bound}",
            severity="LOW",
            records_failing=0,
            message=line,
        )
    m = re.match(r"max\(primary_fact_confidence\)\s*<=\s*([\d.]+)", line)
    if m:
        bound = float(m.group(1))
        vals = [primary_fact_confidence(r) for r in records]
        vals = [v for v in vals if v is not None]
        if not vals:
            return CheckResult(
                check_id=f"{contract_prefix}.quality.primary_fact_confidence.max",
                column_name="primary_fact_confidence",
                check_type="range",
                status="ERROR",
                actual_value="no samples",
                expected=f"<= {bound}",
                severity="WARNING",
                records_failing=0,
                message=line,
            )
        mx = max(vals)
        cid = f"{contract_prefix}.quality.primary_fact_confidence.max"
        if mx > bound:
            return CheckResult(
                check_id=cid,
                column_name="primary_fact_confidence",
                check_type="range",
                status="FAIL",
                actual_value=f"max={mx}, mean={sum(vals)/len(vals):.4f}",
                expected=f"max<={bound}, min>=0.0",
                severity="CRITICAL",
                records_failing=sum(1 for v in vals if v > bound),
                sample_failing=[],
                message="confidence is in 0–100 range, not 0.0–1.0. Breaking change detected."
                if mx > 1.0
                else line,
            )
        return CheckResult(
            check_id=cid,
            column_name="primary_fact_confidence",
            check_type="range",
            status="PASS",
            actual_value=f"max={mx}",
            expected=f"<= {bound}",
            severity="LOW",
            records_failing=0,
            message=line,
        )
    m = re.match(r"row_count\s*>=\s*(\d+)", line)
    if m:
        need = int(m.group(1))
        n = len(records)
        cid = f"{contract_prefix}.quality.row_count"
        if n < need:
            return CheckResult(
                check_id=cid,
                column_name="*",
                check_type="volume",
                status="FAIL",
                actual_value=f"row_count={n}",
                expected=f">= {need}",
                severity="CRITICAL",
                records_failing=n,
                sample_failing=[],
                message=line,
            )
        return CheckResult(
            check_id=cid,
            column_name="*",
            check_type="volume",
            status="PASS",
            actual_value=f"row_count={n}",
            expected=f">= {need}",
            severity="LOW",
            records_failing=0,
            message=line,
        )
    return CheckResult(
        check_id=f"{contract_prefix}.quality.unparsed",
        column_name="*",
        check_type="custom",
        status="ERROR",
        actual_value=line,
        expected="supported soda subset",
        severity="WARNING",
        records_failing=0,
        message="Quality line not parsed; extend runner parser if needed.",
    )


def result_to_dict(cr: CheckResult) -> dict:
    return {
        "check_id": cr.check_id,
        "column_name": cr.column_name,
        "check_type": cr.check_type,
        "status": cr.status,
        "actual_value": cr.actual_value,
        "expected": cr.expected,
        "severity": cr.severity,
        "records_failing": cr.records_failing,
        "sample_failing": cr.sample_failing,
        "message": cr.message,
    }


def contract_prefix_from_id(contract_id: str) -> str:
    return contract_id.split("-")[0] if contract_id else "contract"
