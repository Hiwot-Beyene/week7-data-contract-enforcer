#!/usr/bin/env python3
"""
ContractGenerator (Phase 1) — entry point.

Reads **migrated** JSONL under outputs/migrate/ (Week 3, 4, 5, LangSmith traces)
plus the Week 4 lineage snapshot for downstream context injection.

Pipeline:
  1. Structural profiling (ydata-profiling + privacy-preserving column shapes)
  2. Statistical profiling (numeric summaries; confidence 0–1 distribution flags)
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
    --output generated_contracts/
"""

from __future__ import annotations

import argparse
import hashlib
import json
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
GENERATED = REPO_ROOT / "generated_contracts"

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


def load_jsonl_records(path: Path) -> List[dict]:
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    out: List[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.loads(line))
    return out


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def numeric_stats(series: pd.Series) -> Optional[Dict[str, Any]]:
    sn = pd.to_numeric(series, errors="coerce").dropna()
    if sn.empty:
        return None
    stddev = float(sn.std(ddof=0)) if len(sn) > 1 else 0.0
    return {
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


def confidence_distribution_flags(series: pd.Series) -> Dict[str, Any]:
    sn = pd.to_numeric(series, errors="coerce").dropna()
    if sn.empty:
        return {"status": "no_numeric_confidence_samples"}
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
    }


# ---------------------------------------------------------------------------
# Step 3: lineage
# ---------------------------------------------------------------------------


def load_lineage_snapshot() -> Optional[dict]:
    recs = load_jsonl_records(MIGRATED["week4"])
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


def df_week3_extractions(records: List[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
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
    for r in records:
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
    return pd.DataFrame(rows)


def df_langsmith_traces(records: List[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
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
        sn = series.dropna()
        if len(sn):
            result["stats"] = {
                "min": float(sn.min()),
                "max": float(sn.max()),
                "mean": float(sn.mean()),
                "p25": float(sn.quantile(0.25)),
                "p50": float(sn.quantile(0.50)),
                "p75": float(sn.quantile(0.75)),
                "p95": float(sn.quantile(0.95)),
                "p99": float(sn.quantile(0.99)),
                "stddev": float(sn.std()),
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
    clause: Dict[str, Any] = {
        "type": infer_type(profile["dtype"]),
        "required": profile["null_fraction"] == 0.0,
    }
    name = profile["name"]
    if "confidence" in name and clause["type"] == "number":
        clause["minimum"] = 0.0
        clause["maximum"] = 1.0
        clause["description"] = (
            "Confidence score. Must remain 0.0–1.0 float. BREAKING if changed to 0–100."
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
) -> None:
    records = load_jsonl_records(source)
    df = flatten_for_profile(records)
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

    contract = build_contract_common(
        contract_id=contract_id,
        title="Week 3 Document Refinery — Extraction Records",
        description="Generated from profiled JSONL via CLI; clauses map nullability, types, and confidence bounds.",
        owner="week3-team",
        relative_data_path=rel_data,
        schema=schema,
        quality_checks=[
            "missing_count(doc_id) = 0",
            "duplicate_count(doc_id) = 0",
            "min(primary_fact_confidence) >= 0.0",
            "max(primary_fact_confidence) <= 1.0",
            "row_count >= 1",
        ],
        lineage_upstream=[],
        lineage_downstream=[],
        profiling={
            "engine": "ydata-profiling",
            "generated_at": utc_now_iso(),
            "structural": {
                "ydata_profiling": yprof,
                "column_profiles": list(column_profiles.values()),
                "contract_violations": contract_violations,
            },
        },
        llm_annotations=llm_annotations,
    )
    inject_lineage_instruction(contract, lineage_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    primary_out = output_dir / f"{contract_id}.yaml"
    write_yaml(primary_out, contract)
    if contract_id == "week3-document-refinery-extractions":
        write_yaml(output_dir / "week3_extractions.yaml", contract)
    write_contract_schema_snapshot(repo_root, contract_id, schema)
    write_dbt_schema(
        output_dir / "week3_extractions_dbt.yml",
        "contract_sources",
        "week3_extractions",
        dbt_week3_extraction_columns(),
    )
    print(f"Wrote {primary_out} (and week3_extractions.yaml when id matches)", file=sys.stderr)


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
    parser.add_argument("--no-llm", action="store_true", help="Skip OpenRouter schema annotations")
    parser.add_argument(
        "--openrouter-model",
        default=os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini"),
        help="OpenRouter model id",
    )
    args = parser.parse_args()
    use_llm = not args.no_llm and bool(os.environ.get("OPENROUTER_API_KEY"))

    if args.source is not None:
        if not args.contract_id:
            parser.error("--contract-id is required when using --source")
        if args.contract_id != "week3-document-refinery-extractions":
            parser.error("CLI --source mode currently supports contract-id week3-document-refinery-extractions only")
        src = Path(args.source)
        if not src.is_absolute():
            src = REPO_ROOT / src
        lin = Path(args.lineage or "outputs/week4/lineage_snapshots.jsonl")
        if not lin.is_absolute():
            lin = REPO_ROOT / lin
        out = Path(args.output or "generated_contracts")
        if not out.is_absolute():
            out = REPO_ROOT / out
        run_targeted_extractions_contract(
            source=src,
            contract_id=args.contract_id,
            lineage_path=lin,
            output_dir=out,
            repo_root=REPO_ROOT,
            use_llm=use_llm,
            openrouter_model=args.openrouter_model,
        )
        return

    GENERATED.mkdir(parents=True, exist_ok=True)
    snapshot = load_lineage_snapshot()

    # --- Week 3 ---
    w3_records = load_jsonl_records(MIGRATED["week3"])
    df3 = df_week3_extractions(w3_records)
    structural3 = structural_column_profiles(df3)
    y3 = ydata_profile_summary(df3, "week3_extractions")
    num3 = {c: numeric_stats(df3[c]) for c in df3.columns if numeric_stats(df3[c])}
    conf3 = confidence_distribution_flags(df3["primary_fact_confidence"]) if not df3.empty else {}

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
            "structural": {"ydata_profiling": y3, "columns": structural3, "numeric": num3, "confidence_distribution": conf3},
        },
        llm_annotations=llm3,
    )
    write_yaml(GENERATED / "week3_extractions.yaml", contract3)
    write_contract_schema_snapshot(REPO_ROOT, contract3["id"], contract3.get("schema") or {})
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
        write_yaml(GENERATED / "week4_lineage.yaml", contract4)
        write_contract_schema_snapshot(REPO_ROOT, contract4["id"], contract4.get("schema") or {})
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
        write_yaml(GENERATED / "week4_lineage.yaml", _ph4)
        write_contract_schema_snapshot(REPO_ROOT, _ph4["id"], _ph4.get("schema") or {})
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
    w5 = load_jsonl_records(MIGRATED["week5"])
    df5 = df_week5_events(w5)
    st5 = structural_column_profiles(df5)
    y5 = ydata_profile_summary(df5, "week5_events")
    num5 = {c: numeric_stats(df5[c]) for c in df5.columns if numeric_stats(df5[c])}
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
            "structural": {"ydata_profiling": y5, "columns": st5, "numeric": num5},
        },
        llm_annotations=llm5,
    )
    write_yaml(GENERATED / "week5_events.yaml", contract5)
    write_contract_schema_snapshot(REPO_ROOT, contract5["id"], contract5.get("schema") or {})
    write_dbt_schema(
        GENERATED / "week5_events_dbt.yml",
        "contract_sources",
        "week5_events",
        dbt_week5_event_columns(),
    )

    # --- LangSmith traces (migrated) ---
    tr = load_jsonl_records(MIGRATED["traces"])
    dft = df_langsmith_traces(tr)
    stt = structural_column_profiles(dft)
    yt = ydata_profile_summary(dft, "langsmith_traces")
    numt = {c: numeric_stats(dft[c]) for c in dft.columns if numeric_stats(dft[c])}
    trace_numeric_note = {
        "outputs_key_count_stats": numeric_stats(dft["outputs_key_count"]) if "outputs_key_count" in dft.columns else None,
    }
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
            },
        },
        llm_annotations=llmt,
    )
    write_yaml(GENERATED / "langsmith_traces.yaml", contract_t)
    write_contract_schema_snapshot(REPO_ROOT, contract_t["id"], contract_t.get("schema") or {})
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
    main()
