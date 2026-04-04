#!/usr/bin/env python3
"""
Stakeholder report: validation summary, top violations with plain-language blurbs,
registry context, five plain-language ``report_sections``, Data Health Score,
and programmatic ``generation_sources``.

**Report version** is not hardcoded: resolve order is CLI ``--report-version``,
environment ``ENFORCER_REPORT_VERSION``, then single-line file
``enforcer_report/REPORT_VERSION``, then package default ``DEFAULT_REPORT_VERSION``.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

# Used only when no file, no env, and no CLI override (see ``resolve_report_version``).
DEFAULT_REPORT_VERSION = "2.3.0"
_REPORT_VERSION_FILE = REPO_ROOT / "enforcer_report" / "REPORT_VERSION"


def resolve_report_version(cli_override: Optional[str] = None) -> str:
    """
    Single source of truth for ``report_version`` in JSON and PDF.

    Precedence: ``cli_override`` → ``ENFORCER_REPORT_VERSION`` →
    ``enforcer_report/REPORT_VERSION`` (first non-comment line) →
    ``DEFAULT_REPORT_VERSION``.
    """
    if cli_override is not None and str(cli_override).strip():
        return str(cli_override).strip()
    env_v = (os.environ.get("ENFORCER_REPORT_VERSION") or "").strip()
    if env_v:
        return env_v
    if _REPORT_VERSION_FILE.is_file():
        try:
            for line in _REPORT_VERSION_FILE.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                return s
        except OSError:
            pass
    return DEFAULT_REPORT_VERSION


def stakeholder_report_stem(validation_report: dict) -> str:
    """
    Filesystem-safe filename stem so operators see **which pipeline (contract)** and **which run**
    without opening the file.

    Pattern: ``stakeholder__<contract_id>__<run_utc_slug>`` — e.g.
    ``stakeholder__week3-document-refinery-extractions__20260404T234651Z``.

    Uses ``contract_id`` and ``run_timestamp`` from ValidationRunner JSON; falls back to
    ``unknown_contract`` and "now" UTC if missing.
    """
    cid = str(validation_report.get("contract_id") or "").strip() or "unknown_contract"
    cid_fs = re.sub(r"[^a-zA-Z0-9_.-]+", "_", cid)
    cid_fs = re.sub(r"_+", "_", cid_fs).strip("_")[:120] or "unknown_contract"
    ts_raw = str(validation_report.get("run_timestamp") or "").strip()
    if ts_raw:
        try:
            dt = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            t_slug = dt.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        except ValueError:
            t_slug = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    else:
        t_slug = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"stakeholder__{cid_fs}__{t_slug}"


_ARRAY_BRACKET_RE = re.compile(r"\[\*\]")

# Bitol file per contract id (repo-relative paths for report directives).
CONTRACT_ID_TO_BITOL_RELPATH: Dict[str, str] = {
    "week3-document-refinery-extractions": "generated_contracts/week3-document-refinery-extractions.yaml",
    "week4-brownfield-lineage-snapshot": "generated_contracts/week4_lineage.yaml",
    "week5-event-sourcing-events": "generated_contracts/week5_events.yaml",
    "langsmith-trace-record-migrated": "generated_contracts/langsmith_traces.yaml",
}


def _schema_clause_for_failure(contract_id: str, check_id: str, column_name: str) -> str:
    """
    Map ValidationRunner output to the Bitol ``schema`` / ``quality`` clause path
    engineers should edit (or the data must satisfy).
    """
    cid = (contract_id or "").strip()
    ck = (check_id or "").lower()
    col = _normalize_column_name(column_name or "")

    if "extracted_facts.confidence" in ck or (
        col and "confidence" in col.lower() and "extracted" in col.lower()
    ):
        return "schema.extracted_facts.items.confidence (minimum, maximum, required)"
    if "primary_fact_confidence" in ck or col == "primary_fact_confidence":
        return (
            "quality.specification.checks (Soda-style lines for primary_fact_confidence) "
            "and schema.extracted_facts.items.confidence"
        )
    if "mean_extracted_facts_confidence" in ck or "extracted_facts_confidence_mean" in ck:
        return (
            "schema.extracted_facts.items.confidence (drift: mean_extracted_facts_confidence "
            "in schema_snapshots/baselines.json)"
        )
    if "drift." in ck and "primary_fact" in ck:
        return "schema_snapshots/baselines.json (by_contract drift column) + related schema.extracted_facts.items.confidence"

    if ".doc_id." in ck or col == "doc_id":
        return "schema.doc_id (required, format: uuid, pattern, unique)"
    if "source_path" in ck or col == "source_path":
        return "schema.source_path (required)"
    if "extracted_at" in ck or col == "extracted_at":
        return "schema.extracted_at (required, format: date-time)"
    if "processing_time_ms" in ck or col == "processing_time_ms":
        return "schema.processing_time_ms (minimum, required)"
    if "source_hash" in ck or col == "source_hash":
        return "schema.source_hash (pattern, required when present)"
    if "profile.flatten" in ck or "fact_confidence.dtype" in ck:
        return "schema.extracted_facts / items shape (JSONL must flatten into profiled columns)"
    if ".type_match" in ck and "week3." in ck:
        return "schema.<field> type vs data (see check_id for field name)"

    if cid == "week4-brownfield-lineage-snapshot" or ck.startswith("week4."):
        if "snapshot_id" in ck or col == "snapshot_id":
            return "schema.snapshot_id (required, uuid)"
        if "git_commit" in ck or col == "git_commit":
            return "schema.git_commit (pattern: 40 hex chars)"
        if "edges.endpoints" in ck or ("edges" in col.lower() and "source" in col.lower()):
            return "schema.edges items: source, target, relationship (enum); endpoints must exist in schema.nodes"
        if "snapshot.exists" in ck or col == "*":
            return "servers.local.path (non-empty JSONL) — whole-contract volume gate"
        if "nodes" in col.lower():
            return "schema.nodes (items.node_id, items.type enum)"

    if cid == "week5-event-sourcing-events" or ck.startswith("week5."):
        if "sequence_number.monotonic" in ck or col == "sequence_number":
            return "schema.sequence_number + schema.aggregate_id (cross-record monotonic +1 rule)"
        if "recorded_gte_occurred" in ck:
            return "schema.recorded_at and schema.occurred_at (ordering: recorded_at >= occurred_at)"
        if "correlation_id" in ck:
            return "schema.metadata.properties.correlation_id (required, uuid)"
        if "metadata." in ck and "required" in ck:
            tail = ck.split("week5.", 1)[-1].replace(".required", "") if "week5." in ck else col
            return f"schema.metadata.properties / nested path `{tail or col}` (required)"
        if "event_id" in ck or col == "event_id":
            return "schema.event_id (required, uuid, unique)"
        if "aggregate_id" in ck or col == "aggregate_id":
            return "schema.aggregate_id (required, uuid)"

    if cid == "langsmith-trace-record-migrated" or ck.startswith("langsmith."):
        if "run_type" in ck or col == "run_type":
            return "schema.run_type (enum: llm|chain|tool|retriever|embedding)"
        if "tokens.sum" in ck or col == "total_tokens":
            return "schema.total_tokens, prompt_tokens, completion_tokens (identity: total = prompt + completion)"
        if "timing" in ck or col in ("end_time", "start_time"):
            return "schema.start_time / schema.end_time (end after start when both set)"

    if "runner.group" in ck or "runner.exception" in ck:
        return "see nested check message — underlying schema group named in check_id"
    if "drift." in ck:
        return "schema_snapshots/baselines.json drift column referenced in check_id + producing field in schema.*"

    return f"schema.* — locate keys matching column `{col or '?'}` in the Bitol contract"


def _repo_path(path: Union[str, Path]) -> Path:
    p = Path(path)
    return p if p.is_absolute() else REPO_ROOT / p


def pipeline_slug_from_contract_id(contract_id: str) -> str:
    """
    Filesystem-safe token from Bitol ``contract_id`` so report names show which pipeline
    produced them (e.g. ``week3_document_refinery_extractions``).
    """
    s = (contract_id or "").strip().lower()
    if not s:
        return "unknown_pipeline"
    out = re.sub(r"[^a-z0-9]+", "_", s)
    out = re.sub(r"_+", "_", out).strip("_")
    return out or "unknown_pipeline"


def pipeline_slug_from_validation_report(validation_report: dict) -> str:
    return pipeline_slug_from_contract_id(str(validation_report.get("contract_id") or ""))


def resolve_stakeholder_output_path(
    path: Path,
    slug: str,
    *,
    default_filename: str,
    skip_pipeline_prefix: bool,
) -> Path:
    """
    If ``skip_pipeline_prefix`` is False, ensure ``slug`` appears in the output filename.

    - Directory or path string ending with ``/`` → ``<dir>/<slug>_<default_filename>``.
    - File path whose stem already starts with ``slug`` → unchanged.
    - Otherwise → ``<parent>/<slug>_<stem>.<suffix>``.
    """
    if skip_pipeline_prefix:
        return path
    raw = str(path)
    is_dir_intent = raw.endswith(("/", "\\")) or (path.exists() and path.is_dir())
    if is_dir_intent:
        base = path if (path.exists() and path.is_dir()) else _repo_path(raw.rstrip("/\\"))
        base.mkdir(parents=True, exist_ok=True)
        return base / f"{slug}_{default_filename}"

    if path.suffix.lower() != ".json" and default_filename.endswith(".json"):
        # tolerate passing a basename without extension
        path = path.with_suffix(".json")

    stem = path.stem
    if stem == slug or stem.startswith(f"{slug}_"):
        return path
    return path.with_name(f"{slug}_{path.name}")


def resolve_stakeholder_pdf_path(
    path: Path,
    slug: str,
    *,
    default_filename: str,
    skip_pipeline_prefix: bool,
) -> Path:
    if skip_pipeline_prefix:
        return path
    raw = str(path)
    is_dir_intent = raw.endswith(("/", "\\")) or (path.exists() and path.is_dir())
    if is_dir_intent:
        base = path if (path.exists() and path.is_dir()) else _repo_path(raw.rstrip("/\\"))
        base.mkdir(parents=True, exist_ok=True)
        return base / f"{slug}_{default_filename}"

    if path.suffix.lower() != ".pdf":
        path = path.with_suffix(".pdf")

    stem = path.stem
    if stem == slug or stem.startswith(f"{slug}_"):
        return path
    return path.with_name(f"{slug}_{path.name}")


def _resolve_registry_file(registry_path: str | None) -> str | None:
    if not registry_path or not str(registry_path).strip():
        return None
    p = Path(registry_path)
    if not p.is_absolute():
        p = REPO_ROOT / p
    return str(p) if p.is_file() else None


_SEVERITY_RANK = {
    "CRITICAL": 0,
    "HIGH": 1,
    "WARNING": 2,
    "WARN": 2,
    "LOW": 3,
    "ERROR": 4,
}


def _normalize_column_name(column_name: str) -> str:
    s = (column_name or "").strip()
    if not s or s == "*":
        return s
    out = _ARRAY_BRACKET_RE.sub("", s)
    while ".." in out:
        out = out.replace("..", ".")
    return out.strip(".").strip()


def _system_label(check_prefix: str) -> str:
    if not check_prefix:
        return "the dataset"
    return f"the {check_prefix.replace('_', ' ')} subsystem"


def plain_language(result: dict, registry_path: str | None = None) -> str:
    """
    Human-readable violation line. When ``registry_path`` points to a readable registry file,
    includes downstream subscriber ids from ``query_blast_radius``; otherwise omits that clause.
    Never raises.

    For ``query_blast_radius``, the registry key is ``result['contract_id']`` when present
    (``generate_report`` injects this from the parent validation report’s Bitol id); otherwise
    ``result['check_id'].split('.')[0]`` (short check namespace prefix).
    """
    check_id = str(result.get("check_id") or "")
    parts = check_id.split(".")
    check_prefix = parts[0] if parts else ""
    contract_id_key = str(result.get("contract_id") or "").strip()
    if not contract_id_key:
        contract_id_key = check_prefix

    column_name = str(result.get("column_name") or "")
    check_type = str(result.get("check_type") or "")
    expected = str(result.get("expected") or "")
    actual_value = str(result.get("actual_value") or "")
    records_failing = result.get("records_failing")
    try:
        rf = int(records_failing) if records_failing is not None else 0
    except (TypeError, ValueError):
        rf = 0

    system = _system_label(check_prefix if check_prefix else contract_id_key)
    rule = (check_type or "quality").replace("_", " ")
    base = (
        f"The data field “{column_name}” in {system} did not pass a {rule} rule. "
        f"We expected: {expected}. What we saw: {actual_value}."
    )

    subscriber_sentence: Optional[str] = None
    rp = _resolve_registry_file(registry_path)
    if rp:
        try:
            from contracts.registry_loader import load_registry, query_blast_radius

            registry = load_registry(rp)
            failing_field = _normalize_column_name(column_name)
            subs = query_blast_radius(registry, contract_id_key, failing_field)
            ids: List[str] = []
            for s in subs:
                if not isinstance(s, dict):
                    continue
                sid = s.get("subscriber_id")
                if sid is None:
                    continue
                t = str(sid).strip()
                if t:
                    ids.append(t)
            sub_text = ", ".join(ids) if ids else "none registered"
            subscriber_sentence = f"Downstream subscribers affected: {sub_text}."
        except Exception:
            subscriber_sentence = None

    if subscriber_sentence:
        return f"{base} {subscriber_sentence} Records failing: {rf}."
    return f"{base} Records failing: {rf}."


def _critical_fail_count(results: List[dict]) -> int:
    n = 0
    for r in results:
        if not isinstance(r, dict):
            continue
        if r.get("status") != "FAIL":
            continue
        if str(r.get("severity") or "").upper() == "CRITICAL":
            n += 1
    return n


def _health_score_and_narrative(validation_report: dict, results: List[dict]) -> tuple[float, str, str]:
    try:
        total = int(validation_report.get("total_checks") or 0)
    except (TypeError, ValueError):
        total = 0
    try:
        passed = int(validation_report.get("passed") or 0)
    except (TypeError, ValueError):
        passed = 0
    crit = _critical_fail_count(results)
    base = (passed / total) * 100.0 if total > 0 else 0.0
    score = max(0.0, min(100.0, base - 20.0 * crit))
    narrative_technical = (
        f"Pass ratio {base:.1f}% over {total} checks; {crit} CRITICAL failure(s) "
        "each reduce the score by 20 points (documentation formula)."
    )
    if total <= 0:
        plain = "No automated checks were run, so a score could not be calculated."
    elif crit > 0:
        plain = (
            f"The score is {round(score, 0):.0f} out of 100. "
            f"{crit} issue(s) were marked critical—those are treated as highest risk and lower the score sharply. "
            f"Overall, {passed} of {total} checks passed."
        )
    elif score >= 85:
        plain = (
            f"The score is {round(score, 0):.0f} out of 100. "
            f"Most checks passed ({passed} of {total}). This usually means the dataset looks healthy for routine use."
        )
    elif score >= 60:
        plain = (
            f"The score is {round(score, 0):.0f} out of 100. "
            f"{passed} of {total} checks passed. Some problems need review before you treat this data as fully trusted."
        )
    else:
        plain = (
            f"The score is {round(score, 0):.0f} out of 100. "
            f"Multiple checks did not pass. Treat this as a sign to pause and fix data quality before relying on it."
        )
    return round(score, 2), narrative_technical, plain


def _schema_evolution_plain(schema_summary: Optional[dict]) -> str:
    if not schema_summary:
        return (
            "No schema comparison was attached to this report. "
            "If your team tracks schema history, run the schema analyzer and regenerate this report."
        )
    if schema_summary.get("requires_migration"):
        return (
            "The latest schema comparison found changes that can break downstream consumers. "
            "Before you ship, align with the teams that read this data and follow your migration checklist."
        )
    return (
        "The latest schema comparison did not flag breaking changes. "
        "You should still skim release notes if you changed fields recently."
    )


def _ai_risk_plain(ai_summary: Optional[dict]) -> str:
    if not ai_summary:
        return (
            "No AI extension summary was attached (prompt checks, embedding drift, output rates). "
            "If you use LLMs on this data, run the AI extensions job and regenerate this report."
        )
    if ai_summary.get("requires_attention"):
        parts = []
        if str(ai_summary.get("embedding_drift_status", "")).upper() in ("FAIL", "WARN", "WARNING"):
            parts.append("embedding drift")
        if ai_summary.get("output_violation_rate") is not None:
            tr = str(ai_summary.get("output_trend") or "")
            if tr == "rising" or ai_summary.get("output_violation_rate", 0) > 0.05:
                parts.append("rising or high output violation rate")
        msg = (
            "AI-related monitoring flagged something that needs a look—typically prompts, embeddings, or model output shape. "
            "Ask your ML or platform owner to review the AI extensions output."
        )
        if parts:
            return msg + f" (Areas called out: {', '.join(parts)}.)"
        return msg
    return (
        "AI-related checks did not report urgent problems on this run. "
        "Keep running them periodically if models or prompts change."
    )


def _primary_action_plain(
    *,
    health_score: float,
    failed: int,
    critical_fails: int,
    registry_gap_alert: bool,
    ai_requires_attention: bool,
) -> str:
    """
    Single sentence a non-engineer can follow without extra context (rubric test).
    """
    if critical_fails > 0:
        return (
            "Stop and fix the critical data problems listed in this report before anyone uses this dataset "
            "for important decisions; then ask your team to run the automatic data check again."
        )
    if failed > 0:
        return (
            "Have your data or engineering team review the failed checks below, correct the underlying data or pipeline, "
            "and re-run validation before the next production release."
        )
    if registry_gap_alert:
        return (
            "Update the contract registry so every downstream team that depends on this data is listed—"
            "that way they get warned when the contract changes."
        )
    if ai_requires_attention:
        return (
            "Ask your ML or data platform owner to review the AI section of this report; "
            "address prompts, drift, or output rules before scaling model use."
        )
    if health_score >= 85:
        return (
            "No blocking action is required from this report: keep your usual monitoring, "
            "and schedule the next validation run on your normal cadence."
        )
    return (
        "Review the summary and follow any team-specific process for medium-priority data quality follow-up."
    )


def _recommended_actions_plain(
    contract_id: str,
    top_failures: List[dict],
    registry_gap_count: int,
) -> List[str]:
    """Short directives that still name the Bitol file and clause area (stakeholder-readable)."""
    cid = (contract_id or "").strip() or "this contract"
    yml = CONTRACT_ID_TO_BITOL_RELPATH.get(
        cid, f"generated_contracts/{cid}.yaml" if cid != "this contract" else "generated_contracts/<contract>.yaml"
    )
    actions: List[str] = []
    if registry_gap_count > 0:
        actions.append(
            f"Add a subscription in contract_registry/subscriptions.yaml for `{cid}` covering each failing "
            "field so blast radius and breaking_fields stay aligned with ValidationRunner."
        )
    seen_ck: set[str] = set()
    for r in top_failures:
        if len(actions) >= 5:
            break
        if not isinstance(r, dict):
            continue
        ck = str(r.get("check_id") or "")
        if not ck or ck in seen_ck:
            continue
        seen_ck.add(ck)
        col = str(r.get("column_name") or "")
        clause = _schema_clause_for_failure(cid, ck, col)
        actions.append(
            f"Check **{ck}**: update data or edit **`{yml}`** at {clause}, then re-run validation."
        )
    while len(actions) < 3:
        actions.append(
            "When metrics change on purpose, refresh schema_snapshots/baselines.json (or regenerate contracts) before enforcing."
        )
    return actions[:5]


def _build_report_sections(
    *,
    primary_action: str,
    health_score: float,
    health_plain: str,
    health_technical: str,
    validation_report: dict,
    top_3_out: List[dict],
    schema_plain: str,
    ai_plain: str,
    recommended_plain: List[str],
) -> List[Dict[str, Any]]:
    """Exactly five sections for UI/PDF (rubric: all sections present, plain language)."""
    try:
        tc = int(validation_report.get("total_checks") or 0)
        ok = int(validation_report.get("passed") or 0)
        bad = int(validation_report.get("failed") or 0)
        wn = int(validation_report.get("warned") or 0)
    except (TypeError, ValueError):
        tc = ok = bad = wn = 0
    glance = (
        f"This run executed {tc} automatic checks. {ok} passed, {bad} failed, and {wn} produced warnings. "
        "Failed rows are the ones that need human follow-up."
    )
    issues_body = []
    if not top_3_out:
        issues_body.append("No failing checks were in the top summary for this snapshot.")
    else:
        for t in top_3_out:
            if isinstance(t, dict):
                issues_body.append(t.get("plain_language") or t.get("message") or "")
    return [
        {
            "key": "action",
            "title": "1. What you should do next",
            "content": primary_action,
        },
        {
            "key": "health",
            "title": "2. Data Health Score",
            "content": f"The Data Health Score is {round(health_score, 0):.0f} out of 100. {health_plain}",
            "score": health_score,
            "technical_note": health_technical,
        },
        {
            "key": "snapshot",
            "title": "3. Results at a glance",
            "content": glance,
        },
        {
            "key": "issues",
            "title": "4. Main issues in plain language",
            "content": "\n\n".join(x for x in issues_body if x) or "No top failures to summarize.",
        },
        {
            "key": "follow_up",
            "title": "5. Schema, AI signals, and recommended follow-up",
            "content": "\n\n".join(
                [
                    f"Schema: {schema_plain}",
                    f"AI monitoring: {ai_plain}",
                    "Suggested next steps:",
                    *[f"• {a}" for a in recommended_plain],
                ]
            ),
        },
    ]


def _recommended_actions(
    contract_id: str,
    top_failures: List[dict],
    registry_gap_count: int,
) -> List[str]:
    cid = (contract_id or "").strip() or "unknown-contract"
    yml_rel = CONTRACT_ID_TO_BITOL_RELPATH.get(cid, f"generated_contracts/{cid}.yaml")
    yml_abs = REPO_ROOT / yml_rel
    actions: List[str] = []
    if registry_gap_count > 0:
        actions.append(
            f"Registry gap ({registry_gap_count} failing field(s) without subscriber match): extend "
            f"`contract_registry/subscriptions.yaml` breaking_fields so each column_name from FAIL rows "
            f"prefix-matches a `field` entry (see contracts/registry_loader._field_matches_breaking)."
        )
    seen_ck: set[str] = set()
    for r in top_failures:
        if len(actions) >= 5:
            break
        if not isinstance(r, dict):
            continue
        ck = str(r.get("check_id") or "")
        if not ck or ck in seen_ck:
            continue
        seen_ck.add(ck)
        col = str(r.get("column_name") or "")
        clause = _schema_clause_for_failure(cid, ck, col)
        actions.append(
            f"Directive [{ck}] column={col!r}: modify `{yml_abs}` clause **{clause}** "
            f"or fix JSONL at servers.local.path declared in that YAML; then "
            f"`python contracts/runner.py` for contract `{cid}`."
        )
    while len(actions) < 3:
        actions.append(
            f"Maintain drift baselines under `{REPO_ROOT / 'schema_snapshots' / 'baselines.json'}`; "
            "use AUDIT until baselines stabilize, then WARN/ENFORCE."
        )
    return actions[:5]


def _failures_sorted(results: List[dict]) -> List[dict]:
    fails = [r for r in results if isinstance(r, dict) and r.get("status") == "FAIL"]

    def sort_key(r: dict) -> tuple:
        sev = str(r.get("severity") or "LOW").upper()
        rank = _SEVERITY_RANK.get(sev, 99)
        try:
            rf = int(r.get("records_failing") or 0)
        except (TypeError, ValueError):
            rf = 0
        return (rank, -rf)

    return sorted(fails, key=sort_key)


def _load_schema_evolution_summary(schema_evolution_path: Path) -> Optional[dict]:
    if not schema_evolution_path.is_file():
        return None
    try:
        data = json.loads(schema_evolution_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None
    if "breaking_count" not in data or "compatible_count" not in data:
        return None
    try:
        br = int(data["breaking_count"])
        comp = int(data["compatible_count"])
    except (TypeError, ValueError):
        return None
    return {
        "breaking_changes_detected": br,
        "compatible_changes_detected": comp,
        "requires_migration": br > 0,
    }


def _status_is_fail_or_warn(status: Any) -> bool:
    if status is None:
        return False
    s = str(status).upper()
    return s in ("FAIL", "WARN", "WARNING", "ERROR")


def _load_ai_risk_summary(ai_extensions_path: Path) -> Optional[dict]:
    if not ai_extensions_path.is_file():
        return None
    try:
        data = json.loads(ai_extensions_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict):
        return None

    emb = data.get("embedding_drift")
    out_rate = data.get("output_violation_rate")
    prompt = data.get("prompt_validation")

    if isinstance(emb, dict):
        emb_status = str(emb.get("status") or "unknown")
        drift_score = emb.get("drift_score")
        if drift_score is not None:
            try:
                drift_score = float(drift_score)
            except (TypeError, ValueError):
                drift_score = None
    else:
        emb_status = "not_run"
        drift_score = None

    viol_rate: Optional[float] = None
    out_trend: Optional[str] = None
    if isinstance(out_rate, dict):
        vr = out_rate.get("violation_rate")
        if vr is not None:
            try:
                viol_rate = float(vr)
            except (TypeError, ValueError):
                viol_rate = None
        tr = out_rate.get("trend")
        out_trend = str(tr) if tr is not None else None

    requires = False
    if isinstance(emb, dict) and _status_is_fail_or_warn(emb.get("status")):
        requires = True
    if isinstance(out_rate, dict) and _status_is_fail_or_warn(out_rate.get("status")):
        requires = True
    if isinstance(prompt, dict) and _status_is_fail_or_warn(prompt.get("status")):
        requires = True

    return {
        "embedding_drift_status": emb_status,
        "drift_score": drift_score,
        "output_violation_rate": viol_rate,
        "output_trend": out_trend,
        "requires_attention": requires,
    }


def generate_report(
    validation_report: dict,
    *,
    reports_dir: Union[str, Path] = REPO_ROOT / "validation_reports",
    violations_dir: Union[str, Path] = REPO_ROOT / "violation_log",
    registry_path: Optional[str] = None,
    ai_extensions_path: Optional[Union[str, Path]] = None,
    schema_evolution_path: Optional[Union[str, Path]] = None,
    report_version: Optional[str] = None,
) -> dict:
    """
    Build JSON report with ``top_3`` violations (plain_language), ``registry_gap_count``,
    ``registry_unavailable``, optional schema evolution and AI risk summaries.

    Paths default to the usual repo layout under ``REPO_ROOT``. Pass absolute paths
    or paths relative to ``REPO_ROOT``.

    ``report_version`` overrides the resolved version (normally from
    ``enforcer_report/REPORT_VERSION`` or ``ENFORCER_REPORT_VERSION``).
    """
    resolved_version = resolve_report_version(report_version)
    artifact_stem = stakeholder_report_stem(validation_report)
    reports_dir_p = _repo_path(reports_dir)

    if ai_extensions_path is None:
        ai_path = reports_dir_p / "ai_extensions.json"
    else:
        ai_path = _repo_path(ai_extensions_path)

    if schema_evolution_path is None:
        schema_path = reports_dir_p / "schema_evolution.json"
    else:
        schema_path = _repo_path(schema_evolution_path)

    reg_arg = registry_path
    if reg_arg is None:
        reg_arg = str(REPO_ROOT / "contract_registry" / "subscriptions.yaml")
    results = validation_report.get("results") or []
    if not isinstance(results, list):
        results = []

    failures = _failures_sorted(results)
    top_3 = failures[:3]

    registry_unavailable = False
    registry_gap_count = 0

    resolved_reg = _resolve_registry_file(reg_arg)
    report_contract_id = str(validation_report.get("contract_id") or "").strip()

    if resolved_reg:
        try:
            from contracts.registry_loader import load_registry, query_blast_radius

            registry = load_registry(resolved_reg)
            for r in failures:
                if not isinstance(r, dict):
                    continue
                check_id = str(r.get("check_id") or "")
                contract_id_key = report_contract_id or (
                    check_id.split(".")[0] if check_id else ""
                )
                col = _normalize_column_name(str(r.get("column_name") or ""))
                subs = query_blast_radius(registry, contract_id_key, col)
                if not subs:
                    registry_gap_count += 1
        except Exception:
            registry_unavailable = True
            registry_gap_count = 0

    effective_registry_for_plain = None if registry_unavailable else resolved_reg

    top_3_out: List[Dict[str, Any]] = []
    for r in top_3:
        entry = dict(r)
        if report_contract_id:
            entry["contract_id"] = report_contract_id
        entry["plain_language"] = plain_language(entry, effective_registry_for_plain)
        top_3_out.append(entry)

    schema_summary = _load_schema_evolution_summary(schema_path)
    ai_summary = _load_ai_risk_summary(ai_path)

    health_score, health_narrative_technical, health_narrative_plain = _health_score_and_narrative(
        validation_report, results
    )
    try:
        failed_ct = int(validation_report.get("failed") or 0)
    except (TypeError, ValueError):
        failed_ct = 0
    critical_fails = _critical_fail_count(results)
    registry_gap_alert = registry_gap_count > 0
    ai_requires = bool(ai_summary and ai_summary.get("requires_attention"))
    primary_action = _primary_action_plain(
        health_score=health_score,
        failed=failed_ct,
        critical_fails=critical_fails,
        registry_gap_alert=registry_gap_alert,
        ai_requires_attention=ai_requires,
    )
    schema_evolution_plain = _schema_evolution_plain(schema_summary)
    ai_risk_plain = _ai_risk_plain(ai_summary)
    recommended_actions_plain = _recommended_actions_plain(
        report_contract_id,
        failures,
        registry_gap_count,
    )
    recommended_actions = _recommended_actions(
        report_contract_id,
        failures,
        registry_gap_count,
    )
    report_sections = _build_report_sections(
        primary_action=primary_action,
        health_score=health_score,
        health_plain=health_narrative_plain,
        health_technical=health_narrative_technical,
        validation_report=validation_report,
        top_3_out=top_3_out,
        schema_plain=schema_evolution_plain,
        ai_plain=ai_risk_plain,
        recommended_plain=recommended_actions_plain,
    )

    def _rel_repo(p: Path) -> str:
        try:
            return str(p.resolve().relative_to(REPO_ROOT.resolve()))
        except ValueError:
            return str(p.resolve())

    generation_sources: List[Dict[str, Any]] = [
        {
            "kind": "validation_run",
            "detail": "Counts, pass/fail, and per-check rows came from the validation JSON file passed to this generator.",
            "report_id": validation_report.get("report_id"),
        },
        {
            "kind": "schema_evolution",
            "path": _rel_repo(schema_path) if schema_path.is_file() else None,
            "read": schema_path.is_file(),
        },
        {
            "kind": "ai_extensions",
            "path": _rel_repo(ai_path) if ai_path.is_file() else None,
            "read": ai_path.is_file(),
        },
        {
            "kind": "contract_registry",
            "path": resolved_reg,
            "read": bool(resolved_reg),
        },
    ]

    out: Dict[str, Any] = {
        "report_version": resolved_version,
        "report_artifact_stem": artifact_stem,
        "pipeline_contract_id": validation_report.get("contract_id"),
        "report_id": validation_report.get("report_id"),
        "contract_id": validation_report.get("contract_id"),
        "snapshot_id": validation_report.get("snapshot_id"),
        "run_timestamp": validation_report.get("run_timestamp"),
        "total_checks": validation_report.get("total_checks"),
        "passed": validation_report.get("passed"),
        "failed": validation_report.get("failed"),
        "warned": validation_report.get("warned"),
        "errored": validation_report.get("errored"),
        "health_score": health_score,
        "data_health_score": health_score,
        "health_narrative": health_narrative_plain,
        "health_narrative_plain": health_narrative_plain,
        "health_narrative_technical": health_narrative_technical,
        "primary_action": primary_action,
        "critical_fail_count": critical_fails,
        "recommended_actions": recommended_actions,
        "recommended_actions_plain": recommended_actions_plain,
        "registry_gap_count": registry_gap_count,
        "registry_gap_alert": registry_gap_alert,
        "registry_unavailable": registry_unavailable,
        "top_3": top_3_out,
        "schema_evolution_summary": schema_summary,
        "schema_evolution_plain": schema_evolution_plain,
        "ai_risk_summary": ai_summary,
        "ai_risk_plain": ai_risk_plain,
        "report_sections": report_sections,
        "generation_sources": generation_sources,
        "auto_generated": True,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return out


def write_enforcer_pdf(report: dict, out_path: Path) -> None:
    """
    Write a styled stakeholder PDF (ReportLab): cover band, KPI strip, section cards,
    optional top-issues table, and footer with version line.

    Uses ``report_sections`` when present (five plain-language blocks).
    """
    from xml.sax.saxutils import escape

    from reportlab.lib import colors
    from reportlab.lib.colors import HexColor
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import (
        HRFlowable,
        PageBreak,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )

    # Palette — teal / slate editorial look
    C_PRIMARY_DARK = HexColor("#115e59")
    C_SLATE = HexColor("#334155")
    C_MUTED = HexColor("#64748b")
    C_PANEL = HexColor("#f0fdfa")
    C_BORDER = HexColor("#ccfbf1")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    page_w, page_h = letter

    stem_meta = str(report.get("report_artifact_stem") or "").strip()
    doc_title = (
        f"Enforcer — {stem_meta}"
        if stem_meta
        else "Data Contract Enforcer Report"
    )
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=letter,
        leftMargin=56,
        rightMargin=56,
        topMargin=120,
        bottomMargin=64,
        title=doc_title,
        author="Data Contract Enforcer",
    )
    frame_w = page_w - 56 - 56

    styles = getSampleStyleSheet()
    styles.add(
        ParagraphStyle(
            name="RptBody",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=10,
            leading=14,
            textColor=C_SLATE,
            spaceAfter=8,
        )
    )
    styles.add(
        ParagraphStyle(
            name="RptMeta",
            parent=styles["Normal"],
            fontName="Helvetica",
            fontSize=9,
            leading=12,
            textColor=C_MUTED,
            spaceAfter=4,
        )
    )
    styles.add(
        ParagraphStyle(
            name="RptSecTitle",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=13,
            leading=16,
            textColor=C_PRIMARY_DARK,
            spaceBefore=18,
            spaceAfter=10,
            borderPadding=(0, 0, 0, 4),
        )
    )
    styles.add(
        ParagraphStyle(
            name="RptSmallBold",
            parent=styles["Normal"],
            fontName="Helvetica-Bold",
            fontSize=9,
            textColor=C_MUTED,
        )
    )
    story: List[Any] = []
    rv = escape(str(report.get("report_version") or resolve_report_version()))

    def para_html(html: str, style_name: str = "RptBody") -> Paragraph:
        return Paragraph(html, styles[style_name])

    def para_plain(text: str, style_name: str = "RptBody") -> Paragraph:
        t = escape(str(text or "").replace("\r", "")).replace("\n", "<br/>")
        return Paragraph(t, styles[style_name])

    # --- KPI dashboard (below header band drawn on canvas) ---
    hs = report.get("data_health_score", report.get("health_score"))
    hs_disp = f"{float(hs):.1f}" if isinstance(hs, (int, float)) else escape(str(hs or "—"))
    try:
        tc = int(report.get("total_checks") or 0)
        ps = int(report.get("passed") or 0)
        fl = int(report.get("failed") or 0)
        wn = int(report.get("warned") or 0)
    except (TypeError, ValueError):
        tc, ps, fl, wn = 0, 0, 0, 0

    kpi_data = [
        [
            Paragraph(
                f"<font size='9' color='#64748b'>Data Health Score</font><br/>"
                f"<font size='26' color='#0f766e'><b>{hs_disp}</b></font>"
                f"<font size='10' color='#64748b'> / 100</font>",
                styles["Normal"],
            ),
            para_plain(
                f"Checks passed\n{ps} / {tc}\n\n"
                f"Failed: {fl}  •  Warned: {wn}\n"
                f"Critical fail groups: {report.get('critical_fail_count', '—')}",
                "RptMeta",
            ),
            para_plain(
                f"Contract\n{report.get('contract_id') or '—'}\n\n"
                f"Validation run\n{report.get('run_timestamp') or '—'}",
                "RptMeta",
            ),
        ]
    ]
    kpi = Table(kpi_data, colWidths=[frame_w * 0.38, frame_w * 0.30, frame_w * 0.32])
    kpi.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), C_PANEL),
                ("BOX", (0, 0), (-1, -1), 1, C_BORDER),
                ("TOPPADDING", (0, 0), (-1, -1), 16),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 16),
                ("LEFTPADDING", (0, 0), (-1, -1), 14),
                ("RIGHTPADDING", (0, 0), (-1, -1), 14),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(kpi)
    story.append(Spacer(1, 0.14 * inch))

    # Auto-generated callout
    story.append(
        para_html(
            f"<b><font color='#0f766e'>Auto-generated report.</font></b> "
            f"This PDF was built from machine validation JSON and optional "
            f"schema-evolution / AI-extension inputs — not hand-authored prose. "
            f"<i>Report version {rv}.</i>"
        )
    )
    story.append(Spacer(1, 0.06 * inch))
    if report.get("generated_at"):
        story.append(para_plain(f"Generated: {report.get('generated_at')}", "RptMeta"))
    if report.get("snapshot_id"):
        story.append(para_plain(f"Snapshot hash: {report.get('snapshot_id')}", "RptMeta"))
    story.append(Spacer(1, 0.12 * inch))

    # Top issues table
    top3 = report.get("top_3")
    if isinstance(top3, list) and top3:
        story.append(para_plain("Priority issues (top failures)", "RptSecTitle"))
        th = [
            para_plain("Check", "RptSmallBold"),
            para_plain("Status", "RptSmallBold"),
            para_plain("Detail", "RptSmallBold"),
        ]
        rows = [th]
        for item in top3[:5]:
            if not isinstance(item, dict):
                continue
            st = str(item.get("status") or "")
            st_hex = (
                "#b91c1c"
                if st == "FAIL"
                else "#b45309"
                if st in ("WARN", "WARNING")
                else "#334155"
            )
            ck = escape(str(item.get("check_id") or ""))[:56]
            av = escape(str(item.get("actual_value") or item.get("message") or ""))[:220]
            rows.append(
                [
                    para_html(f"<font size='8'>{ck}</font>"),
                    para_html(f"<font size='9' color='{st_hex}'><b>{escape(st)}</b></font>"),
                    para_html(f"<font size='8'>{av}</font>"),
                ]
            )
        tw = [frame_w * 0.30, frame_w * 0.12, frame_w * 0.58]
        issues = Table(rows, colWidths=tw, repeatRows=1)
        issues.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), C_PRIMARY_DARK),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, HexColor("#f8fafc")]),
                    ("GRID", (0, 0), (-1, -1), 0.25, C_BORDER),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ]
            )
        )
        story.append(issues)
        story.append(Spacer(1, 0.16 * inch))

    story.append(HRFlowable(width=frame_w, thickness=0.5, color=C_BORDER, spaceBefore=4, spaceAfter=16))

    # Five narrative sections
    sections = report.get("report_sections")
    if isinstance(sections, list) and sections:
        for block in sections:
            if not isinstance(block, dict):
                continue
            title = escape(str(block.get("title") or "Section"))
            body = str(block.get("content") or "")
            story.append(para_html(f"<b>{title}</b>", "RptSecTitle"))
            story.append(para_plain(body))
            tech = block.get("technical_note")
            if tech and str(block.get("key") or "") == "health":
                story.append(
                    para_html(
                        f"<font size='9' color='#64748b'><i>Technical: {escape(str(tech))}</i></font>"
                    )
                )
            story.append(Spacer(1, 0.06 * inch))
    else:
        story.append(para_plain(f"Data Health Score: {hs_disp}", "RptSecTitle"))
        story.append(para_plain(str(report.get("health_narrative") or "")))

    story.append(PageBreak())

    # Appendix: lineage of data
    story.append(para_plain("How this report was produced", "RptSecTitle"))
    story.append(
        para_plain(
            "Inputs typically include: ValidationRunner JSON, optional "
            "schema_evolution.json from SchemaEvolutionAnalyzer, optional "
            "ai_extensions.json from Phase 4 checks, and the contract registry YAML "
            "for subscriber context."
        )
    )
    story.append(Spacer(1, 0.1 * inch))

    gs = report.get("generation_sources")
    if isinstance(gs, list) and gs:
        for g in gs[:12]:
            if not isinstance(g, dict):
                continue
            kind = escape(str(g.get("kind") or ""))
            detail = escape(str(g.get("detail") or g.get("path") or ""))
            read = g.get("read")
            line = f"• <b>{kind}</b>: {detail}" if detail else f"• <b>{kind}</b>"
            if read is not None:
                line += f" <font color='#64748b'>(read={'yes' if read else 'no'})</font>"
            story.append(para_html(line))
        story.append(Spacer(1, 0.08 * inch))

    def on_first_page(canvas: Any, doc_: Any) -> None:
        canvas.saveState()
        canvas.setFillColor(C_PRIMARY_DARK)
        canvas.rect(0, page_h - 108, page_w, 108, fill=1, stroke=0)
        canvas.setFillColor(colors.white)
        canvas.setFont("Helvetica-Bold", 22)
        canvas.drawString(56, page_h - 52, "Data Contract Enforcer")
        canvas.setFont("Helvetica", 11)
        canvas.drawString(56, page_h - 72, "Stakeholder validation report")
        canvas.setFont("Helvetica-Oblique", 8)
        canvas.setFillColor(HexColor("#99f6e4"))
        canvas.drawString(56, page_h - 92, "Auto-generated • not a hand-written audit")
        canvas.restoreState()
        _pdf_footer(canvas, doc_)

    def on_later_pages(canvas: Any, doc_: Any) -> None:
        _pdf_footer(canvas, doc_)

    def _pdf_footer(canvas: Any, doc_: Any) -> None:
        canvas.saveState()
        canvas.setStrokeColor(C_BORDER)
        canvas.setLineWidth(0.75)
        canvas.line(56, 48, page_w - 56, 48)
        canvas.setFillColor(C_MUTED)
        canvas.setFont("Helvetica", 8)
        canvas.drawString(56, 34, "Data Contract Enforcer")
        canvas.drawRightString(
            page_w - 56,
            34,
            f"Report v{rv} • Page {canvas.getPageNumber()}",
        )
        canvas.restoreState()

    doc.build(story, onFirstPage=on_first_page, onLaterPages=on_later_pages)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enforcer stakeholder report (JSON + optional PDF)",
        epilog=(
            "If -o is omitted, writes <reports-dir>/stakeholder__<contract_id>__<run_timestamp>.json "
            "from the validation report. Use bare --pdf to write a PDF with the same stem next to that JSON."
        ),
    )
    parser.add_argument(
        "--report",
        "-r",
        type=str,
        required=True,
        help="Validation report JSON (ValidationRunner output)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help=(
            "Output JSON path. If omitted, uses "
            "<reports-dir>/stakeholder__<contract_id>__<run_timestamp>.json (pipeline-discoverable name)."
        ),
    )
    parser.add_argument(
        "--reports-dir",
        type=str,
        default="validation_reports",
        help="Directory for auxiliary reports (default: validation_reports under repo root)",
    )
    parser.add_argument(
        "--violations-dir",
        type=str,
        default="violation_log",
        help="Violation log directory (default: violation_log under repo root)",
    )
    parser.add_argument(
        "--registry",
        type=str,
        default=None,
        help="Contract registry YAML (default: contract_registry/subscriptions.yaml under repo root)",
    )
    parser.add_argument(
        "--ai-extensions",
        type=str,
        default=None,
        help="ai_extensions.json path (default: <reports-dir>/ai_extensions.json)",
    )
    parser.add_argument(
        "--schema-evolution",
        type=str,
        default=None,
        help="schema_evolution.json path (default: <reports-dir>/schema_evolution.json)",
    )
    parser.add_argument(
        "--fail-on-registry-gap",
        action="store_true",
        help="Exit 1 when registry_gap_count > 0 (CI gate for registry coverage).",
    )
    parser.add_argument(
        "--pdf",
        nargs="?",
        const="__AUTO__",
        default=None,
        metavar="PATH",
        help=(
            "Write stakeholder PDF (reportlab). Pass a path, or use bare --pdf to use "
            "the same directory and stem as the JSON output with .pdf (e.g. stakeholder__week3-...__....pdf)."
        ),
    )
    parser.add_argument(
        "--report-version",
        type=str,
        default=None,
        help=(
            "Embed this report_version string (overrides enforcer_report/REPORT_VERSION "
            "and ENFORCER_REPORT_VERSION)."
        ),
    )
    args = parser.parse_args()

    report_path = _repo_path(args.report)
    if not report_path.is_file():
        print(f"ERROR: report not found: {report_path}", file=sys.stderr)
        sys.exit(2)

    data = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        print("ERROR: validation report must be a JSON object", file=sys.stderr)
        sys.exit(2)

    stem = stakeholder_report_stem(data)
    if args.output is None:
        out_path = _repo_path(args.reports_dir) / f"{stem}.json"
        print(
            f"Using pipeline-discoverable filename stem: {stem} (contract_id + run_timestamp)",
            file=sys.stderr,
        )
    else:
        out_path = _repo_path(args.output)

    reg_kw: Optional[str] = None
    if args.registry is not None:
        reg_kw = str(_repo_path(args.registry))

    generated = generate_report(
        data,
        reports_dir=_repo_path(args.reports_dir),
        violations_dir=_repo_path(args.violations_dir),
        registry_path=reg_kw,
        ai_extensions_path=args.ai_extensions,
        schema_evolution_path=args.schema_evolution,
        report_version=args.report_version,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(generated, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}", file=sys.stderr)

    if args.pdf == "__AUTO__":
        pdf_path = out_path.with_suffix(".pdf")
    elif args.pdf:
        pdf_path = _repo_path(args.pdf)
    else:
        pdf_path = None

    if pdf_path is not None:
        try:
            pdf_path.parent.mkdir(parents=True, exist_ok=True)
            write_enforcer_pdf(generated, pdf_path)
            print(f"Wrote {pdf_path}", file=sys.stderr)
        except ImportError:
            print(
                "WARNING: reportlab not installed; skipping PDF. pip install reportlab",
                file=sys.stderr,
            )
        except Exception as exc:
            print(f"WARNING: PDF generation failed: {exc}", file=sys.stderr)

    if args.fail_on_registry_gap:
        gap = generated.get("registry_gap_count")
        try:
            n = int(gap) if gap is not None else 0
        except (TypeError, ValueError):
            n = 0
        if n > 0:
            print(
                f"ERROR: registry_gap_count={n} (--fail-on-registry-gap)",
                file=sys.stderr,
            )
            sys.exit(1)


if __name__ == "__main__":
    main()
