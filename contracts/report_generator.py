#!/usr/bin/env python3
"""
Stakeholder report: validation summary, top violations with plain-language blurbs,
registry context, five plain-language ``report_sections``, Data Health Score,
and programmatic ``generation_sources`` (report_version 2.1).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

REPORT_VERSION = "2.1"

_ARRAY_BRACKET_RE = re.compile(r"\[\*\]")


def _repo_path(path: Union[str, Path]) -> Path:
    p = Path(path)
    return p if p.is_absolute() else REPO_ROOT / p


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
    """Action bullets without file paths or CLI jargon (stakeholder-safe)."""
    cid = (contract_id or "").strip() or "this contract"
    actions: List[str] = []
    if registry_gap_count > 0:
        actions.append(
            "Add or update subscriber entries in the contract registry so teams that consume this data are notified when it changes."
        )
    for r in top_failures:
        if len(actions) >= 3:
            break
        if not isinstance(r, dict):
            continue
        col = str(r.get("column_name") or "a field")
        actions.append(
            f"Fix data quality for “{col}” under {cid} so it matches the agreed data rules, then run validation again."
        )
    while len(actions) < 3:
        actions.append(
            "When a number’s business meaning changes, refresh your saved comparison snapshots before turning on strict alerts for that field."
        )
    return actions[:3]


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
    actions: List[str] = []
    if registry_gap_count > 0:
        actions.append(
            f"Close registry gaps: add or extend breaking_fields in "
            f"{REPO_ROOT / 'contract_registry' / 'subscriptions.yaml'} "
            f"(registry_gap_count={registry_gap_count})."
        )
    for r in top_failures:
        if len(actions) >= 3:
            break
        if not isinstance(r, dict):
            continue
        ck = str(r.get("check_id") or "")
        col = str(r.get("column_name") or "")
        actions.append(
            f"Resolve {ck} (column {col!r}): align data with Bitol contract for {cid!r}, "
            f"then re-run python {REPO_ROOT / 'contracts' / 'runner.py'}."
        )
    while len(actions) < 3:
        actions.append(
            f"Keep numeric baselines under {REPO_ROOT / 'schema_snapshots'}; "
            "use AUDIT mode until baselines exist, then promote to WARN/ENFORCE."
        )
    return actions[:3]


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
) -> dict:
    """
    Build JSON report with ``top_3`` violations (plain_language), ``registry_gap_count``,
    ``registry_unavailable``, optional schema evolution and AI risk summaries.

    Paths default to the usual repo layout under ``REPO_ROOT``. Pass absolute paths
    or paths relative to ``REPO_ROOT``.
    """
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
        "report_version": REPORT_VERSION,
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
    }
    return out


def write_enforcer_pdf(report: dict, out_path: Path) -> None:
    """
    Write a stakeholder PDF alongside ``report_data.json`` (ReportLab).
    Uses ``report_sections`` when present (five plain-language blocks).
    Safe no-op if reportlab is not installed (caller may catch ImportError).
    """
    from xml.sax.saxutils import escape

    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer

    out_path.parent.mkdir(parents=True, exist_ok=True)
    doc = SimpleDocTemplate(
        str(out_path),
        pagesize=letter,
        rightMargin=54,
        leftMargin=54,
        topMargin=54,
        bottomMargin=54,
    )
    styles = getSampleStyleSheet()
    story: List[Any] = []

    def para(text: str, style_name: str = "Normal") -> Paragraph:
        t = escape(str(text or "").replace("\r", ""))
        t = t.replace("\n", "<br/>")
        return Paragraph(t, styles[style_name])

    story.append(para("Data Contract Enforcer Report", "Title"))
    story.append(Spacer(1, 0.12 * inch))

    meta_lines = [
        f"Report version: {report.get('report_version', REPORT_VERSION)}",
        f"Contract: {report.get('contract_id') or '—'}",
        f"Run time: {report.get('run_timestamp') or '—'}",
        f"Snapshot: {report.get('snapshot_id') or '—'}",
    ]
    if report.get("generated_at"):
        meta_lines.append(f"Generated: {report.get('generated_at')}")
    if report.get("project_id"):
        meta_lines.append(f"Project: {report.get('project_id')}")
    if report.get("auto_generated"):
        meta_lines.append("Built automatically from validation and supporting files (not hand-written).")
    story.append(para("\n".join(meta_lines)))
    story.append(Spacer(1, 0.15 * inch))

    sections = report.get("report_sections")
    if isinstance(sections, list) and sections:
        for block in sections:
            if not isinstance(block, dict):
                continue
            title = str(block.get("title") or "Section")
            body = str(block.get("content") or "")
            story.append(para(title, "Heading2"))
            story.append(para(body))
            tech = block.get("technical_note")
            if tech and str(block.get("key") or "") == "health":
                story.append(para(f"Technical detail: {tech}"))
            story.append(Spacer(1, 0.1 * inch))
    else:
        hs = report.get("data_health_score", report.get("health_score", "—"))
        story.append(para(f"Data Health Score: {hs}", "Heading2"))
        story.append(para(str(report.get("health_narrative") or "")))
        story.append(Spacer(1, 0.1 * inch))

    gs = report.get("generation_sources")
    if isinstance(gs, list) and gs:
        story.append(para("Where this report came from", "Heading2"))
        for g in gs[:8]:
            if not isinstance(g, dict):
                continue
            kind = escape(str(g.get("kind") or ""))
            detail = escape(str(g.get("detail") or g.get("path") or ""))
            read = g.get("read")
            line = f"• {kind}: {detail}" if detail else f"• {kind}"
            if read is not None:
                line += f" (read={'yes' if read else 'no'})"
            story.append(para(line))
        story.append(Spacer(1, 0.08 * inch))

    doc.build(story)


def main() -> None:
    parser = argparse.ArgumentParser(description="Enforcer stakeholder report (JSON)")
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
        required=True,
        help="Output path for generated report JSON",
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
        type=str,
        default=None,
        help="Also write stakeholder PDF to this path (requires reportlab).",
    )
    args = parser.parse_args()

    report_path = _repo_path(args.report)
    if not report_path.is_file():
        print(f"ERROR: report not found: {report_path}", file=sys.stderr)
        sys.exit(2)

    out_path = _repo_path(args.output)

    data = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        print("ERROR: validation report must be a JSON object", file=sys.stderr)
        sys.exit(2)

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
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(generated, indent=2), encoding="utf-8")
    print(f"Wrote {out_path}", file=sys.stderr)

    if args.pdf:
        pdf_path = _repo_path(args.pdf)
        try:
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
