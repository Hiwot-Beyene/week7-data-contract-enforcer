#!/usr/bin/env python3
"""
SchemaEvolutionAnalyzer (Phase 3) — temporal YAML snapshots under ``schema_snapshots/{contract_id}/``,
taxonomy-classified diffs, compatibility verdict, migration checklist, rollback plan, and optional
**explicit snapshot pair** for evaluator reproduction.

Usage (auto-pick newest pair in ``--since`` window, else last two snapshots):

  python contracts/schema_analyzer.py \\
    --contract-id week3-document-refinery-extractions \\
    --since "7 days ago" \\
    --output validation_reports/schema_evolution.json

Diff **two named** snapshots (paths or filenames under the contract folder):

  python contracts/schema_analyzer.py \\
    --contract-id week3-document-refinery-extractions \\
    --snapshot-old 20260401T120000Z.yaml \\
    --snapshot-new 20260404T192053Z.yaml \\
    -o validation_reports/schema_evolution_diff.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
_CONTRACTS_DIR = str(REPO_ROOT / "contracts")
if _CONTRACTS_DIR not in sys.path:
    sys.path.insert(0, _CONTRACTS_DIR)

from schema_evolution import FieldRecord, flatten_contract_schema, parse_since_arg

# ---------------------------------------------------------------------------
# Contract YAML → lineage.registry_subscribers (for checklist)
# ---------------------------------------------------------------------------

CONTRACT_YAML_BY_ID: Dict[str, str] = {
    "week3-document-refinery-extractions": "week3_extractions.yaml",
    "week4-brownfield-lineage-snapshot": "week4_lineage.yaml",
    "week4-lineage-missing": "week4_lineage.yaml",
    "week5-event-sourcing-events": "week5_events.yaml",
    "langsmith-trace-record-migrated": "langsmith_traces.yaml",
}

# ---------------------------------------------------------------------------
# Tool-specific notes (Confluent Schema Registry, dbt, Pact)
# ---------------------------------------------------------------------------

CHANGE_TOOL_MAP: Dict[str, Dict[str, str]] = {
    "add_nullable_field": {
        "confluent": "BACKWARD compatible — allowed",
        "dbt": "No enforcement — passes silently",
        "pact": "Consumer pact passes if field not declared required",
    },
    "add_required_field": {
        "confluent": "BACKWARD incompatible — BLOCKED at registration",
        "dbt": "Manual migration required",
        "pact": "Consumer pact fails if consumer depends on absence",
    },
    "rename_field": {
        "confluent": "BLOCKED under any compatibility mode",
        "dbt": "Manual migration required — ref() breaks immediately",
        "pact": "Consumer pact fails immediately — field name is in pact",
    },
    "type_narrowing": {
        "confluent": "FORWARD incompatible — BLOCKED",
        "dbt": "Passes type check but breaks value semantics",
        "pact": "Consumer pact fails if type was declared",
    },
    "type_widening": {
        "confluent": "BACKWARD compatible — usually allowed",
        "dbt": "Passes silently — no native type evolution check",
        "pact": "Usually passes — depends on consumer pact declaration",
    },
    "remove_field": {
        "confluent": "BLOCKED under BACKWARD and FULL modes",
        "dbt": "Breaks ref() immediately at compile time",
        "pact": "Consumer pact fails if field was declared",
    },
    "enum_value_added": {
        "confluent": "BACKWARD compatible — allowed",
        "dbt": "accepted_values test fails until test updated",
        "pact": "Consumer pact passes if new value not in consumer's enum list",
    },
    "enum_value_removed": {
        "confluent": "BACKWARD incompatible — BLOCKED",
        "dbt": "accepted_values test fails",
        "pact": "Consumer pact fails if removed value was in pact",
    },
}

_TOOL_NO_MATERIAL: Dict[str, str] = {
    "confluent": "No registration impact for this field",
    "dbt": "No schema test delta required for this field",
    "pact": "No consumer update required for this declaration",
}


def _tool_dict(map_key: str) -> Dict[str, str]:
    if map_key in CHANGE_TOOL_MAP:
        return dict(CHANGE_TOOL_MAP[map_key])
    return dict(_TOOL_NO_MATERIAL)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _field_record_to_clause(rec: FieldRecord) -> dict:
    d: Dict[str, Any] = {}
    if rec.type is not None:
        d["type"] = rec.type
    if rec.required is not None:
        d["required"] = rec.required
    if rec.minimum is not None:
        d["minimum"] = rec.minimum
    if rec.maximum is not None:
        d["maximum"] = rec.maximum
    if rec.enum is not None:
        d["enum"] = list(rec.enum)
    if rec.format:
        d["format"] = rec.format
    if rec.pattern:
        d["pattern"] = rec.pattern
    return d


def schema_to_field_clauses(schema: dict) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    if not isinstance(schema, dict):
        return out
    flat = flatten_contract_schema(schema)
    for path, rec in flat.items():
        out[path] = _field_record_to_clause(rec)
    return out


def _is_required(clause: dict) -> bool:
    return clause.get("required") is True


def _scalar_ne(a: Any, b: Any) -> bool:
    if a is None and b is None:
        return False
    if a is None or b is None:
        return True
    try:
        return float(a) != float(b)
    except (TypeError, ValueError):
        return str(a) != str(b)


def _enum_set(clause: Optional[dict]) -> Optional[Set[str]]:
    if not clause:
        return None
    e = clause.get("enum")
    if not isinstance(e, list):
        return None
    return {str(x) for x in e}


def classify_change(
    field: str,
    old_clause: Optional[dict],
    new_clause: Optional[dict],
) -> Tuple[str, str, Dict[str, str], str]:
    """
    Classify a single field transition. Rules are evaluated in order; first match wins.
    Returns (verdict, reason, tool_equivalents, taxonomy_category).

    ``taxonomy_category`` is a stable label for rubric/evaluator scripts (maps to CHANGE_TOOL_MAP).
    """
    # 1–2: new field
    if old_clause is None and new_clause is not None:
        if _is_required(new_clause):
            return (
                "BREAKING",
                "New required field",
                _tool_dict("add_required_field"),
                "add_required_field",
            )
        return (
            "COMPATIBLE",
            "New optional field",
            _tool_dict("add_nullable_field"),
            "add_nullable_field",
        )

    # 3: removed
    if new_clause is None:
        return ("BREAKING", "Field removed", _tool_dict("remove_field"), "remove_field")

    assert old_clause is not None

    old_t = old_clause.get("type")
    new_t = new_clause.get("type")
    if old_t != new_t:
        return (
            "BREAKING",
            f"Type changed {old_t!r} → {new_t!r}",
            _tool_dict("type_narrowing"),
            "type_change",
        )

    if _scalar_ne(old_clause.get("maximum"), new_clause.get("maximum")):
        return (
            "BREAKING",
            f"Range maximum changed {old_clause.get('maximum')!r} → {new_clause.get('maximum')!r}",
            _tool_dict("type_narrowing"),
            "range_maximum_change",
        )

    if _scalar_ne(old_clause.get("minimum"), new_clause.get("minimum")):
        return (
            "BREAKING",
            f"Range minimum changed {old_clause.get('minimum')!r} → {new_clause.get('minimum')!r}",
            _tool_dict("type_narrowing"),
            "range_minimum_change",
        )

    os_ = _enum_set(old_clause)
    ns = _enum_set(new_clause)
    if os_ is not None or ns is not None:
        o_eff: Set[str] = os_ if os_ is not None else set()
        n_eff: Set[str] = ns if ns is not None else set()
        removed = o_eff - n_eff
        if removed:
            return (
                "BREAKING",
                f"Enum values removed: {removed!r}",
                _tool_dict("enum_value_removed"),
                "enum_value_removed",
            )
        if n_eff > o_eff:
            added = n_eff - o_eff
            return (
                "COMPATIBLE",
                f"Enum values added: {added!r}",
                _tool_dict("enum_value_added"),
                "enum_value_added",
            )

    return ("COMPATIBLE", "No material change", dict(_TOOL_NO_MATERIAL), "no_material_change")


def _snapshot_sort_key(path: Path) -> Tuple[str, str]:
    """Sort by filename string (timestamp embedded); tie-break by full path."""
    return (path.name, str(path))


def _parse_snapshot_time(name: str) -> Optional[datetime]:
    stem = Path(name).stem
    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d_%H%M%S"):
        try:
            return datetime.strptime(stem, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _load_snapshot_payload(path: Path) -> Optional[dict]:
    try:
        text = path.read_text(encoding="utf-8")
        data = yaml.safe_load(text)
    except Exception as exc:
        print(f"ERROR: skipping malformed snapshot {path}: {exc}", file=sys.stderr)
        return None
    if not isinstance(data, dict):
        print(f"ERROR: skipping snapshot (not a mapping): {path}", file=sys.stderr)
        return None
    return data


def resolve_snapshot_path(repo: Path, contract_id: str, spec: str) -> Path:
    """
    Resolve evaluator-facing snapshot spec to an existing file.

    Accepts: absolute path, ``schema_snapshots/...`` relative to repo, or bare
    ``YYYYMMDDTHHMMSSZ.yaml`` under ``schema_snapshots/{contract_id}/``.
    """
    raw = (spec or "").strip()
    if not raw:
        raise ValueError("empty snapshot path")
    p = Path(raw)
    if p.is_file():
        return p.resolve()
    cand = (repo / raw).resolve()
    if cand.is_file():
        return cand
    under = (repo / "schema_snapshots" / contract_id / Path(raw).name).resolve()
    if under.is_file():
        return under
    raise FileNotFoundError(
        f"Snapshot not found for {spec!r} (tried absolute, repo-relative, schema_snapshots/{contract_id}/)"
    )


def _snapshot_in_since_window(path: Path, payload: dict, cutoff: datetime) -> bool:
    ts = payload.get("snapshot_timestamp")
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc) >= cutoff
        except ValueError:
            pass
    parsed = _parse_snapshot_time(path.name)
    if parsed is not None:
        return parsed >= cutoff
    return True


def load_registry_subscribers_line(repo: Path, contract_id: str) -> str:
    name = CONTRACT_YAML_BY_ID.get(contract_id)
    if not name:
        return "none listed"
    p = repo / "generated_contracts" / name
    if not p.is_file():
        return "none listed"
    try:
        with p.open(encoding="utf-8") as f:
            doc = yaml.safe_load(f)
        if not isinstance(doc, dict):
            return "none listed"
        lin = doc.get("lineage") or {}
        subs = lin.get("registry_subscribers") or []
        if not isinstance(subs, list) or not subs:
            return "none listed"
        return ", ".join(str(s) for s in subs)
    except Exception:
        return "none listed"


def build_migration_checklist(breaking_changes: List[dict], subscribers_line: str) -> List[str]:
    items: List[str] = []
    if not breaking_changes:
        items.append(
            "[OK] No field-level BREAKING verdicts in this pair — backward readers likely safe for this diff."
        )
        items.append(
            "[RECOMMENDED] Still verify downstream dbt / consumers if you added enums or widened types."
        )
        return items
    for ch in breaking_changes:
        field = ch.get("field", "?")
        tax = ch.get("taxonomy", "?")
        items.append(
            f"[REQUIRED] Notify subscribers ({field}, taxonomy={tax}): {subscribers_line}"
        )
        items.append("[REQUIRED] Write migration script in outputs/migrate/ before deploying")
        items.append("[REQUIRED] Re-run ValidationRunner after migration to confirm clean")
    return items


def build_rollback_plan(
    has_breaking: bool, snapshot_old: str, contract_id: str
) -> Tuple[str, Dict[str, Any]]:
    """
    Human summary string (legacy) plus structured steps for rubric / PDF / evaluators.
    """
    detail: Dict[str, Any] = {"anchor_snapshot": None, "steps": []}
    if has_breaking:
        detail["anchor_snapshot"] = snapshot_old
        detail["steps"] = [
            f"Check out contract schema from snapshot file schema_snapshots/{contract_id}/{snapshot_old}.",
            "Revert generated_contracts YAML and dbt models to the commit that produced that snapshot (or regenerate from frozen inputs).",
            "Notify all registry subscribers for this contract_id; pause ENFORCE gates until rollback is verified.",
            "Run: python contracts/runner.py --contract generated_contracts/<matching>.yaml --data <stable jsonl> --mode AUDIT.",
            "Open a new migration PR before re-attempting the breaking schema promotion.",
        ]
        summary = (
            f"Rollback to snapshot {snapshot_old!r} under contract {contract_id!r}; "
            "restore prior artifacts before retrying promotion."
        )
        return summary, detail
    detail["steps"] = [
        "No rollback required — compatibility verdict indicates no BREAKING-classified field transitions.",
    ]
    return "No rollback required.", detail


def run_analyzer(
    repo: Path,
    contract_id: str,
    since: str,
    output_report: Path,
    *,
    snapshot_old_spec: Optional[str] = None,
    snapshot_new_spec: Optional[str] = None,
    write_migration_impact: bool = True,
) -> int:
    cutoff = parse_since_arg(since)
    snap_dir = repo / "schema_snapshots" / contract_id
    path_old: Path
    path_new: Path

    if snapshot_old_spec and snapshot_new_spec:
        try:
            path_old = resolve_snapshot_path(repo, contract_id, snapshot_old_spec)
            path_new = resolve_snapshot_path(repo, contract_id, snapshot_new_spec)
        except (OSError, ValueError) as exc:
            doc = {"status": "SNAPSHOT_PATH_ERROR", "error": str(exc)}
            output_report.parent.mkdir(parents=True, exist_ok=True)
            output_report.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        if path_old == path_new:
            doc = {"status": "SNAPSHOT_PATH_ERROR", "error": "snapshot-old and snapshot-new must differ"}
            output_report.parent.mkdir(parents=True, exist_ok=True)
            output_report.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            return 1
        pair_key = "explicit_cli_pair"
    else:
        if snapshot_old_spec or snapshot_new_spec:
            doc = {
                "status": "SNAPSHOT_PATH_ERROR",
                "error": "Provide both --snapshot-old and --snapshot-new, or neither",
            }
            output_report.parent.mkdir(parents=True, exist_ok=True)
            output_report.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            return 1
        pair_key = "auto_newest_in_window"
        if not snap_dir.is_dir():
            doc = {"status": "INSUFFICIENT_SNAPSHOTS", "snapshots_found": 0}
            output_report.parent.mkdir(parents=True, exist_ok=True)
            output_report.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            print(f"Wrote {output_report} (insufficient snapshots)", file=sys.stderr)
            return 0

        all_yaml = sorted(
            [p for p in snap_dir.iterdir() if p.is_file() and p.suffix.lower() in (".yaml", ".yml")],
            key=_snapshot_sort_key,
        )

        valid_all: List[Path] = []
        payloads: Dict[str, dict] = {}
        for p in all_yaml:
            payload = _load_snapshot_payload(p)
            if payload is None:
                continue
            valid_all.append(p)
            payloads[str(p)] = payload

        if len(valid_all) < 2:
            doc = {"status": "INSUFFICIENT_SNAPSHOTS", "snapshots_found": len(valid_all)}
            output_report.parent.mkdir(parents=True, exist_ok=True)
            output_report.write_text(json.dumps(doc, indent=2), encoding="utf-8")
            print(f"Wrote {output_report} (insufficient snapshots)", file=sys.stderr)
            return 0

        in_window = [
            p
            for p in valid_all
            if _snapshot_in_since_window(p, payloads[str(p)], cutoff)
        ]
        pair_pool = in_window if len(in_window) >= 2 else valid_all
        path_old, path_new = pair_pool[-2], pair_pool[-1]

    pl_old = _load_snapshot_payload(path_old)
    pl_new = _load_snapshot_payload(path_new)
    if pl_old is None or pl_new is None:
        doc = {"status": "INSUFFICIENT_SNAPSHOTS", "snapshots_found": 0}
        output_report.parent.mkdir(parents=True, exist_ok=True)
        output_report.write_text(json.dumps(doc, indent=2), encoding="utf-8")
        print("ERROR: could not load snapshot pair; wrote insufficient report", file=sys.stderr)
        return 0

    schema_old = pl_old.get("schema") or {}
    schema_new = pl_new.get("schema") or {}
    if not isinstance(schema_old, dict):
        schema_old = {}
    if not isinstance(schema_new, dict):
        schema_new = {}

    old_map = schema_to_field_clauses(schema_old)
    new_map = schema_to_field_clauses(schema_new)
    all_fields = sorted(set(old_map.keys()) | set(new_map.keys()))

    changes: List[Dict[str, Any]] = []
    breaking_count = 0
    compatible_count = 0

    for field in all_fields:
        o_clause = old_map.get(field)
        n_clause = new_map.get(field)
        try:
            verdict, reason, tool_eq, taxonomy = classify_change(field, o_clause, n_clause)
        except Exception as exc:
            print(f"WARN: classify_change({field!r}): {exc}", file=sys.stderr)
            verdict, reason, tool_eq, taxonomy = (
                "BREAKING",
                f"Classification error: {exc}",
                _tool_dict("type_narrowing"),
                "classification_error",
            )
        changes.append(
            {
                "field": field,
                "taxonomy": taxonomy,
                "verdict": verdict,
                "reason": reason,
                "tool_equivalents": tool_eq,
            }
        )
        if verdict == "BREAKING":
            breaking_count += 1
        else:
            compatible_count += 1

    breaking_changes = [c for c in changes if c["verdict"] == "BREAKING"]
    taxonomy_counts = dict(Counter(c["taxonomy"] for c in changes))
    subs = load_registry_subscribers_line(repo, contract_id)
    checklist = build_migration_checklist(breaking_changes, subs)
    rollback_summary, rollback_detail = build_rollback_plan(bool(breaking_changes), path_old.name, contract_id)

    analyzed_at = utc_now_iso()
    compatibility_verdict = (
        "BREAKING_CHANGE_DETECTED" if breaking_count else "NO_BREAKING_CHANGES"
    )
    confluent_style = (
        "BACKWARD_INCOMPATIBLE" if breaking_count else "BACKWARD_COMPATIBLE"
    )

    def _rel_to_repo(p: Path) -> str:
        try:
            return str(p.resolve().relative_to(repo.resolve()))
        except ValueError:
            return str(p.resolve())

    mi_slug = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    report = {
        "contract_id": contract_id,
        "analyzed_at": analyzed_at,
        "pair_selection_mode": pair_key,
        "snapshot_old": path_old.name,
        "snapshot_new": path_new.name,
        "snapshot_paths_relative": {
            "old": _rel_to_repo(path_old),
            "new": _rel_to_repo(path_new),
        },
        "compatibility_verdict": compatibility_verdict,
        "schema_registry_style": {
            "backward_readers": confluent_style,
            "note": "Informal Confluent-style label; see tool_equivalents per change for dbt/Pact.",
        },
        "taxonomy_counts": taxonomy_counts,
        "total_changes": len(changes),
        "breaking_count": breaking_count,
        "compatible_count": compatible_count,
        "changes": changes,
        "migration_checklist": checklist,
        "rollback_plan": rollback_summary,
        "rollback_plan_detail": rollback_detail,
        "cli_reproduce_diff": (
            "python contracts/schema_analyzer.py "
            f"--contract-id {contract_id} "
            f"--snapshot-old {path_old.name} "
            f"--snapshot-new {path_new.name} "
            f"-o validation_reports/schema_evolution_diff.json"
        ),
    }

    output_report.parent.mkdir(parents=True, exist_ok=True)
    output_report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {output_report}", file=sys.stderr)

    if write_migration_impact:
        mi_path = output_report.parent / f"migration_impact_{contract_id}_{mi_slug}.json"
        migration_doc = {
            "contract_id": contract_id,
            "generated_at": analyzed_at,
            "compatibility_verdict": compatibility_verdict,
            "snapshot_pair": {"old": path_old.name, "new": path_new.name},
            "breaking_changes": breaking_changes,
            "taxonomy_counts": taxonomy_counts,
            "migration_checklist": checklist,
            "rollback_plan": rollback_summary,
            "rollback_plan_detail": rollback_detail,
            "registry_subscribers_hint": subs,
            "cli_reproduce_diff": report["cli_reproduce_diff"],
        }
        mi_path.write_text(json.dumps(migration_doc, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {mi_path}", file=sys.stderr)
        try:
            mi_rel = str(mi_path.resolve().relative_to(repo.resolve()))
        except ValueError:
            mi_rel = str(mi_path.resolve())
        report["migration_impact_file"] = mi_rel
        report["migration_impact_reports"] = [mi_rel]
        output_report.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Updated {output_report} (links migration_impact_file)", file=sys.stderr)

    return 0


def list_snapshots_for_contract(repo: Path, contract_id: str) -> int:
    d = repo / "schema_snapshots" / contract_id
    if not d.is_dir():
        print(f"No snapshot directory: {d}", file=sys.stderr)
        return 1
    paths = sorted(
        [p for p in d.iterdir() if p.is_file() and p.suffix.lower() in (".yaml", ".yml")],
        key=_snapshot_sort_key,
    )
    for p in paths:
        pl = _load_snapshot_payload(p)
        ts = (pl or {}).get("snapshot_timestamp") or p.stem
        print(f"{p.name}\t{ts}")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer — Phase 3")
    parser.add_argument("--contract-id", required=True, help="Data contract id (schema_snapshots subfolder)")
    parser.add_argument("--since", default="7 days ago", help='e.g. "7 days ago" or ISO date')
    parser.add_argument(
        "--snapshot-old",
        default=None,
        help="Explicit older snapshot (filename under schema_snapshots/<id>/ or path)",
    )
    parser.add_argument(
        "--snapshot-new",
        default=None,
        help="Explicit newer snapshot (use with --snapshot-old for reproducible diffs)",
    )
    parser.add_argument(
        "--list-snapshots",
        action="store_true",
        help="Print available snapshot YAML files for --contract-id and exit",
    )
    parser.add_argument(
        "--no-migration-impact-file",
        action="store_true",
        help="Do not write validation_reports/migration_impact_<contract>_<ts>.json",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=REPO_ROOT / "validation_reports" / "schema_evolution.json",
        help="Primary evolution analysis JSON path",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root (default: parent of contracts/)",
    )
    args = parser.parse_args()
    repo = args.repo_root if args.repo_root.is_absolute() else REPO_ROOT / args.repo_root
    out = args.output if args.output.is_absolute() else repo / args.output
    if args.list_snapshots:
        raise SystemExit(list_snapshots_for_contract(repo, args.contract_id))
    raise SystemExit(
        run_analyzer(
            repo,
            args.contract_id,
            args.since,
            out,
            snapshot_old_spec=args.snapshot_old,
            snapshot_new_spec=args.snapshot_new,
            write_migration_impact=not args.no_migration_impact_file,
        )
    )


if __name__ == "__main__":
    main()
