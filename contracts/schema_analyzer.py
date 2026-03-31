#!/usr/bin/env python3
"""
SchemaEvolutionAnalyzer (Phase 3) — diffs timestamped contract schema snapshots,
classifies changes against the Week 7 taxonomy, and emits evolution + migration impact reports.

ContractGenerator writes: schema_snapshots/{contract_id}/{timestamp}.yaml

Usage:
  python contracts/schema_analyzer.py \\
    --contract-id week3-document-refinery-extractions \\
    --since "7 days ago" \\
    --output validation_reports/schema_evolution_week3.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
_CONTRACTS_DIR = str(REPO_ROOT / "contracts")
if _CONTRACTS_DIR not in sys.path:
    sys.path.insert(0, _CONTRACTS_DIR)

from schema_evolution import (
    blast_radius_from_lineage,
    field_to_human_line,
    flatten_contract_schema,
    list_snapshot_files,
    load_snapshot_file,
    parse_since_arg,
    snapshot_file_datetime,
)

# ---------------------------------------------------------------------------
# Contract id → generated YAML (for lineage.downstream in impact report)
# ---------------------------------------------------------------------------

CONTRACT_YAML_BY_ID: Dict[str, str] = {
    "week3-document-refinery-extractions": "week3_extractions.yaml",
    "week4-brownfield-lineage-snapshot": "week4_lineage.yaml",
    "week4-lineage-missing": "week4_lineage.yaml",
    "week5-event-sourcing-events": "week5_events.yaml",
    "langsmith-trace-record-migrated": "langsmith_traces.yaml",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_yaml_contract_lineage(repo: Path, contract_id: str) -> List[dict]:
    name = CONTRACT_YAML_BY_ID.get(contract_id)
    if not name:
        return []
    p = repo / "generated_contracts" / name
    if not p.is_file():
        return []
    try:
        import yaml

        with p.open(encoding="utf-8") as f:
            doc = yaml.safe_load(f)
    except Exception:
        return []
    if not isinstance(doc, dict):
        return []
    lin = doc.get("lineage") or {}
    down = lin.get("downstream") or []
    return down if isinstance(down, list) else []


def _type_rank(t: Optional[str]) -> int:
    if t == "integer":
        return 1
    if t == "number":
        return 2
    if t == "string":
        return 3
    return 0


def _scale_suspicion(rec: FieldRecord) -> str:
    """Heuristic: probability-like 0–1 vs percentage 0–100."""
    if rec.type not in ("number", "integer"):
        return "none"
    if rec.minimum is not None and rec.maximum is not None:
        if rec.minimum >= 0 and rec.maximum <= 1:
            return "unit_interval"
        if rec.minimum >= 0 and rec.maximum >= 100:
            return "percentage_scale_suspect"
    return "unknown_numeric"


def classify_enum_change(
    old_e: Optional[Tuple[str, ...]], new_e: Optional[Tuple[str, ...]]
) -> Optional[dict]:
    if old_e is None and new_e is None:
        return None
    os_, ns = set(old_e or ()), set(new_e or ())
    if ns >= os_ and len(ns) > len(os_):
        added = sorted(ns - os_)
        return {
            "change_type": "ENUM_VALUES_ADDED",
            "example": f'Add {added[:5]!r} to enum',
            "backward_compatible": True,
            "required_action": "Additive: notify all consumers of new allowed values.",
            "taxonomy_row": "Change enum values (additive)",
        }
    if os_ - ns:
        removed = sorted(os_ - ns)
        return {
            "change_type": "ENUM_VALUES_REMOVED",
            "example": f"Remove {removed!r} from enum",
            "backward_compatible": False,
            "required_action": "Removal of enum values is a breaking change: deprecation + consumer ack.",
            "taxonomy_row": "Change enum values (removal)",
        }
    return None


def classify_type_change(old: FieldRecord, new: FieldRecord) -> dict:
    ot, nt = old.type, new.type
    o_scale, n_scale = _scale_suspicion(old), _scale_suspicion(new)
    if ot == nt and ot in ("number", "integer"):
        # Same declared type but bounds shifted (e.g. 0–1 → 0–100)
        if o_scale == "unit_interval" and n_scale == "percentage_scale_suspect":
            return {
                "change_type": "TYPE_NARROWING_NUMERIC_SCALE",
                "example": "float 0.0–1.0 → int 0–100 (semantic scale shift)",
                "backward_compatible": False,
                "required_action": "CRITICAL: migration plan with rollback; re-establish statistical baselines; blast radius mandatory.",
                "taxonomy_row": "Change type (narrowing) / scale",
                "severity": "CRITICAL",
            }
    ro, rn = _type_rank(ot), _type_rank(nt)
    if ro and rn and rn > ro:
        return {
            "change_type": "TYPE_WIDENING",
            "example": f"{ot} → {nt}",
            "backward_compatible": True,
            "required_action": "Validate no precision loss on existing data; re-run statistical checks.",
            "taxonomy_row": "Change type (widening)",
            "severity": "LOW",
        }
    if ro and rn and rn < ro:
        return {
            "change_type": "TYPE_NARROWING",
            "example": f"{ot} → {nt}",
            "backward_compatible": False,
            "required_action": "CRITICAL where data loss risk: migration + rollback; blast radius mandatory.",
            "taxonomy_row": "Change type (narrowing)",
            "severity": "HIGH",
        }
    if ot != nt:
        return {
            "change_type": "TYPE_CHANGED",
            "example": f"{ot} → {nt}",
            "backward_compatible": False,
            "required_action": "Assess consumers; migration or coercion layer.",
            "taxonomy_row": "Change type (incompatible)",
            "severity": "HIGH",
        }
    return {
        "change_type": "TYPE_UNCHANGED",
        "example": "",
        "backward_compatible": True,
        "required_action": "None (type).",
        "taxonomy_row": "",
        "severity": "LOW",
    }


def classify_range_change(old: FieldRecord, new: FieldRecord) -> Optional[dict]:
    if old.minimum == new.minimum and old.maximum == new.maximum:
        return None
    narrower = False
    if old.minimum is not None and new.minimum is not None and new.minimum > old.minimum:
        narrower = True
    if old.maximum is not None and new.maximum is not None and new.maximum < old.maximum:
        narrower = True
    if narrower:
        return {
            "change_type": "CONSTRAINT_NARROWED",
            "example": f"range [{old.minimum},{old.maximum}] → [{new.minimum},{new.maximum}]",
            "backward_compatible": False,
            "required_action": "Validate existing data; notify consumers; may reject previously valid payloads.",
            "taxonomy_row": "Constraint narrowing",
            "severity": "MEDIUM",
        }
    return {
        "change_type": "CONSTRAINT_WIDENED",
        "example": f"range [{old.minimum},{old.maximum}] → [{new.minimum},{new.maximum}]",
        "backward_compatible": True,
        "required_action": "Usually compatible; confirm no downstream assumptions on bounds.",
        "taxonomy_row": "Constraint widening",
        "severity": "LOW",
    }


def _pair_suspected_renames(
    o_map: Dict[str, FieldRecord], n_map: Dict[str, FieldRecord], removed: Set[str], added: Set[str]
) -> Tuple[List[Tuple[str, str]], Set[str], Set[str]]:
    pairs: List[Tuple[str, str]] = []
    used_add: Set[str] = set()
    used_rem: Set[str] = set()
    for rp in sorted(removed):
        old = o_map[rp]
        best: Optional[Tuple[str, float]] = None
        for ap in added:
            if ap in used_add:
                continue
            new = n_map[ap]
            if old.type != new.type:
                continue
            parent_o, seg_o = rp.rsplit(".", 1) if "." in rp else ("", rp)
            parent_n, seg_n = ap.rsplit(".", 1) if "." in ap else ("", ap)
            if parent_o != parent_n:
                continue
            if seg_o == seg_n:
                continue
            if abs(len(seg_o) - len(seg_n)) > 8:
                continue
            dist = _levenshtein(seg_o, seg_n)
            if dist <= 2 or (seg_o in seg_n or seg_n in seg_o):
                score = dist
                if best is None or score < best[1]:
                    best = (ap, score)
        if best:
            ap = best[0]
            pairs.append((rp, ap))
            used_add.add(ap)
            used_rem.add(rp)
    remain_add = added - used_add
    remain_rem = removed - used_rem
    return pairs, remain_add, remain_rem


def diff_snapshots(
    old_schema: dict, new_schema: dict
) -> Tuple[List[dict], bool, List[str]]:
    """
    Returns (classified_change_dicts, any_breaking, human_diff_lines).
    """
    o_map = flatten_contract_schema(old_schema)
    n_map = flatten_contract_schema(new_schema)
    o_paths, n_paths = set(o_map), set(n_map)
    added, removed = n_paths - o_paths, o_paths - n_paths
    rename_pairs, added, removed = _pair_suspected_renames(o_map, n_map, removed, added)
    human_lines: List[str] = []
    changes: List[dict] = []
    breaking = False

    for rp, ap in rename_pairs:
        human_lines.append(f"~ RENAME? {rp} → {ap} (heuristic; verify in VCS)")
        breaking = True
        changes.append(
            {
                "path": f"{rp}→{ap}",
                "change_type": "RENAME_COLUMN_SUSPECTED",
                "example": f"{rp.split('.')[-1]} → {ap.split('.')[-1]}",
                "backward_compatible": False,
                "required_action": "Deprecation with alias column; notify via blast radius; ≥1 sprint before removal.",
                "taxonomy_row": "Rename column",
                "severity": "HIGH",
            }
        )

    for p in sorted(added):
        rec = n_map[p]
        nullable = rec.required is not True
        human_lines.append(f"+ ADD {field_to_human_line(p, rec)}")
        if nullable:
            changes.append(
                {
                    "path": p,
                    "change_type": "ADD_COLUMN_NULLABLE",
                    "example": f"ADD COLUMN {p} (nullable)",
                    "backward_compatible": True,
                    "required_action": "None. Downstream consumers can ignore the new column.",
                    "taxonomy_row": "Add nullable column",
                    "severity": "LOW",
                }
            )
        else:
            breaking = True
            changes.append(
                {
                    "path": p,
                    "change_type": "ADD_COLUMN_NOT_NULL",
                    "example": f"ADD COLUMN {p} NOT NULL",
                    "backward_compatible": False,
                    "required_action": "Coordinate producers; default or migration; block deployment until producers updated.",
                    "taxonomy_row": "Add non-nullable column",
                    "severity": "CRITICAL",
                }
            )

    for p in sorted(removed):
        rec = o_map[p]
        human_lines.append(f"- DROP {field_to_human_line(p, rec)}")
        breaking = True
        changes.append(
            {
                "path": p,
                "change_type": "DROP_COLUMN",
                "example": f"DROP COLUMN {p}",
                "backward_compatible": False,
                "required_action": "Deprecation ≥2 sprints; blast radius report; written consumer acknowledgement.",
                "taxonomy_row": "Remove column",
                "severity": "CRITICAL",
            }
        )

    for p in sorted(o_paths & n_paths):
        old, new = o_map[p], n_map[p]
        if old.fingerprint() == new.fingerprint():
            continue
        human_lines.append(f"~ MODIFY {p}")
        human_lines.append(f"    - {field_to_human_line(p, old)}")
        human_lines.append(f"    + {field_to_human_line(p, new)}")

        if old.required is False and new.required is True:
            breaking = True
            changes.append(
                {
                    "path": p,
                    "change_type": "REQUIRED_STRENGTHENED",
                    "example": f"{p}: optional → required",
                    "backward_compatible": False,
                    "required_action": "Treat as non-nullable add: coordinate producers and defaults.",
                    "taxonomy_row": "Add non-nullable column (semantic)",
                    "severity": "CRITICAL",
                }
            )

        ec = classify_enum_change(old.enum, new.enum)
        if ec:
            if not ec["backward_compatible"]:
                breaking = True
            changes.append({**ec, "path": p})

        tc = classify_type_change(old, new)
        if tc["change_type"] not in ("TYPE_UNCHANGED",):
            if not tc["backward_compatible"]:
                breaking = True
            changes.append({**tc, "path": p})

        rc = classify_range_change(old, new)
        if rc:
            if not rc["backward_compatible"]:
                breaking = True
            changes.append({**rc, "path": p})

        if old.format != new.format or old.pattern != new.pattern:
            breaking = True
            changes.append(
                {
                    "path": p,
                    "change_type": "FORMAT_OR_PATTERN_CHANGED",
                    "example": f"format {old.format!r}→{new.format!r}, pattern {old.pattern!r}→{new.pattern!r}",
                    "backward_compatible": False,
                    "required_action": "Validate payloads; versioning or dual-read period.",
                    "taxonomy_row": "Format / validation change",
                    "severity": "HIGH",
                }
            )

    return changes, breaking, human_lines


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    la, lb = len(a), len(b)
    dp = list(range(lb + 1))
    for i in range(1, la + 1):
        prev, dp[0] = dp[0], i
        for j in range(1, lb + 1):
            cur = dp[j]
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[j] = min(dp[j] + 1, dp[j - 1] + 1, prev + cost)
            prev = cur
    return dp[lb]


def build_migration_impact(
    contract_id: str,
    pairwise_breaking: List[dict],
    repo: Path,
    human_readable_diff: str,
) -> dict:
    blast = blast_radius_from_lineage(repo)
    downstream = load_yaml_contract_lineage(repo, contract_id)
    consumers = []
    for d in downstream:
        if not isinstance(d, dict):
            continue
        cid = d.get("id") or d.get("consumer_node_id")
        consumers.append(
            {
                "consumer_ref": str(cid) if cid else "unknown",
                "description": d.get("description", ""),
                "breaking_if_changed": d.get("breaking_if_changed") or [],
            }
        )
    failure_modes = []
    for c in consumers[:40]:
        failure_modes.append(
            {
                "consumer": c["consumer_ref"],
                "failure_mode": "Downstream models/tests expecting removed fields, stricter types, or old enums will fail at read time.",
                "mitigation": "Version the contract, add compatibility views, or schedule coordinated release with feature flags.",
            }
        )
    if not failure_modes:
        failure_modes.append(
            {
                "consumer": "unknown_downstream",
                "failure_mode": "Any implicit JSON consumer (dbt, notebooks, services) may break on strict parsing.",
                "mitigation": "Run ValidationRunner; publish migration impact; require PR comments from owning teams.",
            }
        )

    checklist = [
        "Freeze producer deployments until rollback path is validated.",
        "Notify consumers listed in blast_radius and lineage.downstream.",
        "Ship dual-write or additive schema phase if applicable.",
        "Run ContractGenerator + ValidationRunner on migrated sample data.",
        "Reset or re-baseline schema_snapshots/baselines.json where statistical contracts apply.",
        "Obtain written acknowledgement (ticket/PR) for DROP or breaking TYPE changes.",
        "Schedule removal only after deprecation window (≥2 sprints for DROP).",
    ]
    rollback = [
        "Revert producer commit that emitted the new schema; restore prior ContractGenerator output.",
        "Restore previous schema snapshot YAML from schema_snapshots/{contract_id}/ for documentation.",
        "Re-ingest last known-good JSONL export if data was transformed.",
        "Re-run SchemaEvolutionAnalyzer to confirm zero breaking diffs against prior snapshot.",
    ]
    return {
        "migration_impact_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": utc_now_iso(),
        "compatibility_verdict": "BREAKING",
        "human_readable_diff": human_readable_diff,
        "pairwise_breaking_transitions": pairwise_breaking,
        "blast_radius": blast,
        "lineage_downstream_from_contract": downstream[:50],
        "per_consumer_failure_mode_analysis": failure_modes,
        "ordered_migration_checklist": checklist,
        "rollback_plan": rollback,
    }


def run_analyzer(
    repo: Path,
    contract_id: str,
    since: str,
    output_report: Path,
    migration_dir: Path,
) -> int:
    cutoff = parse_since_arg(since)
    all_files = list_snapshot_files(repo, contract_id)
    if not all_files:
        print(f"No schema snapshots under schema_snapshots/{contract_id}/. Run ContractGenerator first.", file=sys.stderr)
        return 2

    enriched: List[Tuple[Path, dict, datetime]] = []
    for p in all_files:
        payload = load_snapshot_file(p)
        ts = snapshot_file_datetime(p, payload)
        enriched.append((p, payload, ts))
    enriched.sort(key=lambda x: x[2])

    in_window = [(p, pl, ts) for p, pl, ts in enriched if ts >= cutoff]
    if len(in_window) < 2:
        # Fall back: diff last two snapshots overall (still report window metadata)
        if len(enriched) < 2:
            print(
                "Need at least two snapshots to diff consecutive versions. Run ContractGenerator twice.",
                file=sys.stderr,
            )
            return 3
        pair_source = "last_two_global_fallback"
        to_diff = enriched[-2:]
    else:
        pair_source = "consecutive_within_since_window"
        to_diff = in_window

    pairwise_evolution: List[dict] = []
    breaking_pairs: List[dict] = []
    migration_files: List[str] = []

    for i in range(len(to_diff) - 1):
        p_old, pl_old, ts_old = to_diff[i]
        p_new, pl_new, ts_new = to_diff[i + 1]
        s_old = pl_old.get("schema") or {}
        s_new = pl_new.get("schema") or {}
        changes, breaking, human_lines = diff_snapshots(s_old, s_new)
        block = {
            "from_snapshot_file": str(p_old.relative_to(repo)),
            "to_snapshot_file": str(p_new.relative_to(repo)),
            "from_timestamp": ts_old.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to_timestamp": ts_new.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "human_readable_diff": "\n".join(human_lines) if human_lines else "(no structural diff)",
            "classified_changes": changes,
            "breaking_change": breaking,
            "summary_counts": {
                "changes_detected": len(changes),
                "paths_old": len(flatten_contract_schema(s_old)),
                "paths_new": len(flatten_contract_schema(s_new)),
            },
        }
        pairwise_evolution.append(block)
        if breaking:
            breaking_pairs.append(block)

    breaking_any = bool(breaking_pairs)
    if breaking_any:
        ts_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe_id = re.sub(r"[^a-zA-Z0-9._-]+", "_", contract_id)
        mig_path = migration_dir / f"migration_impact_{safe_id}_{ts_tag}.json"
        migration_dir.mkdir(parents=True, exist_ok=True)
        hr_diff = "\n\n--- transition ---\n\n".join(b["human_readable_diff"] for b in breaking_pairs)
        doc = build_migration_impact(contract_id, breaking_pairs, repo, hr_diff)
        with mig_path.open("w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)
        migration_files.append(str(mig_path.relative_to(repo)))

    report = {
        "report_id": str(uuid.uuid4()),
        "contract_id": contract_id,
        "generated_at": utc_now_iso(),
        "since_cutoff_utc": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pair_selection": pair_source,
        "snapshots_total": len(enriched),
        "snapshots_in_since_window": len(in_window),
        "snapshots_considered": [{"file": str(p.relative_to(repo)), "timestamp_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ")} for p, _, ts in to_diff],
        "pairwise_evolution": pairwise_evolution,
        "breaking_change_detected": breaking_any,
        "migration_impact_reports": migration_files,
    }
    output_report.parent.mkdir(parents=True, exist_ok=True)
    with output_report.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    print(f"Wrote {output_report}", file=sys.stderr)
    if migration_files:
        print(f"Wrote migration impact: {migration_files}", file=sys.stderr)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="SchemaEvolutionAnalyzer — Phase 3")
    parser.add_argument("--contract-id", required=True, help="Data contract id (YAML id: field)")
    parser.add_argument("--since", default="7 days ago", help='e.g. "7 days ago" or ISO date')
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "validation_reports" / "schema_evolution.json",
        help="Evolution report JSON path",
    )
    parser.add_argument(
        "--migration-dir",
        type=Path,
        default=REPO_ROOT / "validation_reports",
        help="Directory for migration_impact_*.json",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root (default: parent of contracts/)",
    )
    args = parser.parse_args()
    out = args.output if args.output.is_absolute() else args.repo_root / args.output
    mig = args.migration_dir if args.migration_dir.is_absolute() else args.repo_root / args.migration_dir
    raise SystemExit(run_analyzer(args.repo_root, args.contract_id, args.since, out, mig))


if __name__ == "__main__":
    main()
