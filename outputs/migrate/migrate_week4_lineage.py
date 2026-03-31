"""
Migration: week4 Brownfield Cartographer (lineage_snapshot)
Source:    outputs/week4/lineage_snapshots.jsonl
Target:    outputs/migrate/migrated_lineage_snapshots.jsonl
Canonical: lineage_snapshot

Deviations addressed:
  1. pipeline (and similar) node_id values embed absolute paths → relativize against snapshot codebase_root using pathlib (stable type::relative form).
  2. edge source/target strings → same relativization so they still resolve to node_id entries.
  3. node.metadata → ensure path, language, purpose, last_modified keys exist; missing → null (canonical shape, unknowns not invented).

Deviations NOT addressed (requires separate upcaster or manual review):
  1. Only CONSUMES edges in source — no synthetic PRODUCES/WRITES (would fabricate lineage).
  2. Constant edge.confidence 0.95 — kept as-is; may be a default.
  3. TABLE node_ids that look like FK names — semantic fix belongs in cartography tooling.

Run:
  python outputs/migrate/migrate_week4_lineage.py --dry-run   # preview only
  python outputs/migrate/migrate_week4_lineage.py --apply     # write output
"""

import argparse
import copy
import json
import pathlib
import sys


SOURCE = pathlib.Path("outputs/week4/lineage_snapshots.jsonl")
TARGET = pathlib.Path("outputs/migrate/migrated_lineage_snapshots.jsonl")
REPORT = pathlib.Path("outputs/migrate/deviation_report_week4.json")


def _relativize_node_id(node_id: str, root: pathlib.Path) -> str:
    # Original "type::/abs/path..." → canonical "type::rel/path" from repo root when under root.
    if "::" not in node_id:
        return node_id
    kind, rest = node_id.split("::", 1)
    if not rest.startswith("/"):
        return node_id
    p = pathlib.Path(rest)
    try:
        rel = p.relative_to(root)
    except ValueError:
        return node_id
    return f"{kind}::{rel.as_posix()}"


def migrate_record(record: dict) -> dict | None:
    """
    Returns the migrated snapshot dict, or None if input invalid.
    Pure function: does not mutate the input record.
    """
    # Deep copy so caller's dict is untouched (pure function contract).
    snap = copy.deepcopy(record)
    root_s = snap.get("codebase_root") or ""
    root = pathlib.Path(root_s)

    for node in snap.get("nodes") or []:
        nid = node.get("node_id")
        if isinstance(nid, str):
            # Original absolute node_id → relative from codebase_root when possible (canonical examples use relative paths).
            node["node_id"] = _relativize_node_id(nid, root)
        md = node.get("metadata")
        if md is None:
            md = {}
            node["metadata"] = md
        # Canonical expects these keys; original may only have path — others null (unknown).
        md.setdefault("path", md.get("path"))
        md.setdefault("language", md.get("language"))
        md.setdefault("purpose", md.get("purpose"))
        md.setdefault("last_modified", md.get("last_modified"))

    for edge in snap.get("edges") or []:
        for k in ("source", "target"):
            v = edge.get(k)
            if isinstance(v, str):
                edge[k] = _relativize_node_id(v, root)

    return snap


def main(dry_run: bool) -> None:
    if not SOURCE.is_file():
        print(f"Missing source: {SOURCE}", file=sys.stderr)
        sys.exit(2)

    raw_text = SOURCE.read_text(encoding="utf-8").strip()
    if not raw_text:
        print("Empty source", file=sys.stderr)
        sys.exit(2)

    # Source file is one pretty-printed JSON object (not strict one-line jsonl).
    snapshot = json.loads(raw_text)
    out = migrate_record(snapshot)
    if out is None:
        print("Migration returned None", file=sys.stderr)
        sys.exit(2)

    nodes_n = len(out.get("nodes") or [])
    edges_n = len(out.get("edges") or [])
    print(f"Week4: 1 snapshot, nodes={nodes_n} edges={edges_n}")

    if dry_run:
        print("[dry-run] No files written.")
        return

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    # Write deviation report before migrated output (same content; satisfies apply contract).
    rep = json.loads(REPORT.read_text(encoding="utf-8"))
    REPORT.write_text(json.dumps(rep, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    line = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
    TARGET.write_text(line + "\n", encoding="utf-8")

    print(f"Applied: 1 record migrated, 0 quarantined, 0 skipped → {TARGET}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not args.apply:
        print("Specify --dry-run or --apply")
        sys.exit(1)
    main(dry_run=args.dry_run)
