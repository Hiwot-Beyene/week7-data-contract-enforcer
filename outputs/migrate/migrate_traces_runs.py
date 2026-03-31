"""
Migration: LangSmith trace export (trace_record)
Source:    outputs/traces/runs.jsonl
Target:    outputs/migrate/migrated_runs.jsonl
Canonical: trace_record

Deviations addressed:
  1. Wrap custom export {inputs, outputs, metadata, langsmith} into canonical trace_record top-level keys.
  2. id: deterministic UUID5 (native LangSmith run id not present in this file).
  3. name: from tracing project + session id when available.
  4. run_type: "chain" (orchestrator-style graph; documented approximation).
  5. session_id: UUID5 from raw session string (e.g. sess-dec-057c94ad).
  6. tags: from LANGSMITH_PROJECT / project name (non-secret labels only).
  7. Preserve metadata + langsmith under inputs._migration_source_envelope (canonical has no home for them).
  8. metadata.revision_id \"*-dirty\" → strip suffix in preserved copy; document dirty working tree at export.

Deviations NOT addressed (requires LangSmith re-export or backfill):
  1. start_time, end_time, token counts, total_cost — absent; set null (do not invent measurements).
  2. Strict validator rules (end_time > start_time, total_tokens = sum) until fields are backfilled.

Run:
  python outputs/migrate/migrate_traces_runs.py --dry-run   # preview only
  python outputs/migrate/migrate_traces_runs.py --apply     # write output
"""

import argparse
import copy
import json
import pathlib
import sys
import uuid

SOURCE = pathlib.Path("outputs/traces/runs.jsonl")
TARGET = pathlib.Path("outputs/migrate/migrated_runs.jsonl")
REPORT = pathlib.Path("outputs/migrate/deviation_report_traces.json")
QUARANTINE = pathlib.Path("outputs/migrate/quarantine/quarantine.jsonl")

_NS_TRACE_ID = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/traces/run_id")
_NS_SESSION = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/traces/session_id")


def _strip_dirty_revision(rev: object) -> object:
    # Original \"401e57e-dirty\" → keep commit prefix for lineage; \"-dirty\" only flags uncommitted tree at export time.
    if isinstance(rev, str) and rev.endswith("-dirty"):
        return rev[: -len("-dirty")]
    return rev


def migrate_record(record: dict) -> dict | None:
    """
    Returns the migrated trace_record, or None if quarantined.
    Pure function: does not mutate the input record.
    """
    if not isinstance(record, dict):
        return None

    inputs_block = record.get("inputs")
    outputs_block = record.get("outputs")
    # Original must expose graph inputs/outputs; otherwise cannot map to canonical shape safely.
    if not isinstance(inputs_block, dict) or not isinstance(outputs_block, dict):
        return None

    raw_meta = copy.deepcopy(record.get("metadata"))
    if isinstance(raw_meta, dict) and "revision_id" in raw_meta:
        raw_meta = dict(raw_meta)
        raw_meta["revision_id"] = _strip_dirty_revision(raw_meta.get("revision_id"))
        # Preserve original under audit key (string copy of original revision if needed — original top-level lost in envelope).
        orig_rev = record.get("metadata", {}).get("revision_id") if isinstance(record.get("metadata"), dict) else None
        if orig_rev is not None and raw_meta.get("revision_id") != orig_rev:
            raw_meta["revision_id_original"] = orig_rev

    envelope = {
        "metadata": raw_meta if isinstance(raw_meta, dict) else record.get("metadata"),
        "langsmith": copy.deepcopy(record.get("langsmith")),
    }

    # Original inner inputs → canonical inputs, plus envelope so metadata/langsmith are not dropped.
    canonical_inputs = copy.deepcopy(inputs_block)
    canonical_inputs["_migration_source_envelope"] = envelope

    # Fingerprint excludes envelope we are building (stable over source inputs/outputs/meta/langsmith).
    fingerprint = json.dumps(
        {
            "inputs": inputs_block,
            "outputs": outputs_block,
            "metadata": record.get("metadata"),
            "langsmith": record.get("langsmith"),
        },
        sort_keys=True,
        default=str,
    )
    trace_id = str(uuid.uuid5(_NS_TRACE_ID, fingerprint))

    session_raw = None
    if isinstance(inputs_block.get("session_id"), str):
        session_raw = inputs_block["session_id"]
    elif isinstance(outputs_block.get("session_id"), str):
        session_raw = outputs_block["session_id"]
    session_uuid = str(uuid.uuid5(_NS_SESSION, session_raw or "missing-session"))

    ls = record.get("langsmith") or {}
    tp = (ls.get("tracing_project") or {}) if isinstance(ls, dict) else {}
    proj_name = tp.get("name") or "unknown-project"
    meta = record.get("metadata") or {}
    ls_proj = meta.get("LANGSMITH_PROJECT") if isinstance(meta, dict) else None
    tags = []
    if isinstance(ls_proj, str) and ls_proj:
        tags.append(ls_proj)
    if isinstance(proj_name, str) and proj_name and proj_name not in tags:
        tags.append(proj_name)
    tags.append("trace_migration_v1")

    # Original had no chain name — derive from tracing context + session suffix for humans.
    name_suffix = session_raw or "no-session"
    name = f"{proj_name}/{name_suffix}"

    return {
        "id": trace_id,
        "name": name,
        # Original export is an orchestration trace (inputs/outputs graph), not a raw llm leaf — \"chain\" is documented choice.
        "run_type": "chain",
        "inputs": canonical_inputs,
        "outputs": copy.deepcopy(outputs_block),
        "error": None,
        # Original has no wall times in export — null (do not fabricate start/end).
        "start_time": None,
        "end_time": None,
        # Original has no token breakdown — null (do not set 0).
        "total_tokens": None,
        "prompt_tokens": None,
        "completion_tokens": None,
        "total_cost": None,
        "tags": tags,
        "parent_run_id": None,
        "session_id": session_uuid,
    }


def _write_report(record_count: int) -> None:
    base = json.loads(REPORT.read_text(encoding="utf-8"))
    base["record_count"] = record_count
    REPORT.write_text(
        json.dumps(base, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main(dry_run: bool) -> None:
    if not SOURCE.is_file():
        print(f"Missing source: {SOURCE}", file=sys.stderr)
        sys.exit(2)

    raw_text = SOURCE.read_text(encoding="utf-8").strip()
    if not raw_text:
        print("Empty source", file=sys.stderr)
        sys.exit(2)

    try:
        blob = json.loads(raw_text)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}", file=sys.stderr)
        sys.exit(2)

    records: list[dict]
    if isinstance(blob, list):
        records = [x for x in blob if isinstance(x, dict)]
    else:
        records = [blob] if isinstance(blob, dict) else []

    migrated: list[dict] = []
    quarantine_rows: list[dict] = []

    for i, rec in enumerate(records, start=1):
        out = migrate_record(rec)
        if out is None:
            q = dict(rec) if isinstance(rec, dict) else {"_raw": rec}
            q["reason"] = "missing_inputs_or_outputs_dict"
            q["quarantine_source"] = str(SOURCE)
            q["source_index"] = i
            quarantine_rows.append(q)
        else:
            migrated.append(out)

    print(
        f"Traces: records={len(records)} migrated={len(migrated)} quarantined={len(quarantine_rows)}"
    )

    if dry_run:
        print("[dry-run] No files written.")
        return

    QUARANTINE.parent.mkdir(parents=True, exist_ok=True)
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    _write_report(len(records))

    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) for r in migrated) + "\n",
        encoding="utf-8",
    )

    if quarantine_rows:
        with QUARANTINE.open("a", encoding="utf-8") as fq:
            for q in quarantine_rows:
                fq.write(json.dumps(q, ensure_ascii=False) + "\n")

    print(
        f"Applied: {len(migrated)} records migrated, {len(quarantine_rows)} quarantined, 0 skipped → {TARGET}"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not args.dry_run and not args.apply:
        print("Specify --dry-run or --apply")
        sys.exit(1)
    main(dry_run=args.dry_run)
