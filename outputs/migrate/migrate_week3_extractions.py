"""
Migration: week3 Document Refinery (extraction_record)
Source:    outputs/week3/extractions.jsonl
Target:    outputs/migrate/migrated_extractions.jsonl
Canonical: extraction_record

Deviations addressed:
  1. timestamp → extracted_at (same string value; canonical key name).
  2. document_name → source_path (filename-only preserved; canonical prefers absolute URL/path — documented approximation).
  3. confidence_score → single extracted_facts[] entry with .confidence (document-level legacy field, not real multi-fact extraction).
  4. prompt_tokens / completion_tokens → token_count.input / token_count.output when present; else null.
  5. doc_id: 16-char hex → deterministic UUID5 (canonical asks for uuid-v4 shape; v5 is stable and valid UUID syntax).
  6. source_hash, extraction_model → null (unknown in source; not invented).
  7. entities → [] (empty; no entity export in source).

Deviations NOT addressed (requires separate upcaster or manual review):
  1. Verbatim multi-fact extraction and entity graphs — source never contained them.

Run:
  python outputs/migrate/migrate_week3_extractions.py --dry-run   # preview only
  python outputs/migrate/migrate_week3_extractions.py --apply     # write output
"""

import argparse
import json
import pathlib
import sys
import uuid

SOURCE = pathlib.Path("outputs/week3/extractions.jsonl")
TARGET = pathlib.Path("outputs/migrate/migrated_extractions.jsonl")
REPORT = pathlib.Path("outputs/migrate/deviation_report_week3.json")
QUARANTINE = pathlib.Path("outputs/migrate/quarantine/quarantine.jsonl")

# Stable namespaces for deterministic UUIDs (not claiming RFC-4122 v4 randomness).
_NS_DOC = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/week3/doc_id")
_NS_FACT = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/week3/fact_id")


def migrate_record(record: dict) -> dict | None:
    """
    Returns the migrated record, or None if the record is quarantined.
    Every transformation is commented with: original → canonical reasoning.
    """
    # Original doc_id "doc123" is non-hex test data → quarantine per contract (do not migrate).
    raw_doc_id = record.get("doc_id")
    if raw_doc_id == "doc123":
        return None

    # Original 16-char hex is not UUID v4 → UUID5(namespace, hex) gives stable canonical-compatible id string.
    new_doc_id = str(uuid.uuid5(_NS_DOC, str(raw_doc_id)))

    # Original key "timestamp" → canonical "extracted_at" (ISO string unchanged).
    extracted_at = record.get("timestamp")

    # Original "document_name" (filename) → "source_path"; canonical wants absolute/https — we keep literal filename only (safe, lossy vs ideal).
    source_path = record.get("document_name")

    # Original top-level confidence_score → belongs inside extracted_facts[*].confidence per canonical.
    conf = record.get("confidence_score")
    notes = record.get("notes")
    # Original notes may explain low confidence (e.g. layout_vision_fallback) → embed in source_excerpt for audit trail.
    excerpt = (
        f"[legacy notes] {notes}"
        if notes
        else "[legacy aggregate confidence; no verbatim source span in export]"
    )
    fact_id = str(uuid.uuid5(_NS_FACT, f"{raw_doc_id}|{extracted_at}|{conf}"))
    extracted_facts = [
        {
            "fact_id": fact_id,
            "text": "Legacy document-level extraction confidence (source had no per-fact breakdown).",
            "entity_refs": [],
            "confidence": float(conf) if conf is not None else None,
            "page_ref": None,
            "source_excerpt": excerpt,
        }
    ]

    # Original optional prompt_tokens/completion_tokens → canonical token_count object; missing keys → null (unknown).
    token_count = {
        "input": record.get("prompt_tokens"),
        "output": record.get("completion_tokens"),
    }

    return {
        "doc_id": new_doc_id,
        "source_path": source_path,
        "source_hash": None,
        "extracted_facts": extracted_facts,
        "entities": [],
        "extraction_model": None,
        "processing_time_ms": record.get("processing_time_ms"),
        "token_count": token_count,
        "extracted_at": extracted_at,
    }


def _write_report(record_count: int) -> None:
    # Refresh record_count only; keep deviation list shape {file, record_count, deviations}.
    report_path = REPORT
    base = json.loads(report_path.read_text(encoding="utf-8"))
    base["record_count"] = record_count
    report_path.write_text(
        json.dumps(base, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main(dry_run: bool) -> None:
    if not SOURCE.is_file():
        print(f"Missing source: {SOURCE}", file=sys.stderr)
        sys.exit(2)

    lines = [ln for ln in SOURCE.read_text(encoding="utf-8").splitlines() if ln.strip()]
    record_count = len(lines)
    migrated_rows: list[dict] = []
    quarantine_rows: list[dict] = []
    line_no = 0

    for line in lines:
        line_no += 1
        record = json.loads(line)
        out = migrate_record(record)
        if out is None:
            # Quarantine: preserve original record + reason (test id).
            q = dict(record)
            q["reason"] = "test_record_non_hex_id"
            q["quarantine_source"] = str(SOURCE)
            q["source_line"] = line_no
            quarantine_rows.append(q)
        else:
            migrated_rows.append(out)

    print(
        f"Week3: lines={record_count} migrated={len(migrated_rows)} quarantined={len(quarantine_rows)}"
    )

    if dry_run:
        print("[dry-run] No files written.")
        return

    QUARANTINE.parent.mkdir(parents=True, exist_ok=True)
    _write_report(record_count)

    TARGET.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in migrated_rows) + "\n",
        encoding="utf-8",
    )

    with QUARANTINE.open("a", encoding="utf-8") as fq:
        for q in quarantine_rows:
            fq.write(json.dumps(q, ensure_ascii=False) + "\n")

    print(
        f"Applied: {len(migrated_rows)} records migrated, {len(quarantine_rows)} quarantined, 0 skipped → {TARGET}"
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
