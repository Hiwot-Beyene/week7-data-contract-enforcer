"""
Migration: week5 Event Sourcing (event_record)
Source:    outputs/week5/events.jsonl
Target:    outputs/migrate/migrated_events.jsonl
Canonical: event_record

Deviations addressed:
  1. Add canonical envelope: event_id (UUID5), aggregate_id (UUID5), aggregate_type, sequence_number, metadata, schema_version, occurred_at.
  2. recorded_at: normalize +00:00 style to UTC Z (same instant, canonical-friendly formatting).
  3. payload: coerce string *_usd fields to float where present (safe parse; failures quarantined).
  4. Top-level stream_id / event_version removed from migrated rows (recover from source file).

Deviations NOT addressed (requires separate upcaster or manual review):
  1. CreditAnalysisCompleted event_version 2 with null denormalized payload fields — requires UpcasterRegistry entry — see contracts/upcasters.py (read-time; do not fabricate mirrors here).
  2. metadata.revision_id \"*-dirty\" — not present in this export; no strip applied.
  3. Naive payload timestamps (no timezone): occurred_at falls back to recorded_at (documented approximation; Z not appended to payload strings).

Run:
  python outputs/migrate/migrate_week5_events.py --dry-run   # preview only
  python outputs/migrate/migrate_week5_events.py --apply     # write output
"""

import argparse
import json
import pathlib
import re
import sys
import uuid
from datetime import datetime, timezone

SOURCE = pathlib.Path("outputs/week5/events.jsonl")
TARGET = pathlib.Path("outputs/migrate/migrated_events.jsonl")
REPORT = pathlib.Path("outputs/migrate/deviation_report_week5.json")
QUARANTINE = pathlib.Path("outputs/migrate/quarantine/quarantine.jsonl")

_EVENT_NS = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/week5/event_id")
_AGG_NS = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/week5/aggregate_id")
_CORR_NS = uuid.uuid5(uuid.NAMESPACE_URL, "data-contract-enforcer/week5/correlation_id")

# Lower number = preferred primary business timestamp for occurred_at.
_AT_PRIORITY = {
    "submitted_at": 10,
    "uploaded_at": 15,
    "added_at": 18,
    "validated_at": 20,
    "started_at": 22,
    "initiated_at": 24,
    "created_at": 26,
    "opened_at": 28,
    "consumed_at": 30,
    "completed_at": 35,
    "generated_at": 38,
    "assessed_at": 40,
    "ready_at": 42,
    "requested_at": 44,
    "written_at": 46,
    "reviewed_at": 70,
    "declined_at": 72,
    "approved_at": 74,
    "evaluated_at": 48,
}


def _is_tz_aware(ts: str) -> bool:
    if not isinstance(ts, str):
        return False
    if ts.endswith("Z"):
        return True
    return bool(re.search(r"[+-]\d{2}:\d{2}$", ts))


def _parse_iso(ts: str) -> datetime | None:
    if not isinstance(ts, str):
        return None
    try:
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _to_utc_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    utc = dt.astimezone(timezone.utc)
    return utc.isoformat().replace("+00:00", "Z")


def recorded_to_z(s: str) -> str:
    # Original recorded_at may use "+00:00" — canonical examples use Z; same instant after normalization.
    dt = _parse_iso(s)
    if dt is None:
        return s
    return _to_utc_z(dt)


def _iter_at_fields(obj, depth: int = 0):
    if depth > 8:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.endswith("_at") and isinstance(v, str):
                yield (_AT_PRIORITY.get(k, 55), k, v)
            else:
                yield from _iter_at_fields(v, depth + 1)
    elif isinstance(obj, list):
        for x in obj:
            yield from _iter_at_fields(x, depth + 1)


def pick_occurred_at(payload: dict, recorded_z: str) -> str:
    # Prefer first timezone-aware *at field (by priority) that is <= recorded_at; else use recorded_at as approximation.
    rec_dt = _parse_iso(recorded_z)
    candidates = sorted(_iter_at_fields(payload), key=lambda t: (t[0], t[1]))
    for _pr, _key, val in candidates:
        if not _is_tz_aware(val):
            continue
        ev_dt = _parse_iso(val)
        if ev_dt is None or rec_dt is None:
            continue
        ev_utc = ev_dt.astimezone(timezone.utc) if ev_dt.tzinfo else ev_dt.replace(tzinfo=timezone.utc)
        rec_utc = rec_dt.astimezone(timezone.utc) if rec_dt.tzinfo else rec_dt.replace(tzinfo=timezone.utc)
        if ev_utc <= rec_utc:
            return _to_utc_z(ev_dt)
    return recorded_z


def coerce_usd_strings(obj):
    """
    Recursively convert string values for keys ending in _usd to float.
    Returns (coerced_object, error_message_or_None).
    """
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if k.endswith("_usd") and isinstance(v, str):
                try:
                    out[k] = float(v.strip())
                except ValueError:
                    return None, f"usd_coerce_failed:{k}={v!r}"
            elif isinstance(v, (dict, list)):
                nv, err = coerce_usd_strings(v)
                if err:
                    return None, err
                out[k] = nv
            else:
                out[k] = v
        return out, None
    if isinstance(obj, list):
        out = []
        for item in obj:
            nv, err = coerce_usd_strings(item)
            if err:
                return None, err
            out.append(nv)
        return out, None
    return obj, None


def infer_aggregate_uuid(payload: dict, stream_id: str) -> str:
    app = payload.get("application_id") if isinstance(payload, dict) else None
    if isinstance(app, str) and app:
        return str(uuid.uuid5(_AGG_NS, f"app:{app}"))
    return str(uuid.uuid5(_AGG_NS, f"stream:{stream_id}"))


def infer_aggregate_type(stream_id: str, payload: dict) -> str:
    app = payload.get("application_id") if isinstance(payload, dict) else None
    if isinstance(app, str) and app:
        return "LoanApplication"
    if stream_id.startswith("docpkg-"):
        return "DocumentPackage"
    if stream_id.startswith("agent-"):
        return "AgentSession"
    return "SystemAggregate"


def correlation_uuid(payload: dict, stream_id: str) -> str:
    sid = None
    if isinstance(payload, dict):
        sid = payload.get("session_id")
    if isinstance(sid, str) and sid:
        return str(uuid.uuid5(_CORR_NS, f"session:{sid}"))
    return str(uuid.uuid5(_CORR_NS, f"stream:{stream_id}"))


def migrate_record(record: dict) -> dict | None:
    """
    Returns the migrated record, or None if the record is quarantined.
    Expects record['_migrate'] with keys:
      event_id, aggregate_id, aggregate_type, sequence_number, correlation_id, occurred_at, recorded_at
    Expects payload already numeric-coerced.
    """
    ctx = record.get("_migrate")
    if not ctx:
        return None
    payload = record.get("payload")
    if not isinstance(payload, dict):
        payload = {}

    metadata = {
        "causation_id": None,
        "correlation_id": ctx["correlation_id"],
        "user_id": None,
        # Original export had no service field; constant labels ingestion pipeline (not a fabricated user).
        "source_service": "week5-apex-fintech-export",
    }

    return {
        "event_id": ctx["event_id"],
        "event_type": record["event_type"],
        "aggregate_id": ctx["aggregate_id"],
        "aggregate_type": ctx["aggregate_type"],
        "sequence_number": ctx["sequence_number"],
        "payload": payload,
        "metadata": metadata,
        "schema_version": "1.0",
        "occurred_at": ctx["occurred_at"],
        "recorded_at": ctx["recorded_at"],
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

    lines = [ln for ln in SOURCE.read_text(encoding="utf-8").splitlines() if ln.strip()]
    indexed = [(i, json.loads(ln)) for i, ln in enumerate(lines, start=1)]

    # Pass 1: aggregate ids and sort keys
    enriched = []
    for line_no, raw in indexed:
        payload = raw.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        stream_id = raw.get("stream_id", "")
        agg_id = infer_aggregate_uuid(payload, stream_id)
        rec_z = recorded_to_z(raw.get("recorded_at", ""))
        sort_key = (_parse_iso(raw.get("recorded_at", "")) or datetime.min.replace(tzinfo=timezone.utc), line_no)
        enriched.append(
            {
                "line_no": line_no,
                "raw": raw,
                "payload": payload,
                "agg_id": agg_id,
                "rec_z": rec_z,
                "sort_key": sort_key,
            }
        )

    by_agg: dict[str, list] = {}
    for row in enriched:
        by_agg.setdefault(row["agg_id"], []).append(row)

    seq_map: dict[tuple[str, int], int] = {}
    for _aid, rows in by_agg.items():
        rows.sort(key=lambda r: r["sort_key"])
        for seq, r in enumerate(rows, start=1):
            seq_map[(r["agg_id"], r["line_no"])] = seq

    migrated_rows: list[dict] = []
    quarantine_rows: list[dict] = []

    for row in enriched:
        raw = row["raw"]
        line_no = row["line_no"]
        payload = row["payload"]
        stream_id = raw.get("stream_id", "")
        rec_z = row["rec_z"]

        coerced, err = coerce_usd_strings(payload)
        if err:
            q = dict(raw)
            q["reason"] = err
            q["quarantine_source"] = str(SOURCE)
            q["source_line"] = line_no
            quarantine_rows.append(q)
            continue

        event_id = str(
            uuid.uuid5(
                _EVENT_NS,
                f"{line_no}|{raw.get('recorded_at')}|{stream_id}|{raw.get('event_type')}",
            )
        )
        agg_id = row["agg_id"]
        agg_type = infer_aggregate_type(stream_id, coerced)
        seq = seq_map[(agg_id, line_no)]
        corr = correlation_uuid(coerced, stream_id)
        occurred_z = pick_occurred_at(coerced, rec_z)

        tmp = {
            "event_type": raw["event_type"],
            "payload": coerced,
            "_migrate": {
                "event_id": event_id,
                "aggregate_id": agg_id,
                "aggregate_type": agg_type,
                "sequence_number": seq,
                "correlation_id": corr,
                "occurred_at": occurred_z,
                "recorded_at": rec_z,
            },
        }
        out = migrate_record(tmp)
        if out:
            migrated_rows.append(out)

    print(
        f"Week5: lines={len(lines)} migrated={len(migrated_rows)} quarantined={len(quarantine_rows)}"
    )

    if dry_run:
        print("[dry-run] No files written.")
        return

    QUARANTINE.parent.mkdir(parents=True, exist_ok=True)
    _write_report(len(lines))

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
