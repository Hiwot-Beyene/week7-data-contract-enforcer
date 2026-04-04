"""
Shared schema snapshot I/O and normalization for ContractGenerator + SchemaEvolutionAnalyzer.

Operator-facing taxonomy notes (Kafka / dbt / Schema Registry) live in
``schema_analyzer.TAXONOMY_OPERATOR_GUIDE`` and are copied into evolution JSON reports.

Temporal snapshots live under ``schema_snapshots/{contract_id}/{UTC}.yaml`` (see
``write_contract_schema_snapshot``). Diff two files with::

    python contracts/schema_analyzer.py --contract-id <id> \\
        --snapshot-old <older.yaml> --snapshot-new <newer.yaml> -o <report.json>
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# Snapshot paths (repo root relative)
# ---------------------------------------------------------------------------


def snapshot_dir(repo_root: Path, contract_id: str) -> Path:
    return repo_root / "schema_snapshots" / contract_id


def write_contract_schema_snapshot(
    repo_root: Path,
    contract_id: str,
    schema: dict,
    *,
    registry_subscribers: Optional[List[str]] = None,
) -> Path:
    """
    Persist inferred contract schema for evolution diffs.
    Path: schema_snapshots/{contract_id}/{timestamp}.yaml
    """
    now = datetime.now(timezone.utc)
    ts_file = now.strftime("%Y%m%dT%H%M%SZ")
    ts_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    out = snapshot_dir(repo_root, contract_id) / f"{ts_file}.yaml"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {
        "contract_id": contract_id,
        "snapshot_timestamp": ts_iso,
        "snapshot_filename_ts": ts_file,
        "schema": schema,
    }
    if registry_subscribers is not None:
        payload["registry_subscribers"] = list(registry_subscribers)
    with out.open("w", encoding="utf-8") as f:
        yaml.dump(payload, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=100)
    return out


def load_snapshot_file(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}


def list_snapshot_files(repo_root: Path, contract_id: str) -> List[Path]:
    d = snapshot_dir(repo_root, contract_id)
    if not d.is_dir():
        return []
    files = sorted(d.glob("*.yaml"))
    # Exclude non-snapshot clutter if any
    return [p for p in files if p.name != ".gitkeep"]


def parse_since_arg(since: str) -> datetime:
    """
    Accept '7 days ago', '14 days ago', or ISO8601 date/datetime (UTC).
    """
    s = since.strip().lower()
    m = re.match(r"^(\d+)\s+days?\s+ago$", s)
    if m:
        days = int(m.group(1))
        return datetime.now(timezone.utc) - timedelta(days=days)
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.fromisoformat(s + "T00:00:00+00:00")
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        raise ValueError(f"Unrecognized --since value: {since!r}") from e


def snapshot_file_datetime(path: Path, payload: dict) -> datetime:
    ts = payload.get("snapshot_timestamp") or payload.get("generated_at")
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            pass
    # Filename YYYYmmddTHHMMSSZ
    stem = path.stem
    try:
        return datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


# ---------------------------------------------------------------------------
# Normalize Bitol-style schema to flat field records
# ---------------------------------------------------------------------------


@dataclass
class FieldRecord:
    path: str
    type: Optional[str] = None
    required: Optional[bool] = None
    format: Optional[str] = None
    pattern: Optional[str] = None
    enum: Optional[Tuple[str, ...]] = None
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    unique: Optional[bool] = None
    description: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def fingerprint(self) -> Tuple[Any, ...]:
        return (
            self.type,
            self.required,
            self.format,
            self.pattern,
            self.enum,
            self.minimum,
            self.maximum,
            self.unique,
        )


def _normalize_array_items(items: Any) -> dict:
    if not isinstance(items, dict):
        return {}
    if "properties" in items:
        return items
    if items.get("type") == "object" and isinstance(items.get("properties"), dict):
        return items
    props = {
        k: v
        for k, v in items.items()
        if isinstance(v, dict) and ("type" in v or "properties" in v or "enum" in v)
    }
    if props:
        return {"type": "object", "properties": props}
    return items if "type" in items else {"type": "object", "properties": props}


def _record_from_node(path: str, node: dict) -> FieldRecord:
    en = node.get("enum")
    enum_t: Optional[Tuple[str, ...]] = None
    if isinstance(en, list):
        enum_t = tuple(str(x) for x in en)
    elif isinstance(en, str):
        enum_t = (en,)
    mn, mx = node.get("minimum"), node.get("maximum")
    try:
        min_v = float(mn) if mn is not None else None
    except (TypeError, ValueError):
        min_v = None
    try:
        max_v = float(mx) if mx is not None else None
    except (TypeError, ValueError):
        max_v = None
    return FieldRecord(
        path=path,
        type=node.get("type"),
        required=node.get("required"),
        format=node.get("format"),
        pattern=node.get("pattern"),
        enum=enum_t,
        minimum=min_v,
        maximum=max_v,
        unique=node.get("unique"),
        description=node.get("description") if isinstance(node.get("description"), str) else None,
        extra={k: v for k, v in node.items() if k not in _CORE_KEYS},
    )


_CORE_KEYS = frozenset(
    {
        "type",
        "required",
        "format",
        "pattern",
        "enum",
        "minimum",
        "maximum",
        "unique",
        "description",
        "items",
        "properties",
    }
)


def iter_schema_fields(prefix: str, node: Any) -> Iterator[FieldRecord]:
    if not isinstance(node, dict):
        return
    if "type" not in node and "properties" not in node and "items" not in node:
        return

    typ = node.get("type")

    if typ == "array":
        yield _record_from_node(prefix, node)
        items = _normalize_array_items(node.get("items"))
        if isinstance(items, dict):
            if items.get("type") == "object" and isinstance(items.get("properties"), dict):
                for sub, subn in items["properties"].items():
                    if isinstance(subn, dict):
                        yield from iter_schema_fields(f"{prefix}[].{sub}", subn)
            elif "properties" not in items and "type" in items:
                yield from iter_schema_fields(f"{prefix}[]", items)
        return

    if typ == "object" or "properties" in node:
        yield _record_from_node(prefix, node)
        props = node.get("properties") or {}
        if isinstance(props, dict):
            for sub, subn in props.items():
                if isinstance(subn, dict):
                    yield from iter_schema_fields(f"{prefix}.{sub}" if prefix else sub, subn)
        return

    # Top-level leaf or inline object without explicit type:object
    if typ or node.get("format") or node.get("pattern") or "enum" in node:
        yield _record_from_node(prefix, node)


def flatten_contract_schema(schema: dict) -> Dict[str, FieldRecord]:
    out: Dict[str, FieldRecord] = {}
    if not isinstance(schema, dict):
        return out
    for name, sub in schema.items():
        if not isinstance(sub, dict):
            continue
        for rec in iter_schema_fields(name, sub):
            out[rec.path] = rec
    return out


def field_to_human_line(path: str, rec: FieldRecord) -> str:
    parts = [
        path,
        f"type={rec.type!r}",
    ]
    if rec.required is not None:
        parts.append(f"required={rec.required}")
    if rec.format:
        parts.append(f"format={rec.format!r}")
    if rec.pattern:
        parts.append(f"pattern={rec.pattern!r}")
    if rec.enum is not None:
        parts.append(f"enum={list(rec.enum)}")
    if rec.minimum is not None or rec.maximum is not None:
        parts.append(f"range=[{rec.minimum},{rec.maximum}]")
    if rec.unique:
        parts.append("unique=true")
    return " | ".join(parts)


# ---------------------------------------------------------------------------
# Lineage blast radius (no pandas)
# ---------------------------------------------------------------------------


def load_jsonl_last(path: Path) -> Optional[dict]:
    if not path.is_file():
        return None
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return None
    return json.loads(lines[-1])


def blast_radius_from_lineage(repo_root: Path) -> Dict[str, Any]:
    path = repo_root / "outputs/migrate/migrated_lineage_snapshots.jsonl"
    snap = load_jsonl_last(path)
    if not snap:
        return {
            "status": "no_lineage_snapshot",
            "affected_nodes": [],
            "affected_pipelines": [],
            "consumes_edges_sample": [],
        }
    edges = snap.get("edges") or []
    nodes = snap.get("nodes") or []
    node_ids = {n.get("node_id") for n in nodes if isinstance(n, dict)}
    pipelines: List[str] = []
    consumes: List[dict] = []
    for e in edges:
        if not isinstance(e, dict):
            continue
        if e.get("relationship") != "CONSUMES":
            continue
        src, tgt = e.get("source"), e.get("target")
        if not src or not tgt:
            continue
        consumes.append({"source": str(src), "target": str(tgt)})
        if str(tgt).startswith("pipeline::"):
            pipelines.append(str(tgt))
    return {
        "status": "ok",
        "snapshot_id": snap.get("snapshot_id"),
        "node_count": len(node_ids),
        "edge_count": len(edges),
        "affected_nodes": sorted(node_ids)[:200],
        "affected_pipelines": sorted(set(pipelines))[:80],
        "consumes_edges_sample": consumes[:50],
    }
