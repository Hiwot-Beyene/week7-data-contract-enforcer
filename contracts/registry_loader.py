"""
ContractRegistry loader — parse and validate consumer subscriptions.

Safe to import from generator, runner, attributor, and report_generator: this module
imports no other project packages. All filesystem paths are passed as parameters.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import yaml

_ARRAY_BRACKET_RE = re.compile(r"\[\*\]")

_SUB_REQUIRED_KEYS = frozenset(
    {
        "contract_id",
        "subscriber_id",
        "subscriber_team",
        "fields_consumed",
        "breaking_fields",
        "validation_mode",
        "registered_at",
        "contact",
    }
)
_VALID_MODES = frozenset({"AUDIT", "WARN", "ENFORCE"})
_ISO8601_LOOSE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$"
)


def _as_str(value: Any, field: str, index: int) -> str:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"subscriptions[{index}].{field}: required non-empty string")
    if not isinstance(value, str):
        raise ValueError(f"subscriptions[{index}].{field}: must be a string, got {type(value).__name__}")
    return value.strip()


def _normalize_field_token(raw: str) -> str:
    """Align ValidationRunner column_name tokens with registry paths (strip [*], spaces)."""
    s = (raw or "").strip()
    if not s or s == "*":
        return s
    s = _ARRAY_BRACKET_RE.sub("", s)
    return s.strip(".").strip()


def _dot_prefix_match(failing_field: str, breaking_field: str) -> bool:
    """
    True if failing_field equals breaking_field, or either is a dot-prefix of the other
    (e.g. failing_field 'extracted_facts' matches breaking_field 'extracted_facts.confidence').
    """
    if failing_field == breaking_field:
        return True
    if breaking_field.startswith(failing_field + "."):
        return True
    if failing_field.startswith(breaking_field + "."):
        return True
    return False


def _field_matches_breaking(failing_field: str, breaking_field: str) -> bool:
    """
    Match registry breaking_fields to ValidationRunner ``column_name`` values.

    Handles ``extracted_facts[*].confidence`` vs ``extracted_facts.confidence`` and
    composite columns like ``recorded_at,occurred_at`` (either side can match).
    """
    ff = _normalize_field_token(failing_field)
    bf = _normalize_field_token(breaking_field)
    if not ff or ff == "*" or not bf:
        return False
    parts = [p.strip() for p in ff.split(",") if p.strip()]
    if not parts:
        parts = [ff]
    for p in parts:
        pn = _normalize_field_token(p)
        if _dot_prefix_match(pn, bf) or _dot_prefix_match(bf, pn):
            return True
    return False


def load_registry(registry_path: str) -> dict:
    """
    Load subscriptions YAML and validate every subscription entry.

    Raises:
        ValueError: missing required keys, invalid types, or invalid validation_mode.
        FileNotFoundError, yaml.YAMLError: propagated from IO / parse.
    """
    with open(registry_path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if data is None:
        raise ValueError("registry file is empty or contains no YAML document")

    if not isinstance(data, dict):
        raise ValueError("registry root must be a mapping with key 'subscriptions'")

    rv = data.get("registry_version")
    if rv is not None:
        if not isinstance(rv, str) or not rv.strip():
            raise ValueError("registry_version: when present, must be a non-empty string")
        data["registry_version"] = rv.strip()

    subs = data.get("subscriptions")
    if subs is None:
        raise ValueError("missing required key 'subscriptions'")
    if not isinstance(subs, list):
        raise ValueError("'subscriptions' must be a list")

    seen_pairs: set[tuple[str, str]] = set()

    for i, entry in enumerate(subs):
        if not isinstance(entry, dict):
            raise ValueError(f"subscriptions[{i}]: must be a mapping, got {type(entry).__name__}")

        missing = _SUB_REQUIRED_KEYS - entry.keys()
        if missing:
            raise ValueError(
                f"subscriptions[{i}]: missing required field(s): {', '.join(sorted(missing))}"
            )

        contract_id = _as_str(entry.get("contract_id"), "contract_id", i)
        subscriber_id = _as_str(entry.get("subscriber_id"), "subscriber_id", i)
        _as_str(entry.get("subscriber_team"), "subscriber_team", i)
        _as_str(entry.get("contact"), "contact", i)
        reg_at = _as_str(entry.get("registered_at"), "registered_at", i)
        if not _ISO8601_LOOSE.match(reg_at):
            raise ValueError(
                f"subscriptions[{i}].registered_at: must be ISO 8601 timestamp, got {reg_at!r}"
            )

        mode = _as_str(entry.get("validation_mode"), "validation_mode", i).upper()
        if mode not in _VALID_MODES:
            raise ValueError(
                f"subscriptions[{i}].validation_mode: must be one of AUDIT, WARN, ENFORCE, got {mode!r}"
            )
        entry["validation_mode"] = mode

        if "mode_upgraded_at" in entry:
            mua = entry.get("mode_upgraded_at")
            if mua is None or (isinstance(mua, str) and not mua.strip()):
                raise ValueError(
                    f"subscriptions[{i}].mode_upgraded_at: when present, must be a non-empty string"
                )
            mua_s = _as_str(entry.get("mode_upgraded_at"), "mode_upgraded_at", i)
            if not _ISO8601_LOOSE.match(mua_s):
                raise ValueError(
                    f"subscriptions[{i}].mode_upgraded_at: must be ISO 8601 timestamp, got {mua_s!r}"
                )

        fc = entry.get("fields_consumed")
        if not isinstance(fc, list) or not fc:
            raise ValueError(f"subscriptions[{i}].fields_consumed: must be a non-empty list")
        for j, p in enumerate(fc):
            if not isinstance(p, str) or not p.strip():
                raise ValueError(f"subscriptions[{i}].fields_consumed[{j}]: must be a non-empty string")

        bf = entry.get("breaking_fields")
        if not isinstance(bf, list) or not bf:
            raise ValueError(f"subscriptions[{i}].breaking_fields: must be a non-empty list")
        for j, item in enumerate(bf):
            if not isinstance(item, dict):
                raise ValueError(
                    f"subscriptions[{i}].breaking_fields[{j}]: must be a mapping"
                )
            if "field" not in item or "reason" not in item:
                raise ValueError(
                    f"subscriptions[{i}].breaking_fields[{j}]: each entry must have 'field' and 'reason'"
                )
            fld = item.get("field")
            rsn = item.get("reason")
            if not isinstance(fld, str) or not fld.strip():
                raise ValueError(
                    f"subscriptions[{i}].breaking_fields[{j}].field: must be a non-empty string"
                )
            if not isinstance(rsn, str) or not rsn.strip():
                raise ValueError(
                    f"subscriptions[{i}].breaking_fields[{j}].reason: must be a non-empty string"
                )

        pair = (contract_id, subscriber_id)
        if pair in seen_pairs:
            raise ValueError(
                f"subscriptions[{i}]: duplicate (contract_id, subscriber_id) "
                f"({contract_id!r}, {subscriber_id!r})"
            )
        seen_pairs.add(pair)

    return data


def registry_staleness_warnings(registry: dict) -> List[str]:
    """
    Operational warnings for long-lived AUDIT subscriptions (documentation runbook).

    Emits a warning when validation_mode is AUDIT and mode_upgraded_at is older than 30 days,
    or when mode_upgraded_at is missing for an AUDIT row (cannot prove freshness).
    """
    warnings: List[str] = []
    subs = registry.get("subscriptions")
    if not isinstance(subs, list):
        return warnings

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)

    for i, entry in enumerate(subs):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("validation_mode") or "").upper() != "AUDIT":
            continue
        sid = str(entry.get("subscriber_id") or "?")
        mua = entry.get("mode_upgraded_at")
        if not mua or not isinstance(mua, str) or not mua.strip():
            warnings.append(
                f"subscriptions[{i}] ({sid}): AUDIT mode without mode_upgraded_at; "
                "set mode_upgraded_at (ISO 8601) when entering AUDIT for staleness tracking."
            )
            continue
        mua_s = mua.strip()
        if not _ISO8601_LOOSE.match(mua_s):
            continue
        try:
            ts = mua_s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            if dt < cutoff:
                warnings.append(
                    f"subscriptions[{i}] ({sid}): AUDIT for more than 30 days "
                    f"(mode_upgraded_at={mua_s}); review promotion to WARN/ENFORCE per runbook."
                )
        except ValueError:
            pass

    return warnings


def query_blast_radius(registry: dict, contract_id: str, failing_field: str) -> List[Dict[str, Any]]:
    """
    Return subscribers whose breaking_fields match failing_field via dot-prefix rules.

    Each dict: subscriber_id, contact, validation_mode, reason (from first matching break).
    Never raises; returns [] if no matches.
    """
    if not failing_field or not isinstance(failing_field, str):
        return []

    subs = registry.get("subscriptions") if isinstance(registry, dict) else None
    if not isinstance(subs, list):
        return []

    out: List[Dict[str, Any]] = []

    for entry in subs:
        if not isinstance(entry, dict):
            continue
        if entry.get("contract_id") != contract_id:
            continue
        bfs = entry.get("breaking_fields")
        if not isinstance(bfs, list):
            continue
        reason: Optional[str] = None
        ff_raw = (failing_field or "").strip()
        volume_style = ff_raw in ("*", "") or ff_raw == "*"
        if volume_style:
            first = bfs[0] if bfs and isinstance(bfs[0], dict) else None
            if isinstance(first, dict):
                reason = str(first.get("reason", "")).strip()
        else:
            for item in bfs:
                if not isinstance(item, dict):
                    continue
                field_path = item.get("field")
                if not isinstance(field_path, str):
                    continue
                if _field_matches_breaking(failing_field, field_path):
                    reason = str(item.get("reason", "")).strip()
                    break
        if reason is None:
            continue
        out.append(
            {
                "subscriber_id": entry.get("subscriber_id", ""),
                "contact": entry.get("contact", ""),
                "validation_mode": entry.get("validation_mode", ""),
                "reason": reason,
            }
        )

    return out


def validate_registry(registry_path: str) -> List[str]:
    """
    Validate subscriptions file; return human-readable errors (empty = valid).
    Does not raise on structural problems — collects them into the list.
    """
    errors: List[str] = []

    try:
        with open(registry_path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except OSError as e:
        return [f"cannot read registry: {e}"]
    except yaml.YAMLError as e:
        return [f"YAML parse error: {e}"]

    if data is None:
        errors.append("registry file is empty or contains no YAML document")
        return errors

    if not isinstance(data, dict):
        errors.append("root must be a mapping with key 'subscriptions'")
        return errors

    rv = data.get("registry_version")
    if rv is not None and (not isinstance(rv, str) or not str(rv).strip()):
        errors.append("registry_version: when present, must be a non-empty string")

    subs = data.get("subscriptions")
    if subs is None:
        errors.append("missing required key 'subscriptions'")
        return errors
    if not isinstance(subs, list):
        errors.append("'subscriptions' must be a list")
        return errors

    seen_pairs: set[tuple[str, str]] = set()

    for i, entry in enumerate(subs):
        prefix = f"subscriptions[{i}]"
        if not isinstance(entry, dict):
            errors.append(f"{prefix}: must be a mapping")
            continue

        for key in ("contract_id", "subscriber_id", "breaking_fields"):
            if key not in entry:
                errors.append(f"{prefix}: missing required field {key!r}")

        cid = entry.get("contract_id")
        sid = entry.get("subscriber_id")
        if isinstance(cid, str) and isinstance(sid, str) and cid.strip() and sid.strip():
            pair = (cid.strip(), sid.strip())
            if pair in seen_pairs:
                errors.append(
                    f"{prefix}: duplicate (contract_id, subscriber_id) {pair!r}"
                )
            seen_pairs.add(pair)

        bf = entry.get("breaking_fields")
        if bf is not None:
            if not isinstance(bf, list):
                errors.append(f"{prefix}.breaking_fields: must be a list")
            elif len(bf) == 0:
                errors.append(f"{prefix}.breaking_fields: must be a non-empty list")
            else:
                for j, item in enumerate(bf):
                    if not isinstance(item, dict):
                        errors.append(f"{prefix}.breaking_fields[{j}]: must be a mapping")
                        continue
                    if "field" not in item or "reason" not in item:
                        errors.append(
                            f"{prefix}.breaking_fields[{j}]: must include 'field' and 'reason'"
                        )

        mua = entry.get("mode_upgraded_at")
        if mua is not None:
            if not isinstance(mua, str) or not mua.strip():
                errors.append(f"{prefix}.mode_upgraded_at: must be a non-empty string when present")
            elif not _ISO8601_LOOSE.match(mua.strip()):
                errors.append(
                    f"{prefix}.mode_upgraded_at: must be ISO 8601 timestamp, got {mua.strip()!r}"
                )

    return errors
