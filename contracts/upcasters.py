"""
Read-time schema upcasting for stored events (not one-shot JSONL migration).

CreditAnalysisCompleted v2 rows may leave denormalized payload fields null while
values exist under payload[\"decision\"]. Register an upcaster in UpcasterRegistry
to populate canonical views at read time without fabricating data on disk.

See: outputs/migrate/deviation_report_week5.json (upcaster_notes).
"""

from typing import Any, Callable, Dict, Optional, Tuple


class UpcasterRegistry:
    """Placeholder: map (event_type, schema_version) → upcallables."""

    def __init__(self) -> None:
        self._by_key: Dict[Tuple[str, str], Callable[..., Any]] = {}

    def register(self, event_type: str, schema_version: str, fn: Callable[..., Any]) -> None:
        self._by_key[(event_type, schema_version)] = fn

    def get(self, event_type: str, schema_version: str) -> Optional[Callable[..., Any]]:
        return self._by_key.get((event_type, schema_version))
