#!/usr/bin/env python3
"""Validate contract_registry/subscriptions.yaml (structure + optional CI gate)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from contracts.registry_loader import (  # noqa: E402
    load_registry,
    registry_staleness_warnings,
    validate_registry,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate consumer registry YAML")
    parser.add_argument(
        "path",
        nargs="?",
        default=str(REPO_ROOT / "contract_registry" / "subscriptions.yaml"),
        help="Path to subscriptions.yaml (default: contract_registry/subscriptions.yaml)",
    )
    args = parser.parse_args()
    path = args.path

    errs = validate_registry(path)
    if errs:
        for e in errs:
            print(f"ERROR: {e}", file=sys.stderr)
        return 1

    try:
        reg = load_registry(path)
    except Exception as exc:
        print(f"ERROR: strict load failed: {exc}", file=sys.stderr)
        return 1

    for w in registry_staleness_warnings(reg):
        print(f"WARNING: {w}", file=sys.stderr)

    print(f"OK: {path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
