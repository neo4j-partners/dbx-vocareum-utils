"""Shared console output helpers.

Every tool prints a transcript that someone reads later, often pasted into a
ticket, so the section headers are the same width and shape in all three.
"""

from __future__ import annotations

import json
from typing import Any

SECTION_WIDTH = 78


def section(title: str) -> None:
    """Print a visually distinct section header."""
    print()
    print("=" * SECTION_WIDTH)
    print(title)
    print("=" * SECTION_WIDTH)


def rule() -> None:
    """Print a horizontal rule the width of a section header."""
    print("-" * SECTION_WIDTH)


def dump_json(payload: Any) -> str:
    """Serialize a payload for printing, tolerating non-JSON types."""
    return json.dumps(payload, indent=2, sort_keys=True, default=str)
