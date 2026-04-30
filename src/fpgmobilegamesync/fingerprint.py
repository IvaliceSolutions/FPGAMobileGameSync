"""Fingerprint helpers for sync manifest items."""

from __future__ import annotations

from pathlib import Path
from typing import Any

SHA256_FINGERPRINT = "sha256"
SIZE_FINGERPRINT = "size"


def uses_size_fingerprint(system: str, content_type: str) -> bool:
    """Return whether the content can be compared by logical name and size."""
    return content_type == "games"


def size_fingerprint(content_path: str, size: int) -> str:
    folded_path = "/".join(part.casefold() for part in Path(content_path).parts)
    return f"size:{folded_path}:{int(size)}"


def item_uses_size_fingerprint(item: dict[str, Any]) -> bool:
    return (
        item.get("fingerprint_type") == SIZE_FINGERPRINT
        or item.get("type") == "games"
    )
