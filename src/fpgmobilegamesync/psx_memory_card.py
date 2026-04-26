"""PlayStation memory card inspection and raw-format conversion."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any


RAW_CARD_SIZE = 128 * 1024
FRAME_SIZE = 128
BLOCK_SIZE = 8 * 1024
SYSTEM_FRAME_COUNT = 16
DIRECTORY_ENTRY_COUNT = 15
RAW_CARD_EXTENSIONS = {".sav", ".srm", ".mcr", ".mcd"}
USED_BLOCK_STATES = {0x51, 0x52, 0x53}


class PsxMemoryCardError(Exception):
    """Raised when a PSX memory card image is invalid or unsupported."""


def inspect_psx_memory_card(path: Path) -> dict[str, Any]:
    data = _read_raw_card(path)
    header_valid = data[:2] == b"MC"
    system_checksums = _system_frame_checksums(data)
    directory_entries = _directory_entries(data)
    used_entries = [entry for entry in directory_entries if entry["is_used"]]

    return {
        "path": str(path),
        "format": "raw_psx_memory_card",
        "size": len(data),
        "raw_card_size": RAW_CARD_SIZE,
        "header_valid": header_valid,
        "system_frame_checksums_valid": all(item["valid"] for item in system_checksums),
        "invalid_system_frames": [
            item["frame"] for item in system_checksums if not item["valid"]
        ],
        "directory_entry_count": DIRECTORY_ENTRY_COUNT,
        "used_entry_count": len(used_entries),
        "used_block_count": sum(entry["block_count"] for entry in used_entries),
        "entries": used_entries,
        "canonical_format": "psx_raw_memory_card_v1",
        "canonical_size": len(data),
        "canonical_sha256": hashlib.sha256(data).hexdigest(),
    }


def canonical_psx_memory_card_bytes(path: Path) -> bytes:
    data = _read_raw_card(path)
    _validate_raw_card(data, path)
    return data


def canonical_psx_memory_card_sha256(path: Path) -> str:
    return hashlib.sha256(canonical_psx_memory_card_bytes(path)).hexdigest()


def convert_psx_memory_card(source_path: Path, output_path: Path) -> dict[str, Any]:
    data = canonical_psx_memory_card_bytes(source_path)
    _require_raw_extension(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as handle:
        handle.write(data)
    shutil.copystat(source_path, output_path, follow_symlinks=True)

    return {
        "input_format": "raw_psx_memory_card",
        "output_format": "raw_psx_memory_card",
        "canonical_format": "psx_raw_memory_card_v1",
        "canonical_sha256": hashlib.sha256(data).hexdigest(),
        "canonical_size": len(data),
        "structural_validation": inspect_psx_memory_card(output_path),
    }


def _read_raw_card(path: Path) -> bytes:
    if not path.exists():
        raise PsxMemoryCardError(f"PSX memory card not found: {path}")
    if not path.is_file():
        raise PsxMemoryCardError(f"PSX memory card is not a file: {path}")
    _require_raw_extension(path)
    data = path.read_bytes()
    if len(data) != RAW_CARD_SIZE:
        raise PsxMemoryCardError(
            f"unsupported PSX memory card size for {path.name}: "
            f"{len(data)}; expected {RAW_CARD_SIZE}"
        )
    return data


def _validate_raw_card(data: bytes, path: Path) -> None:
    if data[:2] != b"MC":
        raise PsxMemoryCardError(f"invalid PSX memory card header for {path.name}")
    invalid = [
        item["frame"] for item in _system_frame_checksums(data) if not item["valid"]
    ]
    if invalid:
        raise PsxMemoryCardError(
            f"invalid PSX memory card checksums for {path.name}: frames {invalid}"
        )


def _require_raw_extension(path: Path) -> None:
    if path.suffix.lower() not in RAW_CARD_EXTENSIONS:
        raise PsxMemoryCardError(
            f"unsupported PSX memory card extension for {path.name}: {path.suffix}"
        )


def _system_frame_checksums(data: bytes) -> list[dict[str, Any]]:
    return [
        {
            "frame": frame_index,
            "expected": _frame_checksum(_frame(data, frame_index)),
            "actual": _frame(data, frame_index)[127],
            "valid": _frame_checksum(_frame(data, frame_index))
            == _frame(data, frame_index)[127],
        }
        for frame_index in range(SYSTEM_FRAME_COUNT)
    ]


def _directory_entries(data: bytes) -> list[dict[str, Any]]:
    entries = []
    for index in range(DIRECTORY_ENTRY_COUNT):
        frame_index = index + 1
        frame = _frame(data, frame_index)
        save_size = int.from_bytes(frame[4:8], byteorder="little")
        block_count = max(1, save_size // BLOCK_SIZE) if save_size else 0
        filename = _decode_ascii(frame[10:30])
        state = frame[0]
        entries.append(
            {
                "slot": index,
                "frame": frame_index,
                "state": f"0x{state:02x}",
                "state_name": _state_name(state),
                "is_used": state in USED_BLOCK_STATES,
                "save_size": save_size,
                "block_count": block_count,
                "filename": filename,
                "checksum_valid": _frame_checksum(frame) == frame[127],
            }
        )
    return entries


def _frame(data: bytes, frame_index: int) -> bytes:
    start = frame_index * FRAME_SIZE
    return data[start : start + FRAME_SIZE]


def _frame_checksum(frame: bytes) -> int:
    checksum = 0
    for value in frame[:127]:
        checksum ^= value
    return checksum


def _decode_ascii(value: bytes) -> str:
    return value.split(b"\x00", 1)[0].decode("ascii", errors="replace")


def _state_name(state: int) -> str:
    return {
        0x51: "used_first_block",
        0x52: "used_middle_block",
        0x53: "used_last_block",
        0xA0: "free",
        0xA1: "deleted_first_block",
        0xA2: "deleted_middle_block",
        0xA3: "deleted_last_block",
    }.get(state, "unknown")
