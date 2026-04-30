"""PlayStation memory card inspection and raw-format conversion."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any


RAW_CARD_SIZE = 128 * 1024
CANONICAL_FORMAT = "psx_raw_memory_card_v2"
EMPTY_CANONICAL_SHA256 = "0a36c94a2a96926ecd1855ab7de34841fe446d18ae0bb8b993f340a3cde02058"
EMPTY_RAW_SHA256 = "7706c7d43edaf8cb7618e574f03457105153e3bdc196db803a600ad96a8f58e8"
EMPTY_CARD_SHA256ES = frozenset({EMPTY_CANONICAL_SHA256, EMPTY_RAW_SHA256})
FRAME_SIZE = 128
BLOCK_SIZE = 8 * 1024
SYSTEM_FRAME_COUNT = 16
DIRECTORY_ENTRY_COUNT = 15
RAW_CARD_EXTENSIONS = {".sav", ".srm", ".mcr", ".mcd"}
USED_BLOCK_STATES = {0x51, 0x52, 0x53}
FIRST_BLOCK_STATE = 0x51
MIDDLE_BLOCK_STATE = 0x52
LAST_BLOCK_STATE = 0x53
FREE_BLOCK_STATE = 0xA0
UNUSABLE_BLOCK_STATE = 0xFF
NO_NEXT_BLOCK = 0xFFFF


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
        "canonical_format": CANONICAL_FORMAT,
        "canonical_size": RAW_CARD_SIZE,
        "canonical_sha256": hashlib.sha256(_normalized_raw_card(data, path)).hexdigest(),
    }


def canonical_psx_memory_card_bytes(path: Path) -> bytes:
    data = _read_raw_card(path)
    _validate_raw_card(data, path)
    return _normalized_raw_card(data, path)


def canonical_psx_memory_card_bytes_from_data(data: bytes, name: str) -> bytes:
    path = Path(name)
    _require_raw_extension(path)
    if len(data) != RAW_CARD_SIZE:
        raise PsxMemoryCardError(
            f"unsupported PSX memory card size for {path.name}: "
            f"{len(data)}; expected {RAW_CARD_SIZE}"
        )
    _validate_raw_card(data, path)
    return _normalized_raw_card(data, path)


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
        "canonical_format": CANONICAL_FORMAT,
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
    _save_files(data, path)


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


def _normalized_raw_card(data: bytes, path: Path) -> bytes:
    save_files = _save_files(data, path)
    return _build_raw_card(save_files)


def _save_files(data: bytes, path: Path) -> list[dict[str, Any]]:
    save_files = []
    visited_blocks: set[int] = set()

    for block_index in range(DIRECTORY_ENTRY_COUNT):
        directory_frame = _frame(data, block_index + 1)
        state = directory_frame[0]
        if state != FIRST_BLOCK_STATE:
            continue

        block_numbers = _save_block_chain(data, path, block_index)
        for block_number in block_numbers:
            if block_number in visited_blocks:
                raise PsxMemoryCardError(
                    f"PSX memory card has overlapping save blocks in {path.name}: "
                    f"block {block_number}"
                )
            visited_blocks.add(block_number)

        raw_data = b"".join(_data_block(data, block_number) for block_number in block_numbers)
        save_size = int.from_bytes(directory_frame[4:8], byteorder="little")
        if save_size != len(raw_data):
            raise PsxMemoryCardError(
                f"PSX memory card save size mismatch in {path.name}: "
                f"{save_size} != {len(raw_data)}"
            )
        if raw_data[:2] != b"SC":
            raise PsxMemoryCardError(
                f"PSX memory card save block is missing SC header in {path.name}: "
                f"block {block_index}"
            )

        save_files.append(
            {
                "filename_bytes": _trim_null_bytes(directory_frame[10:30]),
                "raw_data": raw_data,
            }
        )

    return save_files


def _save_block_chain(data: bytes, path: Path, first_block: int) -> list[int]:
    block_numbers = []
    seen: set[int] = set()
    current = first_block

    while current != NO_NEXT_BLOCK:
        if current < 0 or current >= DIRECTORY_ENTRY_COUNT:
            raise PsxMemoryCardError(
                f"PSX memory card has invalid next block in {path.name}: {current}"
            )
        if current in seen:
            raise PsxMemoryCardError(
                f"PSX memory card has cyclic save block chain in {path.name}: {current}"
            )
        seen.add(current)
        block_numbers.append(current)

        directory_frame = _frame(data, current + 1)
        state = directory_frame[0]
        if current == first_block and state != FIRST_BLOCK_STATE:
            raise PsxMemoryCardError(
                f"PSX memory card save chain does not start with a first block in {path.name}"
            )
        if current != first_block and state not in {MIDDLE_BLOCK_STATE, LAST_BLOCK_STATE}:
            raise PsxMemoryCardError(
                f"PSX memory card save chain has invalid block state in {path.name}: "
                f"0x{state:02x}"
            )

        current = int.from_bytes(directory_frame[8:10], byteorder="little")

    return block_numbers


def _build_raw_card(save_files: list[dict[str, Any]]) -> bytes:
    directory_frames = [_directory_frame_magic()]
    data_blocks = []

    for save_file in save_files:
        raw_data = save_file["raw_data"]
        block_count = len(raw_data) // BLOCK_SIZE
        first_data_block = len(data_blocks)
        directory_frames.extend(
            _directory_frames_for_save(
                filename_bytes=save_file["filename_bytes"],
                save_size=len(raw_data),
                first_data_block=first_data_block,
                block_count=block_count,
            )
        )
        data_blocks.extend(
            raw_data[offset : offset + BLOCK_SIZE]
            for offset in range(0, len(raw_data), BLOCK_SIZE)
        )

    if len(data_blocks) > DIRECTORY_ENTRY_COUNT:
        raise PsxMemoryCardError(
            f"PSX memory card contains too much save data: {len(data_blocks)} blocks"
        )

    while len(directory_frames) < SYSTEM_FRAME_COUNT:
        directory_frames.append(_directory_frame_empty())
    while len(directory_frames) < BLOCK_SIZE // FRAME_SIZE:
        directory_frames.append(_directory_frame_unusable())
    while len(data_blocks) < DIRECTORY_ENTRY_COUNT:
        data_blocks.append(bytes(BLOCK_SIZE))

    return b"".join(directory_frames) + b"".join(data_blocks)


def _directory_frames_for_save(
    filename_bytes: bytes,
    save_size: int,
    first_data_block: int,
    block_count: int,
) -> list[bytes]:
    frames = []
    for index in range(block_count):
        frame = bytearray(FRAME_SIZE)
        if index == 0:
            frame[0] = FIRST_BLOCK_STATE
            frame[4:8] = save_size.to_bytes(4, byteorder="little")
            frame[10 : 10 + len(filename_bytes)] = filename_bytes[:20]
        elif index == block_count - 1:
            frame[0] = LAST_BLOCK_STATE
        else:
            frame[0] = MIDDLE_BLOCK_STATE

        next_block = (
            NO_NEXT_BLOCK
            if index == block_count - 1
            else first_data_block + index + 1
        )
        frame[8:10] = next_block.to_bytes(2, byteorder="little")
        frame[127] = _frame_checksum(frame)
        frames.append(bytes(frame))
    return frames


def _directory_frame_magic() -> bytes:
    frame = bytearray(FRAME_SIZE)
    frame[0:2] = b"MC"
    frame[127] = _frame_checksum(frame)
    return bytes(frame)


def _directory_frame_empty() -> bytes:
    frame = bytearray(FRAME_SIZE)
    frame[0] = FREE_BLOCK_STATE
    frame[8:10] = NO_NEXT_BLOCK.to_bytes(2, byteorder="little")
    frame[127] = _frame_checksum(frame)
    return bytes(frame)


def _directory_frame_unusable() -> bytes:
    frame = bytearray(FRAME_SIZE)
    frame[0:4] = bytes([UNUSABLE_BLOCK_STATE] * 4)
    frame[8:10] = NO_NEXT_BLOCK.to_bytes(2, byteorder="little")
    return bytes(frame)


def _data_block(data: bytes, block_index: int) -> bytes:
    start = BLOCK_SIZE * (block_index + 1)
    return data[start : start + BLOCK_SIZE]


def _trim_null_bytes(value: bytes) -> bytes:
    return value.split(b"\x00", 1)[0]


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
