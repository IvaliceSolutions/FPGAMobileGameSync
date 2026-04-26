from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.psx_memory_card import (
    RAW_CARD_SIZE,
    PsxMemoryCardError,
    canonical_psx_memory_card_sha256,
    convert_psx_memory_card,
    inspect_psx_memory_card,
)


class PsxMemoryCardTests(unittest.TestCase):
    def test_inspects_raw_memory_card_structure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            card = Path(tmp) / "Game.sav"
            card.write_bytes(_raw_card())

            result = inspect_psx_memory_card(card)

            self.assertEqual(result["format"], "raw_psx_memory_card")
            self.assertEqual(result["size"], RAW_CARD_SIZE)
            self.assertTrue(result["header_valid"])
            self.assertTrue(result["system_frame_checksums_valid"])
            self.assertEqual(result["used_entry_count"], 1)
            self.assertEqual(result["used_block_count"], 1)
            self.assertEqual(result["entries"][0]["filename"], "BASCUS-00000SAVE")

    def test_rejects_bad_header(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            card = Path(tmp) / "Bad.sav"
            data = bytearray(_raw_card())
            data[0:2] = b"NO"
            card.write_bytes(data)

            with self.assertRaises(PsxMemoryCardError):
                canonical_psx_memory_card_sha256(card)

    def test_rejects_bad_system_frame_checksum(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            card = Path(tmp) / "Bad.sav"
            data = bytearray(_raw_card())
            data[128 + 127] ^= 0xFF
            card.write_bytes(data)

            with self.assertRaises(PsxMemoryCardError):
                canonical_psx_memory_card_sha256(card)

    def test_converts_raw_card_to_srm_with_structural_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "Game.sav"
            output = Path(tmp) / "Game.srm"
            source.write_bytes(_raw_card())

            result = convert_psx_memory_card(source, output)

            self.assertEqual(output.read_bytes(), source.read_bytes())
            self.assertEqual(result["input_format"], "raw_psx_memory_card")
            self.assertEqual(result["output_format"], "raw_psx_memory_card")
            self.assertEqual(result["canonical_size"], RAW_CARD_SIZE)
            self.assertTrue(result["structural_validation"]["header_valid"])


def _raw_card() -> bytes:
    data = bytearray(RAW_CARD_SIZE)
    data[0:2] = b"MC"
    for entry in range(15):
        offset = (entry + 1) * 128
        data[offset] = 0xA0
    first = 128
    data[first] = 0x51
    data[first + 4 : first + 8] = (8192).to_bytes(4, byteorder="little")
    data[first + 10 : first + 26] = b"BASCUS-00000SAVE"
    for frame_index in range(16):
        offset = frame_index * 128
        data[offset + 127] = _checksum(data[offset : offset + 128])
    return bytes(data)


def _checksum(frame: bytes | bytearray) -> int:
    value = 0
    for byte in frame[:127]:
        value ^= byte
    return value


if __name__ == "__main__":
    unittest.main()
