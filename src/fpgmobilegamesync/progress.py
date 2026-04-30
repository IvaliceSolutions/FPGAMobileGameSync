"""Small terminal progress reporter used by apply operations."""

from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Iterator


class ProgressReporter:
    def __init__(self, enabled: bool, stream: IO[str] | None = None) -> None:
        self.enabled = enabled
        self.stream = stream or sys.stderr

    @classmethod
    def from_mode(cls, mode: str, stream: IO[str] | None = None) -> "ProgressReporter":
        target_stream = stream or sys.stderr
        enabled = mode == "always" or (mode == "auto" and target_stream.isatty())
        return cls(enabled=enabled, stream=target_stream)

    @contextmanager
    def task(self, label: str, total: int | None) -> Iterator["ProgressTask"]:
        task = ProgressTask(self, label, total)
        task.start()
        try:
            yield task
        except Exception:
            task.finish("failed")
            raise
        else:
            task.finish("done")


class ProgressTask:
    def __init__(self, reporter: ProgressReporter, label: str, total: int | None) -> None:
        self.reporter = reporter
        self.label = _compact_label(label)
        self.total = int(total) if total is not None else None
        self.current = 0
        self.started_at = time.monotonic()
        self.last_draw = 0.0
        self.finished = False

    def start(self) -> None:
        self._draw(force=True)

    def update(self, amount: int) -> None:
        if amount <= 0:
            return
        self.current += amount
        self._draw()

    def finish(self, status: str = "done") -> None:
        if self.finished:
            return
        if status == "done" and self.total is not None:
            self.current = max(self.current, self.total)
        self.finished = True
        self._draw(force=True, status=status)
        if self.reporter.enabled:
            self.reporter.stream.write("\n")
            self.reporter.stream.flush()

    def _draw(self, force: bool = False, status: str | None = None) -> None:
        if not self.reporter.enabled:
            return
        now = time.monotonic()
        if not force and now - self.last_draw < 0.1:
            return
        self.last_draw = now
        elapsed = max(now - self.started_at, 0.001)
        rate = self.current / elapsed
        if self.total and self.total > 0:
            ratio = min(self.current / self.total, 1.0)
            filled = int(ratio * 24)
            bar = "#" * filled + "-" * (24 - filled)
            percent = f"{ratio * 100:5.1f}%"
            size = f"{_format_bytes(self.current)}/{_format_bytes(self.total)}"
        else:
            bar = "#" * 24
            percent = "  n/a"
            size = _format_bytes(self.current)
        suffix = status or f"{_format_bytes(rate)}/s"
        self.reporter.stream.write(
            f"\r{self.label:<46} [{bar}] {percent} {size:>19} {suffix:>12}"
        )
        self.reporter.stream.flush()


def copy_file_with_progress(
    source: Path,
    target: Path,
    progress: ProgressReporter | None,
    label: str,
    chunk_size: int = 1024 * 1024,
) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    reporter = progress or ProgressReporter(False)
    total = source.stat().st_size
    with reporter.task(label, total) as task:
        with source.open("rb") as source_handle, target.open("wb") as target_handle:
            for chunk in iter(lambda: source_handle.read(chunk_size), b""):
                target_handle.write(chunk)
                task.update(len(chunk))


def _compact_label(label: str) -> str:
    label = label.replace("\n", " ")
    if len(label) <= 46:
        return label
    name = Path(label).name
    if 0 < len(name) <= 43:
        return f"...{name}"
    return f"{label[:20]}...{label[-23:]}"


def _format_bytes(value: float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
