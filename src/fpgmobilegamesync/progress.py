"""Small terminal progress reporter used by apply operations."""

from __future__ import annotations

import sys
import time
from contextlib import contextmanager
from pathlib import Path
from shutil import get_terminal_size
from typing import IO, Iterator


class ProgressReporter:
    def __init__(
        self,
        enabled: bool,
        stream: IO[str] | None = None,
        width: int | None = None,
    ) -> None:
        self.enabled = enabled
        self.stream = stream or sys.stderr
        self.width = width

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
        width = _stream_width(self.reporter)
        bar_width = _bar_width(width)
        if self.total and self.total > 0:
            ratio = min(self.current / self.total, 1.0)
            filled = int(ratio * bar_width)
            bar = "#" * filled + "-" * (bar_width - filled)
            percent = f"{ratio * 100:5.1f}%"
            size = f"{_format_bytes(self.current)}/{_format_bytes(self.total)}"
        else:
            bar = "#" * bar_width
            percent = "  n/a"
            size = _format_bytes(self.current)
        suffix = status or f"{_format_bytes(rate)}/s"
        line = _format_line(self.label, bar, percent, size, suffix, width)
        self.reporter.stream.write(f"\r\033[2K{line}")
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


def _format_line(label: str, bar: str, percent: str, size: str, suffix: str, width: int) -> str:
    budget = max(width - 1, 40)
    progress_part = f" [{bar}] {percent}"
    details = ""
    is_final_status = suffix in {"done", "failed"}
    if is_final_status:
        if budget >= 78:
            details += f" {size:>19} {suffix:>8}"
        elif budget >= 58:
            details += f" {suffix:>8}"
    else:
        if budget >= 68:
            details += f" {size:>19}"
        if budget >= 92:
            details += f" {suffix:>12}"
    label_width = max(10, budget - len(progress_part) - len(details))
    fitted_label = _fit_text(label, label_width)
    return f"{fitted_label:<{label_width}}{progress_part}{details}"[:budget]


def _fit_text(value: str, width: int) -> str:
    if len(value) <= width:
        return value
    if width <= 3:
        return value[:width]
    name = Path(value).name
    if len(name) <= width:
        return name
    if width <= 12:
        return f"...{name[-(width - 3):]}"
    return f"{name[: max(1, width // 2 - 2)]}...{name[-(width - width // 2 - 1):]}"


def _stream_width(reporter: ProgressReporter) -> int:
    if reporter.width is not None:
        return max(reporter.width, 40)
    return max(get_terminal_size(fallback=(80, 24)).columns, 40)


def _bar_width(width: int) -> int:
    if width >= 92:
        return 24
    if width >= 68:
        return 18
    return 12


def _format_bytes(value: float) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
