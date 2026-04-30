from __future__ import annotations

import io
import unittest

from fpgmobilegamesync.progress import ProgressReporter


class ProgressReporterTests(unittest.TestCase):
    def test_progress_line_fits_narrow_terminal(self) -> None:
        stream = io.StringIO()
        reporter = ProgressReporter(enabled=True, stream=stream, width=60)

        with reporter.task("download Front Mission 3 (USA).bin", 1000) as task:
            task.update(898)

        lines = [
            chunk.replace("\033[2K", "")
            for chunk in stream.getvalue().split("\r")
            if chunk.strip()
        ]

        self.assertTrue(lines)
        self.assertTrue(all(len(line.rstrip("\n")) <= 59 for line in lines))
        self.assertIn("done", lines[-1])

    def test_progress_line_keeps_size_on_wide_terminal(self) -> None:
        stream = io.StringIO()
        reporter = ProgressReporter(enabled=True, stream=stream, width=100)

        with reporter.task("download Front Mission 3 (USA).bin", 1024) as task:
            task.update(1024)

        output = stream.getvalue()

        self.assertIn("1.0 KiB/1.0 KiB", output)
        self.assertIn("done", output)


if __name__ == "__main__":
    unittest.main()
