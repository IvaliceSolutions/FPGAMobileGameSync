from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from fpgmobilegamesync.script_generator import (
    ScriptGenerationError,
    generate_profile_scripts,
)


class ScriptGeneratorTests(unittest.TestCase):
    def test_generate_profile_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output_dir = root / "launchers"

            result = generate_profile_scripts(
                config=_config(),
                output_dir=output_dir,
                profiles=["thor-pull"],
                project_root=root,
                config_path=Path("mister-thor-sync.json"),
                python_bin="python3",
                apply=True,
                pretty=True,
            )

            script_path = output_dir / "fpgms-thor-pull.sh"
            content = script_path.read_text(encoding="utf-8")
            mode = script_path.stat().st_mode
            self.assertEqual(result["summary"]["script_count"], 1)
            self.assertTrue(mode & stat.S_IXUSR)
            self.assertIn("--profile thor-pull", content)
            self.assertIn("--apply", content)
            self.assertIn("--pretty", content)
            self.assertIn('"$@"', content)
            subprocess.run(["sh", "-n", str(script_path)], check=True)

    def test_generate_refuses_unknown_profile(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ScriptGenerationError):
                generate_profile_scripts(
                    config=_config(),
                    output_dir=Path(tmp),
                    profiles=["missing"],
                )

    def test_cli_scripts_generate_writes_selected_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            output_dir = root / "scripts"
            config_path.write_text(json.dumps(_config()), encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "--config",
                    str(config_path),
                    "scripts",
                    "generate",
                    "--output-dir",
                    str(output_dir),
                    "--profile",
                    "thor-pull",
                    "--project-root",
                    str(root),
                    "--pretty",
                ],
                check=True,
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )

            result = json.loads(completed.stdout)
            script_path = output_dir / "fpgms-thor-pull.sh"
            self.assertEqual(result["summary"]["script_count"], 1)
            self.assertTrue(script_path.exists())
            self.assertTrue(os.access(script_path, os.X_OK))


def _config() -> dict:
    return {
        "sync_profiles": {
            "thor-pull": {
                "direction": "mister-to-thor",
                "backend": "s3",
                "source_backend": "sftp",
                "target_backend": "local",
            },
            "thor-push": {
                "direction": "thor-to-mister",
                "backend": "s3",
                "source_backend": "local",
                "target_backend": "sftp",
            },
        }
    }


if __name__ == "__main__":
    unittest.main()
