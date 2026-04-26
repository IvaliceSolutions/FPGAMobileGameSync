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
    generate_env_template,
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

    def test_generate_env_template_lists_configured_env_vars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "fpgms.env"

            result = generate_env_template(
                config=_config(),
                output_path=output_path,
            )

            content = output_path.read_text(encoding="utf-8")
            self.assertEqual(result["summary"]["env_count"], 9)
            self.assertIn('export FPGMS_PROJECT_ROOT=""', content)
            self.assertIn('export MISTER_THOR_S3_ENDPOINT_URL=""', content)
            self.assertIn('export THOR_SYNC_SFTP_PRIVATE_KEY=""', content)
            self.assertNotIn("secret-value", content)

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

    def test_cli_scripts_env_template_writes_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_path = root / "config.json"
            output_path = root / "fpgms.env"
            config_path.write_text(json.dumps(_config()), encoding="utf-8")

            completed = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "fpgmobilegamesync.cli",
                    "--config",
                    str(config_path),
                    "scripts",
                    "env-template",
                    "--output",
                    str(output_path),
                    "--pretty",
                ],
                check=True,
                cwd=Path.cwd(),
                text=True,
                capture_output=True,
            )

            result = json.loads(completed.stdout)
            self.assertEqual(result["summary"]["env_count"], 9)
            self.assertTrue(output_path.exists())


def _config() -> dict:
    return {
        "s3": {
            "endpoint_url_env": "MISTER_THOR_S3_ENDPOINT_URL",
            "access_key_id_env": "MISTER_THOR_S3_ACCESS_KEY_ID",
            "secret_access_key_env": "MISTER_THOR_S3_SECRET_ACCESS_KEY",
        },
        "devices": {
            "mister": {
                "remote": {
                    "username_env": "MISTER_SYNC_SFTP_USER",
                    "password_env": "MISTER_SYNC_SFTP_PASSWORD",
                    "private_key_env": "MISTER_SYNC_SFTP_PRIVATE_KEY",
                }
            },
            "thor": {
                "remote": {
                    "username_env": "THOR_SYNC_SFTP_USER",
                    "password_env": "THOR_SYNC_SFTP_PASSWORD",
                    "private_key_env": "THOR_SYNC_SFTP_PRIVATE_KEY",
                }
            },
        },
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
