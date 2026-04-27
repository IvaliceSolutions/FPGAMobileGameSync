from __future__ import annotations

import unittest

from fpgmobilegamesync.sftp_client import SftpError, _auth_options, _timeout_options


class SftpClientTests(unittest.TestCase):
    def test_auth_options_disable_agent_when_password_is_explicit(self) -> None:
        self.assertEqual(
            _auth_options({}, password="secret", key_filename=None),
            {"look_for_keys": False, "allow_agent": False},
        )

    def test_auth_options_keep_agent_when_no_explicit_auth_is_set(self) -> None:
        self.assertEqual(
            _auth_options({}, password=None, key_filename=None),
            {"look_for_keys": True, "allow_agent": True},
        )

    def test_auth_options_can_be_overridden_by_config(self) -> None:
        self.assertEqual(
            _auth_options(
                {"look_for_keys": True, "allow_agent": True},
                password="secret",
                key_filename=None,
            ),
            {"look_for_keys": True, "allow_agent": True},
        )

    def test_timeout_options_default_to_ten_seconds(self) -> None:
        self.assertEqual(
            _timeout_options({}),
            {
                "timeout": 10.0,
                "banner_timeout": 10.0,
                "auth_timeout": 10.0,
            },
        )

    def test_timeout_options_can_be_overridden(self) -> None:
        self.assertEqual(
            _timeout_options(
                {
                    "timeout_seconds": 3,
                    "banner_timeout_seconds": 4,
                    "auth_timeout_seconds": 5,
                }
            ),
            {
                "timeout": 3.0,
                "banner_timeout": 4.0,
                "auth_timeout": 5.0,
            },
        )

    def test_timeout_options_reject_invalid_values(self) -> None:
        with self.assertRaises(SftpError):
            _timeout_options({"timeout_seconds": 0})


if __name__ == "__main__":
    unittest.main()
