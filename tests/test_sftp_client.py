from __future__ import annotations

import unittest

from fpgmobilegamesync.sftp_client import _auth_options


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


if __name__ == "__main__":
    unittest.main()
