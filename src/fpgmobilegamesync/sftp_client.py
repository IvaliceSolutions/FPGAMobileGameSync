"""SFTP client helpers."""

from __future__ import annotations

import os
import stat as stat_module
from dataclasses import dataclass
from typing import Any


class SftpError(Exception):
    """Raised when an SFTP operation cannot be completed."""


@dataclass(frozen=True)
class RemoteStat:
    size: int
    modified_ns: int
    is_file: bool
    is_dir: bool


@dataclass(frozen=True)
class RemoteDirEntry:
    name: str
    stat: RemoteStat


class SftpDeviceClient:
    """Small Paramiko-backed SFTP adapter used by remote scanners."""

    def __init__(self, ssh_client: Any, sftp_client: Any, host: str) -> None:
        self.ssh_client = ssh_client
        self.sftp_client = sftp_client
        self.host = host

    @classmethod
    def from_config(cls, config: dict[str, Any], device: str) -> "SftpDeviceClient":
        remote = config.get("devices", {}).get(device, {}).get("remote")
        if not isinstance(remote, dict):
            raise SftpError(f"missing remote configuration for device: {device}")
        if remote.get("protocol") != "sftp":
            raise SftpError(f"unsupported remote protocol for {device}: {remote.get('protocol')}")

        try:
            import paramiko  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:
            raise SftpError(
                "SFTP support requires Paramiko. From the repo, install with: pip install '.[sftp]'"
            ) from exc

        host = _required_string(remote, "host", device)
        username = _value_or_env(remote, "username", "username_env")
        if not username:
            raise SftpError(f"missing SFTP username for device: {device}")
        password = _value_or_env(remote, "password", "password_env")
        key_filename = _value_or_env(remote, "private_key", "private_key_env")
        port = remote.get("port", 22)
        if not isinstance(port, int):
            raise SftpError(f"invalid SFTP port for device: {device}")

        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.WarningPolicy())
        try:
            ssh_client.connect(
                hostname=host,
                port=port,
                username=username,
                password=password or None,
                key_filename=key_filename or None,
                look_for_keys=True,
                allow_agent=True,
            )
            sftp_client = ssh_client.open_sftp()
        except Exception as exc:
            ssh_client.close()
            raise SftpError(f"failed to connect to {device} over SFTP: {exc}") from exc
        return cls(ssh_client=ssh_client, sftp_client=sftp_client, host=host)

    def stat(self, path: str) -> RemoteStat:
        try:
            return _remote_stat(self.sftp_client.stat(path))
        except FileNotFoundError as exc:
            raise SftpError(f"remote path not found: {path}") from exc
        except Exception as exc:
            raise SftpError(f"failed to stat remote path {path}: {exc}") from exc

    def listdir(self, path: str) -> list[RemoteDirEntry]:
        try:
            return [
                RemoteDirEntry(name=entry.filename, stat=_remote_stat(entry))
                for entry in self.sftp_client.listdir_attr(path)
            ]
        except Exception as exc:
            raise SftpError(f"failed to list remote path {path}: {exc}") from exc

    def read_file(self, path: str) -> bytes:
        try:
            with self.sftp_client.open(path, "rb") as handle:
                return handle.read()
        except Exception as exc:
            raise SftpError(f"failed to read remote file {path}: {exc}") from exc

    def close(self) -> None:
        try:
            self.sftp_client.close()
        finally:
            self.ssh_client.close()


def _remote_stat(attr: Any) -> RemoteStat:
    mode = int(attr.st_mode)
    return RemoteStat(
        size=int(attr.st_size),
        modified_ns=int(getattr(attr, "st_mtime", 0)) * 1_000_000_000,
        is_file=stat_module.S_ISREG(mode),
        is_dir=stat_module.S_ISDIR(mode),
    )


def _required_string(remote: dict[str, Any], key: str, device: str) -> str:
    value = remote.get(key)
    if not isinstance(value, str) or not value:
        raise SftpError(f"missing SFTP {key} for device: {device}")
    return value


def _value_or_env(remote: dict[str, Any], value_key: str, env_key: str) -> str | None:
    value = remote.get(value_key)
    if isinstance(value, str) and value:
        return value
    env_name = remote.get(env_key)
    if isinstance(env_name, str) and env_name:
        return os.environ.get(env_name)
    return None
