"""SFTP client helpers."""

from __future__ import annotations

import os
import posixpath
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
        auth_options = _auth_options(remote, password, key_filename)
        timeout_options = _timeout_options(remote)
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
                **timeout_options,
                **auth_options,
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

    def write_file(self, path: str, data: bytes) -> None:
        self.makedirs(posixpath.dirname(path))
        try:
            with self.sftp_client.open(path, "wb") as handle:
                handle.write(data)
        except Exception as exc:
            raise SftpError(f"failed to write remote file {path}: {exc}") from exc

    def exists(self, path: str) -> bool:
        try:
            self.stat(path)
            return True
        except SftpError:
            return False

    def rename(self, old_path: str, new_path: str) -> None:
        self.makedirs(posixpath.dirname(new_path))
        try:
            self.sftp_client.rename(old_path, new_path)
        except Exception as exc:
            raise SftpError(f"failed to rename remote file {old_path} -> {new_path}: {exc}") from exc

    def remove(self, path: str) -> None:
        try:
            self.sftp_client.remove(path)
        except Exception as exc:
            raise SftpError(f"failed to remove remote file {path}: {exc}") from exc

    def makedirs(self, path: str) -> None:
        path = posixpath.normpath(path)
        if path in {"", "."}:
            return
        parts = path.strip("/").split("/")
        current = "/" if path.startswith("/") else ""
        for part in parts:
            current = posixpath.join(current, part) if current else part
            try:
                stat = self.stat(current)
                if not stat.is_dir:
                    raise SftpError(f"remote path is not a directory: {current}")
            except SftpError:
                try:
                    self.sftp_client.mkdir(current)
                except Exception as exc:
                    raise SftpError(f"failed to create remote directory {current}: {exc}") from exc

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


def _auth_options(
    remote: dict[str, Any],
    password: str | None,
    key_filename: str | None,
) -> dict[str, bool]:
    has_explicit_auth = bool(password or key_filename)
    return {
        "look_for_keys": _remote_bool(remote, "look_for_keys", not has_explicit_auth),
        "allow_agent": _remote_bool(remote, "allow_agent", not has_explicit_auth),
    }


def _timeout_options(remote: dict[str, Any]) -> dict[str, float]:
    timeout = _positive_float(remote.get("timeout_seconds", 10.0), "timeout_seconds")
    return {
        "timeout": timeout,
        "banner_timeout": _positive_float(
            remote.get("banner_timeout_seconds", timeout),
            "banner_timeout_seconds",
        ),
        "auth_timeout": _positive_float(
            remote.get("auth_timeout_seconds", timeout),
            "auth_timeout_seconds",
        ),
    }


def _positive_float(value: Any, key: str) -> float:
    if isinstance(value, bool):
        raise SftpError(f"invalid SFTP {key}: {value}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SftpError(f"invalid SFTP {key}: {value}") from exc
    if number <= 0:
        raise SftpError(f"invalid SFTP {key}: {value}")
    return number


def _remote_bool(remote: dict[str, Any], key: str, default: bool) -> bool:
    value = remote.get(key)
    if isinstance(value, bool):
        return value
    return default
