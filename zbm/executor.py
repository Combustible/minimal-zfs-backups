"""Executor protocol and implementations (local, SSH)."""
from __future__ import annotations

import shlex
import subprocess
from typing import Protocol, runtime_checkable


class ExecutorError(Exception):
    """Raised when a command exits with a non-zero status."""
    def __init__(self, cmd: list[str], returncode: int, stderr: str):
        self.cmd = cmd
        self.returncode = returncode
        self.stderr = stderr
        super().__init__(
            f"Command {shlex.join(cmd)!r} exited {returncode}: {stderr.strip()}"
        )


@runtime_checkable
class Executor(Protocol):
    @property
    def label(self) -> str:
        """Short label for display (e.g. 'local', 'ssh://host')."""
        raise NotImplementedError

    def run(self, cmd: list[str]) -> str:
        """Run a command, return stdout. Raise ExecutorError on failure."""
        raise NotImplementedError

    def popen(self, cmd: list[str], **kwargs) -> subprocess.Popen:
        """Launch a command as a Popen object for piping."""
        raise NotImplementedError


class LocalExecutor:
    """Run commands on the local machine."""

    @property
    def label(self) -> str:
        return "local"

    def run(self, cmd: list[str]) -> str:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise ExecutorError(cmd, result.returncode, result.stderr)
        return result.stdout

    def popen(self, cmd: list[str], **kwargs) -> subprocess.Popen:
        return subprocess.Popen(cmd, text=False, **kwargs)


class SSHExecutor:
    """Run commands on a remote host via SSH."""

    def __init__(self, host: str, user: str | None = None, port: int = 22):
        self.host = host
        self.user = user
        self.port = port

    @property
    def label(self) -> str:
        dest = f"{self.user}@{self.host}" if self.user else self.host
        return f"ssh://{dest}:{self.port}"

    def _ssh_prefix(self) -> list[str]:
        dest = f"{self.user}@{self.host}" if self.user else self.host
        return [
            "ssh",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-p", str(self.port),
            dest,
        ]

    def run(self, cmd: list[str]) -> str:
        full_cmd = self._ssh_prefix() + [shlex.join(cmd)]
        result = subprocess.run(
            full_cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise ExecutorError(full_cmd, result.returncode, result.stderr)
        return result.stdout

    def popen(self, cmd: list[str], **kwargs) -> subprocess.Popen:
        full_cmd = self._ssh_prefix() + [shlex.join(cmd)]
        return subprocess.Popen(full_cmd, text=False, **kwargs)
