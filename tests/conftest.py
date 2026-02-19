"""MockExecutor and shared fixtures for testing."""
from __future__ import annotations

import io
import subprocess
from unittest.mock import MagicMock

import pytest


class MockExecutor:
    """
    Executor that returns pre-scripted responses for commands.

    responses: dict mapping frozenset(cmd) or tuple(cmd) -> stdout string
    If the command isn't found, raises KeyError (to catch unexpected calls in tests).

    Pass verbose=True to print every command that goes through the executor.
    """

    def __init__(self, responses: dict | None = None, is_verbose: bool = False, label: str = "mock"):
        self.responses: dict = responses or {}
        self.verbose = is_verbose
        self._label = label
        self.calls: list[list[str]] = []  # record of all commands run
        self.popen_calls: list[tuple[list[str], list[str]]] = []  # (send_cmd, recv_cmd)

    @property
    def label(self) -> str:
        return self._label

    def _key(self, cmd: list[str]) -> tuple:
        return tuple(cmd)

    def run(self, cmd: list[str]) -> str:
        self.calls.append(cmd)
        key = self._key(cmd)
        if self.verbose:
            import shlex
            print(f"  [mock.run] {shlex.join(cmd)}")
        if key not in self.responses:
            raise KeyError(f"MockExecutor: unexpected command: {cmd}")
        result = self.responses[key]
        if isinstance(result, Exception):
            raise result
        return result

    def popen(self, cmd: list[str], **_kwargs) -> subprocess.Popen:
        """
        For send/recv pipe tests: record the call and return mock Popen objects
        that succeed immediately.
        """
        self.calls.append(cmd)
        if self.verbose:
            import shlex
            print(f"  [mock.popen] {shlex.join(cmd)}")

        mock_proc = MagicMock(spec=subprocess.Popen)
        mock_proc.stdout = io.BytesIO(b"")
        mock_proc.stdin = io.BytesIO(b"")
        mock_proc.returncode = 0
        mock_proc.wait.return_value = 0
        return mock_proc


# ---------------------------------------------------------------------------
# Snapshot data drawn from the real examples in the prompt
# ---------------------------------------------------------------------------

SRC_USER_SNAPS = [
    "ipool/home/user@zfs-auto-snap_monthly-2025-09-18-1447",
    "ipool/home/user@zfs-auto-snap_monthly-2025-10-18-1640",
    "ipool/home/user@backup10t-push-2025-11-11",
    "ipool/home/user@zfs-auto-snap_monthly-2025-12-14-1020",
    "ipool/home/user@zfs-auto-snap_weekly-2025-12-30-1544",
    "ipool/home/user@zfs-auto-snap_weekly-2026-01-06-1540",
    "ipool/home/user@zfs-auto-snap_monthly-2026-01-14-1600",
    "ipool/home/user@zfs-auto-snap_daily-2026-01-18-1535",
    "ipool/home/user@zfs-auto-snap_daily-2026-02-03-1539",
    "ipool/home/user@zfs-auto-snap_weekly-2026-02-03-1544",
    "ipool/home/user@zfs-auto-snap_daily-2026-02-04-1537",
    "ipool/home/user@zfs-auto-snap_daily-2026-02-09-1539",
    "ipool/home/user@zfs-auto-snap_daily-2026-02-10-1538",
    "ipool/home/user@zfs-auto-snap_weekly-2026-02-10-1543",
    "ipool/home/user@zfs-auto-snap_daily-2026-02-12-1540",
    "ipool/home/user@zfs-auto-snap_daily-2026-02-17-1450",
    "ipool/home/user@zfs-auto-snap_weekly-2026-02-17-1455",
    "ipool/home/user@zfs-auto-snap_hourly-2026-02-17-1717",
    "ipool/home/user@zfs-auto-snap_hourly-2026-02-17-1917",
    "ipool/home/user@zfs-auto-snap_frequent-2026-02-17-2200",
    "ipool/home/user@zfs-auto-snap_frequent-2026-02-17-2215",
]

# Destination has synced up to backup10t-push-2025-11-11
DST_USER_SNAPS = [
    "xeonpool/BACKUP/ipool/home/user@zfs-auto-snap_monthly-2025-09-18-1447",
    "xeonpool/BACKUP/ipool/home/user@zfs-auto-snap_monthly-2025-10-18-1640",
    "xeonpool/BACKUP/ipool/home/user@backup10t-push-2025-11-11",
]


def _snap_list_output(full_names: list[str]) -> str:
    return "\n".join(full_names) + "\n"


def make_standard_responses(
    src_dataset: str = "ipool/home/user",
    dst_dataset: str = "xeonpool/BACKUP/ipool/home/user",
    src_snaps: list[str] | None = None,
    dst_snaps: list[str] | None = None,
) -> tuple[dict, dict]:
    """
    Return (src_responses, dst_responses) dicts for MockExecutors.
    """
    if src_snaps is None:
        src_snaps = SRC_USER_SNAPS
    if dst_snaps is None:
        dst_snaps = DST_USER_SNAPS

    src_responses = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", src_dataset):
            _snap_list_output(src_snaps),
        ("zfs", "list", "-H", "-o", "name", src_dataset):
            src_dataset + "\n",
    }

    dst_responses = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", dst_dataset):
            _snap_list_output(dst_snaps),
        ("zfs", "list", "-H", "-o", "name", dst_dataset):
            dst_dataset + "\n",
    }

    return src_responses, dst_responses


@pytest.fixture
def verbose(request):
    """True if -v was passed to pytest."""
    return request.config.getoption("--verbose", default=False)
