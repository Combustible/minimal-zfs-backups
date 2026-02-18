"""Tests for zbm.backup module."""
from __future__ import annotations

import pytest

from zbm.backup import run_backup
from zbm.executor import ExecutorError
from zbm.models import DestinationConfig, JobConfig, SourceConfig
from tests.conftest import (
    DST_BMAROHN_SNAPS,
    MockExecutor,
    SRC_BMAROHN_SNAPS,
    _snap_list_output,
    make_standard_responses,
)

SRC = "ipool/home/bmarohn"
DST = "xeonpool/BACKUP/ipool/home/bmarohn"


def _make_config(datasets=None):
    return JobConfig(
        source=SourceConfig(pool="ipool"),
        destination=DestinationConfig(pool="xeonpool", prefix="BACKUP"),
        datasets=datasets or [SRC],
    )


def test_backup_happy_path_dry_run(capsys):
    src_r, dst_r = make_standard_responses()
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    config = _make_config()

    rc = run_backup(config, src_exec, dst_exec, dry_run=True, verbose=True, no_confirm=True)

    assert rc == 0
    captured = capsys.readouterr()
    assert "zfs send" in captured.out
    assert "zfs recv" in captured.out


def test_backup_up_to_date(capsys):
    """When src and dst have the same HEAD, report 'already up to date'."""
    snap = "ipool/home/bmarohn@zfs-auto-snap_monthly-2026-01-14-1600"
    dst_snap = snap.replace("ipool/home/bmarohn", DST)
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            snap + "\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST):
            dst_snap + "\n",
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 0
    captured = capsys.readouterr()
    assert "up to date" in captured.out.lower()


def test_backup_no_common_snapshot(capsys):
    """When no common snapshot exists, skip and print bootstrap command."""
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/bmarohn@snap-new\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST):
            "xeonpool/BACKUP/ipool/home/bmarohn@snap-old\n",
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 1  # partial failure
    captured = capsys.readouterr()
    assert "No common snapshot" in captured.out
    assert "zfs send -p" in captured.out  # bootstrap command shown


def test_backup_dest_missing(capsys):
    """When destination dataset doesn't exist, skip and show bootstrap command."""
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/bmarohn@snap-a\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
        ("zfs", "list", "-H", "-o", "name", DST):
            ExecutorError(["zfs", "list"], 1, "does not exist"),
    }
    src_exec = MockExecutor(src_r)
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", DST):
            ExecutorError(["zfs", "list"], 1, "does not exist"),
    }
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 1
    captured = capsys.readouterr()
    assert "does not exist" in captured.out.lower()


def test_backup_rollback_needed(capsys):
    """When dest HEAD is ahead of common, warn and skip."""
    # Src: snap-a, snap-b, snap-c
    # Dst: snap-a, snap-b, snap-d (snap-d not on src)
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/bmarohn@snap-a\nipool/home/bmarohn@snap-b\nipool/home/bmarohn@snap-c\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST): (
            "xeonpool/BACKUP/ipool/home/bmarohn@snap-a\n"
            "xeonpool/BACKUP/ipool/home/bmarohn@snap-b\n"
            "xeonpool/BACKUP/ipool/home/bmarohn@snap-d\n"
        ),
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 1
    captured = capsys.readouterr()
    assert "rollback" in captured.out.lower()
    assert "zfs rollback" in captured.out


def test_backup_send_failure(capsys):
    """When send/recv fails, report error and continue to next dataset."""
    from unittest.mock import MagicMock, patch
    import io

    src_r, dst_r = make_standard_responses()
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)

    # Patch send_incremental to raise
    with patch("zbm.backup.zfs.send_incremental",
               side_effect=ExecutorError(["zfs", "send"], 1, "pipe broken")):
        config = _make_config()
        rc = run_backup(config, src_exec, dst_exec, dry_run=False, no_confirm=True)

    assert rc == 1
    captured = capsys.readouterr()
    assert "ERROR" in captured.err or "ERROR" in captured.out
