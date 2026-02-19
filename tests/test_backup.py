"""Tests for zbm.backup module."""
from __future__ import annotations

from zbm.backup import run_backup
from zbm.executor import ExecutorError
from zbm.models import DestinationConfig, JobConfig, SourceConfig
from tests.conftest import MockExecutor, make_standard_responses

SRC = "ipool/home/user"
DST = "xeonpool/BACKUP/ipool/home/user"


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
    snap = "ipool/home/user@zfs-auto-snap_monthly-2026-01-14-1600"
    dst_snap = snap.replace("ipool/home/user", DST)
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
            "ipool/home/user@snap-new\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST):
            "xeonpool/BACKUP/ipool/home/user@snap-old\n",
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 1  # partial failure
    captured = capsys.readouterr()
    assert "No common snapshot" in captured.err
    assert "zfs send " in captured.err  # bootstrap command shown


def test_backup_dest_missing(capsys):
    """When destination dataset doesn't exist, skip and show bootstrap command."""
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/user@snap-a\n",
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
    assert "does not exist" in captured.err.lower()


def test_backup_rollback_needed(capsys):
    """When dest HEAD is ahead of common, plan a rollback and show victims."""
    # Src: snap-a, snap-b, snap-c
    # Dst: snap-a, snap-b, snap-d (snap-d not on src)
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/user@snap-a\nipool/home/user@snap-b\nipool/home/user@snap-c\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST): (
            "xeonpool/BACKUP/ipool/home/user@snap-a\n"
            "xeonpool/BACKUP/ipool/home/user@snap-b\n"
            "xeonpool/BACKUP/ipool/home/user@snap-d\n"
        ),
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 0  # dry-run rollback + send succeeds
    captured = capsys.readouterr()
    assert "rollback" in captured.out.lower()
    assert "Rollback to: @snap-b" in captured.out
    assert "@snap-d" in captured.out  # victim shown
    assert "1 rollback(s)" in captured.out  # summary


def test_backup_no_rollback_when_common_is_dest_head(capsys):
    """When dest has extra snaps before common but common IS dest HEAD, no rollback."""
    # Src: snap-a, snap-b, snap-c
    # Dst: snap-a, snap-d, snap-b  (snap-d not on src, but common=b is HEAD)
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/user@snap-a\nipool/home/user@snap-b\nipool/home/user@snap-c\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST): (
            "xeonpool/BACKUP/ipool/home/user@snap-a\n"
            "xeonpool/BACKUP/ipool/home/user@snap-d\n"
            "xeonpool/BACKUP/ipool/home/user@snap-b\n"
        ),
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, verbose=True, no_confirm=True)
    assert rc == 0
    captured = capsys.readouterr()
    assert "rollback" not in captured.out.lower()
    assert "1 snapshot(s)" in captured.out  # snap-c to send


def test_backup_common_later_than_dest_only_snaps(capsys):
    """When dest has extra snaps but common is later, no rollback needed."""
    # Src: snap-a, snap-b, snap-c
    # Dst: snap-a, snap-d, snap-b, snap-c  (common=c is HEAD, up to date)
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/user@snap-a\nipool/home/user@snap-b\nipool/home/user@snap-c\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST): (
            "xeonpool/BACKUP/ipool/home/user@snap-a\n"
            "xeonpool/BACKUP/ipool/home/user@snap-d\n"
            "xeonpool/BACKUP/ipool/home/user@snap-b\n"
            "xeonpool/BACKUP/ipool/home/user@snap-c\n"
        ),
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 0
    captured = capsys.readouterr()
    assert "up to date" in captured.out.lower()
    assert "rollback" not in captured.out.lower()


def test_backup_rollback_only_no_new_snaps(capsys):
    """When dest needs rollback but src has no new snaps beyond common, rollback only."""
    # Src: snap-a, snap-b (common=snap-b, no new snaps)
    # Dst: snap-a, snap-b, snap-d (snap-d not on src, needs rollback)
    src_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", SRC):
            "ipool/home/user@snap-a\nipool/home/user@snap-b\n",
        ("zfs", "list", "-H", "-o", "name", SRC): SRC + "\n",
    }
    dst_r = {
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST): (
            "xeonpool/BACKUP/ipool/home/user@snap-a\n"
            "xeonpool/BACKUP/ipool/home/user@snap-b\n"
            "xeonpool/BACKUP/ipool/home/user@snap-d\n"
        ),
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
        ("zfs", "rollback", "-r", f"{DST}@snap-b"): "",
    }
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)
    # Live run (not dry-run) to exercise actual rollback
    rc = run_backup(_make_config(), src_exec, dst_exec, dry_run=False, no_confirm=True)
    assert rc == 0
    captured = capsys.readouterr()
    assert "Rollback to: @snap-b" in captured.out
    assert "@snap-d" in captured.out  # victim shown
    # Should rollback but NOT send (no new snapshots)
    assert "zfs send" not in captured.out
    rollback_cmds = [c for c in dst_exec.calls if "rollback" in str(c)]
    assert len(rollback_cmds) == 1


def test_backup_send_failure(capsys):
    """When send/recv fails, report error and continue to next dataset."""
    from unittest.mock import patch

    src_r, dst_r = make_standard_responses()
    src_exec = MockExecutor(src_r)
    dst_exec = MockExecutor(dst_r)

    # Patch send_incremental to raise
    with patch(
        "zbm.backup.zfs.send_incremental",
        side_effect=ExecutorError(["zfs", "send"], 1, "pipe broken"),
    ):
        config = _make_config()
        rc = run_backup(config, src_exec, dst_exec, dry_run=False, no_confirm=True)

    assert rc == 1
    captured = capsys.readouterr()
    assert "ERROR" in captured.err or "ERROR" in captured.out
