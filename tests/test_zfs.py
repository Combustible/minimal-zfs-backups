"""Tests for zbm.zfs module."""
from __future__ import annotations

import pytest

from zbm import zfs
from zbm.executor import ExecutorError
from zbm.models import Snapshot
from tests.conftest import (
    DST_BMAROHN_SNAPS,
    MockExecutor,
    SRC_BMAROHN_SNAPS,
    make_standard_responses,
)


def test_list_snapshots_basic():
    src_responses, _ = make_standard_responses()
    exec_ = MockExecutor(src_responses)
    snaps = zfs.list_snapshots("ipool/home/bmarohn", exec_)
    assert len(snaps) == len(SRC_BMAROHN_SNAPS)
    assert snaps[0].name == "zfs-auto-snap_monthly-2025-09-18-1447"
    assert snaps[-1].name == "zfs-auto-snap_frequent-2026-02-17-2215"


def test_list_snapshots_filters_children():
    """Snapshots from child datasets should not appear in parent's list."""
    output = (
        "ipool/home/bmarohn@snap-a\n"
        "ipool/home/bmarohn/subdir@snap-b\n"  # child — should be excluded
        "ipool/home/bmarohn@snap-c\n"
    )
    exec_ = MockExecutor({
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", "ipool/home/bmarohn"):
            output,
    })
    snaps = zfs.list_snapshots("ipool/home/bmarohn", exec_)
    assert len(snaps) == 2
    assert all(s.dataset == "ipool/home/bmarohn" for s in snaps)


def test_find_common_snapshot_basic():
    src_snaps = [Snapshot.parse(s) for s in SRC_BMAROHN_SNAPS]
    dst_snaps = [Snapshot.parse(s.replace("ipool/home/bmarohn", "xeonpool/BACKUP/ipool/home/bmarohn"))
                 for s in DST_BMAROHN_SNAPS]
    common = zfs.find_common_snapshot(src_snaps, dst_snaps)
    assert common is not None
    assert common.name == "backup10t-push-2025-11-11"


def test_find_common_snapshot_none():
    src_snaps = [Snapshot.parse("ipool/ds@snap-A")]
    dst_snaps = [Snapshot.parse("pool2/ds@snap-B")]
    common = zfs.find_common_snapshot(src_snaps, dst_snaps)
    assert common is None


def test_find_common_snapshot_returns_newest_common():
    """Should return the most recent common snapshot, not the oldest."""
    src_snaps = [
        Snapshot.parse("ipool/ds@snap-1"),
        Snapshot.parse("ipool/ds@snap-2"),
        Snapshot.parse("ipool/ds@snap-3"),
    ]
    dst_snaps = [
        Snapshot.parse("pool2/ds@snap-1"),
        Snapshot.parse("pool2/ds@snap-2"),
    ]
    common = zfs.find_common_snapshot(src_snaps, dst_snaps)
    assert common.name == "snap-2"


def test_dataset_exists_true():
    exec_ = MockExecutor({
        ("zfs", "list", "-H", "-o", "name", "ipool/home/bmarohn"):
            "ipool/home/bmarohn\n",
    })
    assert zfs.dataset_exists("ipool/home/bmarohn", exec_) is True


def test_dataset_exists_false():
    from zbm.executor import ExecutorError
    exec_ = MockExecutor({
        ("zfs", "list", "-H", "-o", "name", "ipool/nonexistent"):
            ExecutorError(["zfs", "list"], 1, "dataset does not exist"),
    })
    assert zfs.dataset_exists("ipool/nonexistent", exec_) is False


def test_send_incremental_dry_run(capsys):
    src_responses, _ = make_standard_responses()
    src_exec = MockExecutor(src_responses, verbose=False)
    dst_exec = MockExecutor({}, verbose=False)

    common = Snapshot.parse("ipool/home/bmarohn@backup10t-push-2025-11-11")
    latest = Snapshot.parse("ipool/home/bmarohn@zfs-auto-snap_frequent-2026-02-17-2215")

    zfs.send_incremental(
        common=common,
        latest=latest,
        src_executor=src_exec,
        dst_executor=dst_exec,
        dst_dataset="xeonpool/BACKUP/ipool/home/bmarohn",
        dry_run=True,
        verbose=True,
    )

    # No popen calls in dry-run mode
    assert not any("popen" in str(c) for c in src_exec.calls)
    captured = capsys.readouterr()
    assert "zfs send" in captured.out
    assert "zfs recv" in captured.out


def test_destroy_snapshot_dry_run(capsys):
    exec_ = MockExecutor({})
    snap = Snapshot.parse("xeonpool/BACKUP/ipool/home/bmarohn@old-snap")
    zfs.destroy_snapshot(snap, exec_, dry_run=True, verbose=True)
    captured = capsys.readouterr()
    assert "zfs destroy" in captured.out
    # No actual run call
    assert exec_.calls == []
