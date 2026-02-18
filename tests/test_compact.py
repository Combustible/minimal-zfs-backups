"""Tests for zbm.compact module."""
from __future__ import annotations

import re

import pytest

from zbm.compact import _snapshots_to_delete, run_compact
from zbm.executor import ExecutorError
from zbm.models import (
    DestinationConfig,
    JobConfig,
    RetentionRule,
    Snapshot,
    SourceConfig,
)
from tests.conftest import DST_USER_SNAPS, MockExecutor, _snap_list_output

DST = "xeonpool/BACKUP/ipool/home/user"


def _qualified_rule(pattern: str, keep: int, dataset: str = DST) -> RetentionRule:
    """Build a qualified rule matching full_name: dataset@pattern."""
    return RetentionRule(pattern=re.escape(dataset) + "@" + pattern, keep=keep)


def _make_config(compaction):
    return JobConfig(
        source=SourceConfig(pool="ipool"),
        destination=DestinationConfig(pool="xeonpool", prefix="BACKUP"),
        datasets=["ipool/home/user"],
        compaction=compaction,
    )


def _make_snaps(names: list[str], dataset: str = DST) -> list[Snapshot]:
    return [Snapshot(dataset=dataset, name=n) for n in names]


# ---------------------------------------------------------------------------
# Unit tests for _snapshots_to_delete
# ---------------------------------------------------------------------------

def test_keep_zero_deletes_all_matching():
    snaps = _make_snaps([
        "zfs-auto-snap_frequent-2026-02-17-2200",
        "zfs-auto-snap_frequent-2026-02-17-2215",
        "zfs-auto-snap_monthly-2026-01-14-1600",
    ])
    rules = [_qualified_rule("zfs-auto-snap_frequent-.*", keep=0)]
    to_delete = _snapshots_to_delete(snaps, rules)
    names = {s.name for s in to_delete}
    assert "zfs-auto-snap_frequent-2026-02-17-2200" in names
    assert "zfs-auto-snap_frequent-2026-02-17-2215" in names
    assert "zfs-auto-snap_monthly-2026-01-14-1600" not in names


def test_keep_n_preserves_newest():
    snaps = _make_snaps([
        "zfs-auto-snap_daily-2026-02-01-1000",
        "zfs-auto-snap_daily-2026-02-02-1000",
        "zfs-auto-snap_daily-2026-02-03-1000",
        "zfs-auto-snap_daily-2026-02-04-1000",
        "zfs-auto-snap_daily-2026-02-05-1000",
    ])
    rules = [_qualified_rule("zfs-auto-snap_daily-.*", keep=3)]
    to_delete = _snapshots_to_delete(snaps, rules)
    assert len(to_delete) == 2
    delete_names = [s.name for s in to_delete]
    assert "zfs-auto-snap_daily-2026-02-01-1000" in delete_names
    assert "zfs-auto-snap_daily-2026-02-02-1000" in delete_names
    assert "zfs-auto-snap_daily-2026-02-05-1000" not in delete_names


def test_keep_more_than_count_deletes_nothing():
    snaps = _make_snaps([
        "zfs-auto-snap_monthly-2026-01-14-1600",
        "zfs-auto-snap_monthly-2026-02-14-1600",
    ])
    rules = [_qualified_rule("zfs-auto-snap_monthly-.*", keep=24)]
    to_delete = _snapshots_to_delete(snaps, rules)
    assert to_delete == []


def test_multiple_rules_no_duplicates():
    """A snapshot matching two rules should only appear once in delete list."""
    snaps = _make_snaps([
        "snap-a",
        "snap-b",
    ])
    rules = [
        _qualified_rule("snap-.*", keep=0),
        _qualified_rule("snap-a", keep=0),
    ]
    to_delete = _snapshots_to_delete(snaps, rules)
    full_names = [s.full_name for s in to_delete]
    assert len(full_names) == len(set(full_names))


# ---------------------------------------------------------------------------
# Integration tests for run_compact
# ---------------------------------------------------------------------------

def _dst_responses(snaps: list[str]) -> dict:
    return {
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST):
            _snap_list_output(snaps),
    }


def test_compact_dry_run_no_deletes(capsys):
    dst_snaps = [
        f"{DST}@zfs-auto-snap_frequent-2026-02-17-2200",
        f"{DST}@zfs-auto-snap_frequent-2026-02-17-2215",
        f"{DST}@zfs-auto-snap_monthly-2026-01-14-1600",
    ]
    dst_exec = MockExecutor(_dst_responses(dst_snaps))
    config = _make_config([
        RetentionRule(pattern="zfs-auto-snap_frequent-.*", keep=0),
    ])
    rc = run_compact(config, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 0
    # No destroy commands should have been issued
    assert not any("destroy" in str(c) for c in dst_exec.calls)
    captured = capsys.readouterr()
    assert "frequent" in captured.out


def test_compact_live_deletes(capsys):
    dst_snaps = [
        f"{DST}@zfs-auto-snap_frequent-2026-02-17-2200",
        f"{DST}@zfs-auto-snap_frequent-2026-02-17-2215",
    ]
    destroy_calls = []
    responses = _dst_responses(dst_snaps)
    # Add destroy responses
    for snap in dst_snaps:
        snap_name = snap.split("@")[1]
        responses[("zfs", "destroy", f"{DST}@{snap_name}")] = ""

    dst_exec = MockExecutor(responses)
    config = _make_config([
        RetentionRule(pattern="zfs-auto-snap_frequent-.*", keep=0),
    ])
    rc = run_compact(config, dst_exec, dry_run=False, no_confirm=True)
    assert rc == 0
    destroy_cmds = [c for c in dst_exec.calls if c[0:2] == ["zfs", "destroy"]]
    assert len(destroy_cmds) == 2


def test_compact_nothing_to_delete(capsys):
    dst_snaps = [f"{DST}@zfs-auto-snap_monthly-2026-01-14-1600"]
    dst_exec = MockExecutor(_dst_responses(dst_snaps))
    config = _make_config([
        RetentionRule(pattern="zfs-auto-snap_frequent-.*", keep=0),
    ])
    rc = run_compact(config, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 0
    captured = capsys.readouterr()
    assert "Nothing to delete" in captured.out


def test_compact_ignores_unconfigured_datasets(capsys):
    """Snapshots on datasets not in config.datasets must never be compacted."""
    OTHER_DST = "xeonpool/BACKUP/ipool/other"
    # Set up two datasets on destination — only one is configured
    configured_snaps = [
        f"{DST}@zfs-auto-snap_frequent-2026-02-17-2200",
    ]
    other_snaps = [
        f"{OTHER_DST}@zfs-auto-snap_frequent-2026-02-17-2200",
        f"{OTHER_DST}@zfs-auto-snap_frequent-2026-02-17-2215",
    ]
    responses = {
        ("zfs", "list", "-H", "-o", "name", DST): DST + "\n",
        ("zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", DST):
            _snap_list_output(configured_snaps),
        ("zfs", "destroy", f"{DST}@zfs-auto-snap_frequent-2026-02-17-2200"): "",
        # The other dataset should never be queried
    }
    dst_exec = MockExecutor(responses)
    config = _make_config([
        RetentionRule(pattern="zfs-auto-snap_frequent-.*", keep=0),
    ])
    rc = run_compact(config, dst_exec, dry_run=False, no_confirm=True)
    assert rc == 0
    # Verify no commands were issued for the other dataset
    for cmd in dst_exec.calls:
        assert OTHER_DST not in " ".join(cmd), \
            f"Unexpected command touching unconfigured dataset: {cmd}"


def test_compact_qualified_rules_reject_wrong_dataset():
    """Rules qualified for one dataset must not match snapshots from another."""
    snaps_other = _make_snaps(
        ["zfs-auto-snap_frequent-2026-02-17-2200"],
        dataset="xeonpool/BACKUP/ipool/other",
    )
    # Rules qualified for DST should NOT match snapshots on a different dataset
    rules = [_qualified_rule("zfs-auto-snap_frequent-.*", keep=0, dataset=DST)]
    to_delete = _snapshots_to_delete(snaps_other, rules)
    assert to_delete == [], "Should not delete snapshots from a different dataset"


def test_compact_no_rules(capsys):
    dst_exec = MockExecutor({})
    config = _make_config([])
    rc = run_compact(config, dst_exec, dry_run=True, no_confirm=True)
    assert rc == 0
    captured = capsys.readouterr()
    assert "No compaction rules" in captured.out
