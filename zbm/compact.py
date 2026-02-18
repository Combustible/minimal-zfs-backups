"""Compaction: prune old snapshots on destination per retention rules."""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from zbm import zfs
from zbm.executor import ExecutorError
from zbm.models import RetentionRule, Snapshot

if TYPE_CHECKING:
    from zbm.executor import Executor
    from zbm.models import JobConfig


def _snapshots_to_delete(
    snapshots: list[Snapshot],
    rules: list[RetentionRule],
) -> list[Snapshot]:
    """
    For each rule, find matching snapshots and mark all but the N newest for deletion.
    Returns the combined list of snapshots to delete (no duplicates, preserving order).
    """
    to_delete: list[Snapshot] = []
    seen: set[str] = set()

    for rule in rules:
        matching = [s for s in snapshots if rule.matches(s.name)]
        # matching is oldest→newest (same order as input)
        if rule.keep == 0:
            candidates = matching
        elif len(matching) <= rule.keep:
            candidates = []
        else:
            # Keep the N newest; delete the rest
            candidates = matching[: len(matching) - rule.keep]

        for snap in candidates:
            if snap.full_name not in seen:
                to_delete.append(snap)
                seen.add(snap.full_name)

    return to_delete


def run_compact(
    config: "JobConfig",
    dst_executor: "Executor",
    dry_run: bool = False,
    verbose: bool = False,
    no_confirm: bool = False,
) -> int:
    """
    Run compaction on destination datasets per retention rules.
    Returns exit code (0=success, 1=partial failure).
    """
    if not config.compaction:
        print("No compaction rules defined in config. Nothing to do.")
        return 0

    if config.discover:
        from zbm.executor import LocalExecutor
        # For discover, we need source executor — caller should pass it separately.
        # Compaction only needs destination, so just use the dest datasets.
        # We derive dataset names from destination pool listing.
        all_ds = zfs.list_datasets(config.destination.pool, dst_executor)
        prefix = f"{config.destination.pool}/{config.destination.prefix}/"
        datasets = [
            ds.name for ds in all_ds
            if ds.name.startswith(prefix) and "/" in ds.name[len(prefix):]
        ]
    else:
        datasets = [
            config.destination.dataset_for(ds) for ds in config.datasets
        ]

    any_error = False

    for dst_dataset in datasets:
        print(f"\n{'='*60}")
        print(f"Compacting: {dst_dataset}")

        if not zfs.dataset_exists(dst_dataset, dst_executor):
            print(f"  Dataset does not exist, skipping.")
            continue

        snaps = zfs.list_snapshots(dst_dataset, dst_executor)
        if verbose:
            print(f"  Total snapshots: {len(snaps)}")

        to_delete = _snapshots_to_delete(snaps, config.compaction)

        if not to_delete:
            print("  Nothing to delete.")
            continue

        print(f"  Would delete {len(to_delete)} snapshot(s):")
        for snap in to_delete:
            print(f"    {snap.full_name}")

        if dry_run:
            continue

        if not no_confirm:
            try:
                answer = input(
                    f"  Delete {len(to_delete)} snapshot(s) from {dst_dataset}? [y/N] "
                ).strip().lower()
            except (EOFError, KeyboardInterrupt):
                print()
                print("  Skipped by user.")
                continue
            if answer not in ("y", "yes"):
                print("  Skipped by user.")
                continue

        for snap in to_delete:
            try:
                zfs.destroy_snapshot(snap, dst_executor, dry_run=False, verbose=verbose)
            except ExecutorError as e:
                print(f"  ERROR destroying {snap.full_name}: {e}", file=sys.stderr)
                any_error = True

        print(f"  Deleted {len(to_delete)} snapshot(s).")

    return 1 if any_error else 0
