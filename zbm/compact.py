"""Compaction: prune old snapshots on destination per retention rules."""
from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import re

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
        matching = [s for s in snapshots if rule.matches(s.full_name)]
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

    Two-pass approach: collect all deletions, prompt once, then execute.
    """
    if not config.compaction:
        print("No compaction rules defined in config. Nothing to do.")
        return 0

    datasets = [
        config.destination.dataset_for(ds) for ds in config.datasets
    ]

    # --- Phase 1: Plan ---
    # List of (dst_dataset, to_delete) for datasets with work to do
    delete_plans: list[tuple[str, list[Snapshot]]] = []

    for dst_dataset in datasets:
        if not zfs.dataset_exists(dst_dataset, dst_executor):
            print(f"\n{dst_dataset}: dataset does not exist, skipping.")
            continue

        snaps = zfs.list_snapshots(dst_dataset, dst_executor)
        if verbose:
            print(f"\n{dst_dataset}: {len(snaps)} total snapshots")

        qualified_rules = [
            RetentionRule(
                pattern=re.escape(dst_dataset) + "@" + rule.pattern,
                keep=rule.keep,
            )
            for rule in config.compaction
        ]
        to_delete = _snapshots_to_delete(snaps, qualified_rules)

        if not to_delete:
            print(f"\n{dst_dataset}: nothing to delete.")
            continue

        delete_plans.append((dst_dataset, to_delete))

    if not delete_plans:
        print("\nNothing to compact.")
        return 0

    # --- Phase 2: Show plan and prompt ---
    total = sum(len(td) for _, td in delete_plans)
    label = "Would delete" if dry_run else "Will delete"
    print(f"\n{'='*60}")
    print(f"{label} {total} snapshot(s) across {len(delete_plans)} dataset(s):\n")
    for dst_dataset, to_delete in delete_plans:
        print(f"  {dst_dataset}: {len(to_delete)} snapshot(s)")
        for snap in to_delete:
            print(f"    {snap.full_name}")

    if dry_run:
        return 0

    if not no_confirm:
        try:
            answer = input(
                f"\nDelete {total} snapshot(s)? [y/N] "
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print("\nAborted by user.")
            return 1
        if answer not in ("y", "yes"):
            print("Aborted by user.")
            return 1

    # --- Phase 3: Execute ---
    any_error = False
    for dst_dataset, to_delete in delete_plans:
        print(f"\n{'='*60}")
        print(f"Compacting: {dst_dataset}")
        deleted = 0
        for snap in to_delete:
            try:
                zfs.destroy_snapshot(snap, dst_executor, dry_run=False, verbose=verbose)
                deleted += 1
            except ExecutorError as e:
                print(f"  ERROR destroying {snap.full_name}: {e}", file=sys.stderr)
                any_error = True

        print(f"  Deleted {deleted} of {len(to_delete)} snapshot(s).")

    return 1 if any_error else 0
