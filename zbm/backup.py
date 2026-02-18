"""Backup job orchestration: sync snapshots from source to destination."""
from __future__ import annotations

import shlex
import sys
from typing import TYPE_CHECKING

from zbm import zfs
from zbm.executor import ExecutorError

if TYPE_CHECKING:
    from zbm.executor import Executor
    from zbm.models import JobConfig


def _confirm(prompt: str) -> bool:
    """Ask the user yes/no. Return True if yes."""
    try:
        answer = input(f"{prompt} [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer in ("y", "yes")


def _print_initial_send_commands(
    src_dataset: str,
    first_snap: str,
    dst_executor: "Executor",
    dst_dataset: str,
) -> None:
    """Print commands to bootstrap a dataset that has no common snapshot."""
    recv_cmd = shlex.join(["zfs", "recv", dst_dataset])
    if hasattr(dst_executor, 'host'):
        # SSHExecutor
        dest = (
            f"{dst_executor.user}@{dst_executor.host}"
            if dst_executor.user else dst_executor.host
        )
        print(
            f"    To initialize, run:\n"
            f"    zfs send -p {src_dataset}@{first_snap} | "
            f"ssh {dest} {recv_cmd}"
        )
    else:
        print(
            f"    To initialize, run:\n"
            f"    zfs send -p {src_dataset}@{first_snap} | {recv_cmd}"
        )


def run_backup(
    config: "JobConfig",
    src_executor: "Executor",
    dst_executor: "Executor",
    dry_run: bool = False,
    verbose: bool = False,
    no_confirm: bool = False,
) -> int:
    """
    Run a backup job. Returns exit code (0=success, 1=partial failure).

    For each dataset:
    - List source and destination snapshots
    - Find most recent common snapshot
    - If none: print bootstrap command, skip
    - If dest HEAD is ahead of common: warn, print rollback command, skip
    - Send all snapshots from common to latest (dry-run or live)
    """
    datasets = config.datasets

    if not datasets:
        print("No datasets to back up.", file=sys.stderr)
        return 1

    any_error = False

    for src_dataset in datasets:
        print(f"\n{'='*60}")
        print(f"Dataset: {src_dataset}")
        dst_dataset = config.destination.dataset_for(src_dataset)
        print(f"  -> {dst_dataset}")

        # --- check source exists ---
        if not zfs.dataset_exists(src_dataset, src_executor):
            print(f"  ERROR: source dataset does not exist: {src_dataset}")
            any_error = True
            continue

        # --- check destination exists ---
        if not zfs.dataset_exists(dst_dataset, dst_executor):
            print(f"  WARNING: destination dataset does not exist: {dst_dataset}")
            # Get first source snapshot for bootstrap command
            src_snaps = zfs.list_snapshots(src_dataset, src_executor)
            if src_snaps:
                _print_initial_send_commands(
                    src_dataset, src_snaps[0].name, dst_executor, dst_dataset
                )
            else:
                print("  Source has no snapshots. Nothing to do.")
            any_error = True
            continue

        # --- list snapshots ---
        src_snaps = zfs.list_snapshots(src_dataset, src_executor)
        dst_snaps = zfs.list_snapshots(dst_dataset, dst_executor)

        if verbose:
            print(f"  Source snapshots: {len(src_snaps)}")
            print(f"  Dest   snapshots: {len(dst_snaps)}")

        if not src_snaps:
            print("  Source has no snapshots. Nothing to do.")
            continue

        # --- find common snapshot ---
        common = zfs.find_common_snapshot(src_snaps, dst_snaps)

        if common is None:
            print("  ERROR: No common snapshot found between source and destination.")
            if src_snaps:
                _print_initial_send_commands(
                    src_dataset, src_snaps[0].name, dst_executor, dst_dataset
                )
            any_error = True
            continue

        print(f"  Common: @{common.name}")

        # --- check if dest is ahead of common (rollback needed) ---
        if dst_snaps and dst_snaps[-1].name != common.name:
            print(
                f"  WARNING: Destination HEAD (@{dst_snaps[-1].name}) is newer than "
                f"common snapshot (@{common.name})."
            )
            print(
                f"  Destination has snapshots that don't exist on source. "
                f"A rollback is required before receiving."
            )
            print(
                f"  To fix manually, run:\n"
                f"    zfs rollback -r {dst_dataset}@{common.name}"
            )
            print("  Skipping this dataset to avoid data loss.")
            any_error = True
            continue

        # --- collect new snapshots to send ---
        common_idx = next(
            i for i, s in enumerate(src_snaps) if s.name == common.name
        )
        new_snaps = src_snaps[common_idx + 1:]

        if not new_snaps:
            print("  Already up to date.")
            continue

        latest = new_snaps[-1]
        print(f"  Sending {len(new_snaps)} snapshot(s) up to @{latest.name}")

        if not dry_run and not no_confirm:
            if not _confirm(
                f"  Send {len(new_snaps)} snapshot(s) to {dst_dataset}?"
            ):
                print("  Skipped by user.")
                continue

        try:
            zfs.send_incremental(
                common=common,
                latest=latest,
                src_executor=src_executor,
                dst_executor=dst_executor,
                dst_dataset=dst_dataset,
                dry_run=dry_run,
                verbose=verbose,
            )
        except ExecutorError as e:
            print(f"  ERROR: Transfer failed: {e}", file=sys.stderr)
            any_error = True
            continue

        if not dry_run:
            print("  Transfer complete.")

    return 1 if any_error else 0
