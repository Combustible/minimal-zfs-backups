"""Backup job orchestration: sync snapshots from source to destination."""
from __future__ import annotations

import os
import shlex
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from zbm import zfs
from zbm.executor import ExecutorError
from zbm.models import Snapshot

if TYPE_CHECKING:
    from zbm.executor import Executor
    from zbm.models import JobConfig

# ANSI color codes (respect NO_COLOR convention: https://no-color.org)
if os.environ.get("NO_COLOR") is not None:
    GREEN = RED = YELLOW = RESET = ""
else:
    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    RESET = "\033[0m"


def _confirm(prompt: str) -> bool:
    """Ask the user yes/no. Return True if yes."""
    try:
        answer = input(f"{prompt} [y/N] ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return answer in ("y", "yes")


def _format_bootstrap_command(
    src_dataset: str,
    first_snap: str,
    dst_executor: "Executor",
    dst_dataset: str,
) -> str:
    """Return the bootstrap command string for a dataset with no common snapshot."""
    recv_cmd = shlex.join(["zfs", "recv", "-F", dst_dataset])
    label = dst_executor.label
    if label.startswith("ssh://"):
        # Extract user@host from ssh://user@host:port
        dest = label[len("ssh://"):].rsplit(":", 1)[0]
        return f"zfs send {src_dataset}@{first_snap} | ssh {dest} {recv_cmd}"

    return f"zfs send {src_dataset}@{first_snap} | {recv_cmd}"


@dataclass
class _DatasetPlan:
    """Plan for a single dataset within a backup job."""
    src_dataset: str
    dst_dataset: str
    # One of: "send", "up_to_date", "rollback_and_send", "rollback_only", "error", "skip"
    action: str = "skip"
    # For send / rollback_and_send
    common: Snapshot | None = None
    latest: Snapshot | None = None
    new_snap_count: int = 0
    # For rollback_and_send: snapshots after common on dest that will be removed
    rollback_victims: list[Snapshot] = field(default_factory=list)
    # Error/skip reason
    message: str = ""
    # Bootstrap command (for missing dest or no common snapshot)
    bootstrap_cmd: str = ""


def _plan_dataset(
    src_dataset: str,
    dst_dataset: str,
    src_executor: "Executor",
    dst_executor: "Executor",
    verbose: bool = False,
) -> _DatasetPlan:
    """Analyze a dataset and return a plan for what to do."""
    plan = _DatasetPlan(src_dataset=src_dataset, dst_dataset=dst_dataset)

    # Check source exists
    if not zfs.dataset_exists(src_dataset, src_executor):
        plan.action = "error"
        plan.message = f"Source dataset does not exist: {src_dataset}"
        return plan

    # Check destination exists
    if not zfs.dataset_exists(dst_dataset, dst_executor):
        plan.action = "error"
        plan.message = f"Destination dataset does not exist: {dst_dataset}"
        src_snaps = zfs.list_snapshots(src_dataset, src_executor)
        if src_snaps:
            plan.bootstrap_cmd = _format_bootstrap_command(
                src_dataset, src_snaps[0].name, dst_executor, dst_dataset
            )
        return plan

    # List snapshots
    src_snaps = zfs.list_snapshots(src_dataset, src_executor)
    dst_snaps = zfs.list_snapshots(dst_dataset, dst_executor)

    if verbose:
        print(f"  {src_dataset}: {len(src_snaps)} src, {len(dst_snaps)} dst snapshots")

    if not src_snaps:
        plan.action = "skip"
        plan.message = "Source has no snapshots"
        return plan

    # Find common snapshot
    common = zfs.find_common_snapshot(src_snaps, dst_snaps)

    if common is None:
        plan.action = "error"
        plan.message = "No common snapshot found between source and destination"
        plan.bootstrap_cmd = _format_bootstrap_command(
            src_dataset, src_snaps[0].name, dst_executor, dst_dataset
        )
        return plan

    plan.common = common

    # Check if rollback needed (dest has snapshots after common)
    needs_rollback = dst_snaps and dst_snaps[-1].name != common.name
    if needs_rollback:
        # Find the common snapshot index in dst and collect victims
        common_dst_idx = next(
            i for i, s in enumerate(dst_snaps) if s.name == common.name
        )
        plan.rollback_victims = dst_snaps[common_dst_idx + 1:]

    # Collect new snapshots to send
    common_src_idx = next(
        i for i, s in enumerate(src_snaps) if s.name == common.name
    )
    new_snaps = src_snaps[common_src_idx + 1:]

    if not new_snaps and not needs_rollback:
        plan.action = "up_to_date"
        plan.message = "Already up to date"
        return plan

    if needs_rollback:
        plan.action = "rollback_and_send" if new_snaps else "rollback_only"
    else:
        plan.action = "send"

    if new_snaps:
        plan.latest = new_snaps[-1]
        plan.new_snap_count = len(new_snaps)

    return plan


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

    Two-pass approach:
    1. Plan: analyze all datasets, determine actions needed
    2. Execute: prompt once for rollbacks if needed, then send
    """
    datasets = config.datasets

    if not datasets:
        print("No datasets to back up.", file=sys.stderr)
        return 1

    # --- Phase 1: Plan ---
    plans: list[_DatasetPlan] = []
    for src_dataset in datasets:
        dst_dataset = config.destination.dataset_for(src_dataset)
        plan = _plan_dataset(
            src_dataset, dst_dataset, src_executor, dst_executor, verbose
        )
        plans.append(plan)

    # --- Print plan summary ---
    rollback_plans = [p for p in plans if p.action in ("rollback_and_send", "rollback_only")]
    send_plans = [p for p in plans if p.action == "send"]
    up_to_date_plans = [p for p in plans if p.action == "up_to_date"]
    error_plans = [p for p in plans if p.action == "error"]
    skip_plans = [p for p in plans if p.action == "skip"]

    for plan in error_plans:
        print(f"\n{RED}ERROR: {plan.message}{RESET}", file=sys.stderr)
        if plan.bootstrap_cmd:
            print(f"  To initialize, run:\n    {plan.bootstrap_cmd}", file=sys.stderr)
            print(
                f"  {YELLOW}Consider creating the destination dataset first and then setting desired properties before receiving.\n"
                f"  (e.g. compression, atime, readonly, com.sun:auto-snapshot=false).{RESET}",
                file=sys.stderr,
            )

    for plan in skip_plans:
        if plan.message:
            print(f"\n{plan.src_dataset}: {plan.message}")

    for plan in up_to_date_plans:
        print(f"\n{plan.src_dataset}: {GREEN}Up to date{RESET}")

    for plan in send_plans:
        print(
            f"\n{plan.src_dataset}: "
            f"Send {plan.new_snap_count} snapshot(s) up to @{plan.latest.name}"
        )

    # --- Phase 2: Prompt for rollbacks ---
    if rollback_plans:
        print(f"\n{'='*60}")
        print(f"{YELLOW}The following datasets require rollback before receiving:{RESET}\n")
        for plan in rollback_plans:
            print(f"  {plan.dst_dataset}:")
            print(f"    {GREEN}Rollback to: @{plan.common.name}{RESET}")
            for victim in plan.rollback_victims:
                print(f"    {RED}Delete:      @{victim.name}{RESET}")
            if plan.new_snap_count > 0:
                print(f"    Then send {plan.new_snap_count} new snapshot(s)")
            print()

        if not no_confirm:
            if not _confirm("Proceed with rollbacks?"):
                print("Aborted by user.")
                return 1

    # --- Phase 3: Execute ---
    any_error = bool(error_plans)
    sent_count = 0
    rollback_count = 0

    for plan in rollback_plans:
        print(f"\n{'='*60}")
        print(f"Rolling back: {plan.dst_dataset} -> @{plan.common.name}")
        if not dry_run:
            try:
                dst_executor.run([
                    "zfs", "rollback", "-r",
                    f"{plan.dst_dataset}@{plan.common.name}",
                ])
                rollback_count += 1
            except ExecutorError as e:
                print(f"  {RED}ERROR: Rollback failed: {e}{RESET}", file=sys.stderr)
                any_error = True
                continue
        else:
            print(f"  [dry-run] zfs rollback -r {plan.dst_dataset}@{plan.common.name}")
            rollback_count += 1

        # Send after rollback
        if plan.latest:
            print(f"  Sending {plan.new_snap_count} snapshot(s) up to @{plan.latest.name}")
            try:
                zfs.send_incremental(
                    common=plan.common,
                    latest=plan.latest,
                    src_executor=src_executor,
                    dst_executor=dst_executor,
                    dst_dataset=plan.dst_dataset,
                    dry_run=dry_run,
                    verbose=verbose,
                )
                sent_count += 1
            except ExecutorError as e:
                print(f"  {RED}ERROR: Transfer failed: {e}{RESET}", file=sys.stderr)
                any_error = True
                continue
            print(f"  {GREEN}Transfer complete.{RESET}")

    for plan in send_plans:
        print(f"\n{'='*60}")
        print(f"Sending: {plan.src_dataset} -> {plan.dst_dataset}")
        print(f"  {plan.new_snap_count} snapshot(s) up to @{plan.latest.name}")
        try:
            zfs.send_incremental(
                common=plan.common,
                latest=plan.latest,
                src_executor=src_executor,
                dst_executor=dst_executor,
                dst_dataset=plan.dst_dataset,
                dry_run=dry_run,
                verbose=verbose,
            )
            sent_count += 1
        except ExecutorError as e:
            print(f"  {RED}ERROR: Transfer failed: {e}{RESET}", file=sys.stderr)
            any_error = True
            continue
        print(f"  {GREEN}Transfer complete.{RESET}")

    # --- Phase 4: Summary ---
    print(f"\n{'='*60}")
    prefix = "[dry-run] " if dry_run else ""
    print(f"{prefix}Backup complete.")
    parts = []
    if sent_count:
        parts.append(f"{sent_count} dataset(s) sent")
    if rollback_count:
        parts.append(f"{rollback_count} rollback(s)")
    if len(up_to_date_plans):
        parts.append(f"{len(up_to_date_plans)} already up to date")
    if len(error_plans):
        parts.append(f"{RED}{len(error_plans)} error(s){RESET}")
    if len(skip_plans):
        parts.append(f"{len(skip_plans)} skipped")
    if parts:
        print(f"  {', '.join(parts)}")

    return 1 if any_error else 0
