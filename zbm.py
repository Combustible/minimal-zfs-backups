"""ZFS Backup Manager — single-file distribution."""
from __future__ import annotations

import argparse
import os
import re
import shlex
import subprocess
import sys
import types as _types
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

import yaml

# ============================================================
# MODELS
# ============================================================


@dataclass(frozen=True, order=True)
class Snapshot:
    """A ZFS snapshot: pool/dataset@name."""
    dataset: str
    name: str  # just the snapshot name after '@'

    @property
    def full_name(self) -> str:
        return f"{self.dataset}@{self.name}"

    @classmethod
    def parse(cls, full_name: str) -> "Snapshot":
        dataset, _, name = full_name.partition("@")
        if not name:
            raise ValueError(f"Not a snapshot: {full_name!r}")
        return cls(dataset=dataset, name=name)


@dataclass(frozen=True)
class Dataset:
    name: str  # e.g. ipool/home/user

    @property
    def pool(self) -> str:
        return self.name.split("/")[0]


@dataclass
class RetentionRule:
    """Keep the N most recent snapshots matching a pattern.

    Pattern is matched with re.fullmatch against the entire snapshot name,
    so it must match the complete string. E.g. "zfs-auto-snap_daily-.*"
    matches "zfs-auto-snap_daily-2026-02-01-1000" but "daily" alone does not.
    """
    pattern: str
    keep: int

    def matches(self, snapshot_name: str) -> bool:
        return bool(re.fullmatch(self.pattern, snapshot_name))


@dataclass
class SourceConfig:
    pool: str


@dataclass
class DestinationConfig:
    pool: str
    prefix: str = "BACKUP"
    host: str | None = None
    user: str | None = None
    port: int = 22

    @property
    def is_remote(self) -> bool:
        return self.host is not None

    def dataset_for(self, src_dataset: str) -> str:
        """Return the destination dataset path for a given source dataset.

        Example: ipool/home/user -> xeonpool/BACKUP/ipool/home/user
        """
        return f"{self.pool}/{self.prefix}/{src_dataset}"


@dataclass
class JobConfig:
    source: SourceConfig
    destination: DestinationConfig
    datasets: list[str]          # explicit dataset names
    compaction: list[RetentionRule] = field(default_factory=list)


# ============================================================
# EXECUTOR
# ============================================================


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


# ============================================================
# CONFIG
# ============================================================


class ConfigError(Exception):
    pass


def load_source_pool(path: str) -> str:
    """Load only the source pool name from a config file (for discover)."""
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ConfigError(f"Config must be a YAML mapping: {path}")
    src_raw = raw.get("source")
    if not src_raw or not src_raw.get("pool"):
        raise ConfigError("source.pool is required")
    return src_raw["pool"]


def load_job(path: str) -> JobConfig:
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ConfigError(f"Config must be a YAML mapping: {path}")

    # --- source ---
    src_raw = raw.get("source")
    if not src_raw or not src_raw.get("pool"):
        raise ConfigError("source.pool is required")
    source = SourceConfig(pool=src_raw["pool"])

    # --- destination ---
    dst_raw = raw.get("destination")
    if not dst_raw or not dst_raw.get("pool"):
        raise ConfigError("destination.pool is required")
    prefix = dst_raw.get("prefix", "BACKUP")
    if not prefix:
        raise ConfigError("destination.prefix must not be empty")
    destination = DestinationConfig(
        pool=dst_raw["pool"],
        prefix=prefix,
        host=dst_raw.get("host"),
        user=dst_raw.get("user"),
        port=int(dst_raw.get("port", 22)),
    )

    # --- datasets ---
    datasets_raw = raw.get("datasets", [])
    if not datasets_raw:
        raise ConfigError("'datasets' list is required")
    datasets = []
    for d in datasets_raw:
        name = str(d).strip() if d is not None else ""
        if not name or name == "None":
            raise ConfigError(f"Invalid dataset entry: {d!r}")
        datasets.append(name)

    # --- compaction ---
    compaction = []
    for rule in raw.get("compaction", []):
        if "pattern" not in rule or "keep" not in rule:
            raise ConfigError("Each compaction rule needs 'pattern' and 'keep'")
        keep = int(rule["keep"])
        if keep < 0:
            raise ConfigError(f"Compaction rule 'keep' must be >= 0, got {keep}")
        pattern = rule["pattern"]
        try:
            re.compile(pattern)
        except re.error as e:
            raise ConfigError(f"Invalid regex in compaction pattern {pattern!r}: {e}") from e
        compaction.append(RetentionRule(pattern=pattern, keep=keep))

    return JobConfig(
        source=source,
        destination=destination,
        datasets=datasets,
        compaction=compaction,
    )


# ============================================================
# ZFS OPERATIONS
# ============================================================


def list_datasets(pool: str, executor: "Executor") -> list[Dataset]:
    """Return all datasets in a pool (excluding the pool root itself)."""
    output = executor.run(["zfs", "list", "-H", "-o", "name", "-r", pool])
    results = []
    for line in output.splitlines():
        name = line.strip()
        if name and name != pool:
            results.append(Dataset(name=name))
    return results


def list_snapshots(dataset: str, executor: "Executor") -> list[Snapshot]:
    """Return snapshots for a dataset, oldest first."""
    output = executor.run([
        "zfs", "list", "-H", "-o", "name", "-t", "snapshot", "-r", dataset,
    ])
    results = []
    for line in output.splitlines():
        name = line.strip()
        if not name:
            continue
        # Only include snapshots directly on this dataset (not children)
        if "@" in name and name.split("@")[0] == dataset:
            results.append(Snapshot.parse(name))
    return results


def dataset_exists(dataset: str, executor: "Executor") -> bool:
    """Return True if the dataset exists."""
    try:
        executor.run(["zfs", "list", "-H", "-o", "name", dataset])
        return True
    except ExecutorError:
        return False


def get_auto_snapshot_property(dataset: str, executor: "Executor") -> bool:
    """Return the effective value of com.sun:auto-snapshot for a dataset."""
    try:
        output = executor.run([
            "zfs", "get", "-H", "-o", "value", "com.sun:auto-snapshot", dataset,
        ])
        return output.strip() == "true"
    except ExecutorError:
        return False


def discover_datasets(pool: str, executor: "Executor") -> list[Dataset]:
    """Return datasets in pool where com.sun:auto-snapshot is effectively true."""
    all_ds = list_datasets(pool, executor)
    return [ds for ds in all_ds if get_auto_snapshot_property(ds.name, executor)]


def find_common_snapshot(
    src_snaps: list[Snapshot],
    dst_snaps: list[Snapshot],
) -> Snapshot | None:
    """Return the most recent snapshot present in both lists, or None."""
    dst_names = {s.name for s in dst_snaps}
    # Iterate src newest→oldest
    for snap in reversed(src_snaps):
        if snap.name in dst_names:
            return snap
    return None


def send_incremental(
    common: Snapshot,
    latest: Snapshot,
    src_executor: "Executor",
    dst_executor: "Executor",
    dst_dataset: str,
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """
    Send all snapshots from common (exclusive) to latest (inclusive) to dst_dataset.

    Uses: zfs send -c -I pool/dataset@common pool/dataset@latest | [ssh] zfs recv dst_dataset

    The -c (--compressed) flag sends blocks in their on-disk compressed form,
    avoiding a needless decompress/recompress cycle and reducing bytes over the
    wire when the source dataset has compression enabled.  It is a safe no-op
    when the source dataset is uncompressed.
    """
    send_cmd = [
        "zfs", "send", "-c", "-I",
        common.full_name,
        latest.full_name,
    ]
    recv_cmd = ["zfs", "recv", dst_dataset]

    if dry_run or verbose:
        print(f"  [send] {shlex.join(send_cmd)}")
        print(f"  [recv ({dst_executor.label})] {shlex.join(recv_cmd)}")

    if dry_run:
        return

    try:
        send_proc = src_executor.popen(send_cmd, stdout=subprocess.PIPE)
    except OSError as e:
        raise ExecutorError(send_cmd, 1, str(e)) from e
    try:
        recv_proc = dst_executor.popen(recv_cmd, stdin=send_proc.stdout)
    except OSError as e:
        send_proc.kill()
        send_proc.wait()
        raise ExecutorError(recv_cmd, 1, str(e)) from e
    # Allow send_proc to receive SIGPIPE if recv_proc dies
    send_proc.stdout.close()

    recv_rc = recv_proc.wait()
    send_rc = send_proc.wait()

    if send_rc != 0 or recv_rc != 0:
        raise ExecutorError(
            send_cmd + ["|"] + recv_cmd,
            max(send_rc, recv_rc),
            f"send exited {send_rc}, recv exited {recv_rc}",
        )


def destroy_snapshot(
    snapshot: Snapshot,
    executor: "Executor",
    dry_run: bool = False,
    verbose: bool = False,
) -> None:
    """Destroy a single snapshot."""
    cmd = ["zfs", "destroy", snapshot.full_name]
    if dry_run or verbose:
        print(f"  [destroy] {shlex.join(cmd)}")
    if dry_run:
        return
    executor.run(cmd)


# Namespace alias so `from zbm import zfs` continues to work in tests
zfs = _types.SimpleNamespace(
    list_datasets=list_datasets,
    list_snapshots=list_snapshots,
    dataset_exists=dataset_exists,
    get_auto_snapshot_property=get_auto_snapshot_property,
    discover_datasets=discover_datasets,
    find_common_snapshot=find_common_snapshot,
    send_incremental=send_incremental,
    destroy_snapshot=destroy_snapshot,
)


# ============================================================
# BACKUP
# ============================================================

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
        return f"zfs send -c {src_dataset}@{first_snap} | ssh {dest} {recv_cmd}"

    return f"zfs send -c {src_dataset}@{first_snap} | {recv_cmd}"


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
    if not dataset_exists(src_dataset, src_executor):
        plan.action = "error"
        plan.message = f"Source dataset does not exist: {src_dataset}"
        return plan

    # Check destination exists
    if not dataset_exists(dst_dataset, dst_executor):
        plan.action = "error"
        plan.message = f"Destination dataset does not exist: {dst_dataset}"
        src_snaps = list_snapshots(src_dataset, src_executor)
        if src_snaps:
            plan.bootstrap_cmd = _format_bootstrap_command(
                src_dataset, src_snaps[0].name, dst_executor, dst_dataset
            )
        return plan

    # List snapshots
    src_snaps = list_snapshots(src_dataset, src_executor)
    dst_snaps = list_snapshots(dst_dataset, dst_executor)

    if verbose:
        print(f"  {src_dataset}: {len(src_snaps)} src, {len(dst_snaps)} dst snapshots")

    if not src_snaps:
        plan.action = "skip"
        plan.message = "Source has no snapshots"
        return plan

    # Find common snapshot
    common = find_common_snapshot(src_snaps, dst_snaps)

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
            if not _confirm(("[dry-run] " if dry_run else "") + "Proceed with rollbacks?"):
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
                send_incremental(
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
            send_incremental(
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
    print(("[dry-run] " if dry_run else "") + "Backup complete.")
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


# ============================================================
# COMPACT
# ============================================================


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
        if not dataset_exists(dst_dataset, dst_executor):
            print(f"\n{dst_dataset}: dataset does not exist, skipping.")
            continue

        snaps = list_snapshots(dst_dataset, dst_executor)
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

    if not no_confirm:
        try:
            answer = input(
                "\n" + ("[dry run] " if dry_run else "") + f"Delete {total} snapshot(s)? [y/N] "
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
                destroy_snapshot(snap, dst_executor, dry_run=dry_run, verbose=verbose)
                deleted += 1
            except ExecutorError as e:
                print(f"  ERROR destroying {snap.full_name}: {e}", file=sys.stderr)
                any_error = True

        print("  " + ("[dry run] " if dry_run else "") + f"Deleted {deleted} of {len(to_delete)} snapshot(s).")

    return 1 if any_error else 0


# ============================================================
# CLI
# ============================================================


def _make_executors(config):
    """Build src (always local) and dst (local or SSH) executors from config."""
    src_exec = LocalExecutor()
    if config.destination.is_remote:
        dst_exec = SSHExecutor(
            host=config.destination.host,
            user=config.destination.user,
            port=config.destination.port,
        )
    else:
        dst_exec = LocalExecutor()
    return src_exec, dst_exec


def cmd_backup(args) -> int:
    try:
        config = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec, dst_exec = _make_executors(config)
    return run_backup(
        config=config,
        src_executor=src_exec,
        dst_executor=dst_exec,
        dry_run=args.dry_run,
        verbose=args.verbose,
        no_confirm=args.no_confirm,
    )


def cmd_compact(args) -> int:
    try:
        config = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    _, dst_exec = _make_executors(config)
    return run_compact(
        config=config,
        dst_executor=dst_exec,
        dry_run=args.dry_run,
        verbose=args.verbose,
        no_confirm=args.no_confirm,
    )


def cmd_status(args) -> int:
    """Show sync state: how many snapshots behind each dataset is."""
    try:
        config = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec, dst_exec = _make_executors(config)
    datasets = config.datasets

    any_error = False
    for src_dataset in datasets:
        dst_dataset = config.destination.dataset_for(src_dataset)
        try:
            src_snaps = list_snapshots(src_dataset, src_exec)
        except ExecutorError:
            print(f"{src_dataset}: ERROR (source dataset not found)")
            any_error = True
            continue
        try:
            dst_snaps = list_snapshots(dst_dataset, dst_exec)
        except ExecutorError:
            print(f"{src_dataset}: DESTINATION MISSING")
            any_error = True
            continue

        common = find_common_snapshot(src_snaps, dst_snaps)

        if common is None:
            status = "NO COMMON SNAPSHOT (needs bootstrap)"
        else:
            common_idx = next(i for i, s in enumerate(src_snaps) if s.name == common.name)
            behind = len(src_snaps) - common_idx - 1
            status = "UP TO DATE" if behind == 0 else f"{behind} snapshot(s) behind"

        print(f"{src_dataset}: {status}")

    return 1 if any_error else 0


def cmd_list(args) -> int:
    """List datasets and snapshot counts on source and destination."""
    try:
        config = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec, dst_exec = _make_executors(config)
    datasets = config.datasets

    col_w = max((len(d) for d in datasets), default=7) + 2
    print(f"{'Dataset':<{col_w}} {'Src snaps':>10} {'Dst snaps':>10}")
    print("-" * (col_w + 22))
    for src_dataset in datasets:
        dst_dataset = config.destination.dataset_for(src_dataset)
        try:
            src_snaps = list_snapshots(src_dataset, src_exec)
            src_count = str(len(src_snaps))
        except ExecutorError:
            src_count = "missing"
        try:
            dst_snaps = list_snapshots(dst_dataset, dst_exec)
            dst_count = str(len(dst_snaps))
        except ExecutorError:
            dst_count = "missing"
        print(f"{src_dataset:<{col_w}} {src_count:>10} {dst_count:>10}")

    return 0


def cmd_discover(args) -> int:
    """Discover datasets with auto-snapshot enabled and print YAML datasets block."""
    try:
        pool = load_source_pool(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec = LocalExecutor()
    datasets = discover_datasets(pool, src_exec)

    if not datasets:
        print(f"No datasets with com.sun:auto-snapshot=true found in pool '{pool}'.",
              file=sys.stderr)
        return 1

    print("datasets:")
    for ds in datasets:
        print(f"  - {ds.name}")

    return 0


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        prog="zbm",
        description="ZFS Backup Manager — sync snapshots between pools",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # Shared options
    def add_common(p):
        p.add_argument("config", help="Path to job YAML config file")
        p.add_argument("--dry-run", "-n", action="store_true",
                       help="Show what would happen without making changes")
        p.add_argument("--verbose", "-v", action="store_true",
                       help="Show detailed command output")
        p.add_argument("--no-confirm", action="store_true",
                       help="Skip confirmation prompts")

    p_backup = sub.add_parser("backup", help="Sync snapshots from source to destination")
    add_common(p_backup)
    p_backup.set_defaults(func=cmd_backup)

    p_compact = sub.add_parser("compact", help="Prune old snapshots on destination per retention rules")
    add_common(p_compact)
    p_compact.set_defaults(func=cmd_compact)

    p_status = sub.add_parser("status", help="Show sync state for each dataset")
    p_status.add_argument("config", help="Path to job YAML config file")
    p_status.set_defaults(func=cmd_status)

    p_list = sub.add_parser("list", help="List datasets and snapshot counts")
    p_list.add_argument("config", help="Path to job YAML config file")
    p_list.set_defaults(func=cmd_list)

    p_discover = sub.add_parser("discover",
        help="Discover datasets with auto-snapshot and print YAML datasets block")
    p_discover.add_argument("config", help="Path to job YAML config file")
    p_discover.set_defaults(func=cmd_discover)

    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
