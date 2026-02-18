"""ZFS operations using an Executor for dependency injection."""
from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from zbm.executor import ExecutorError
from zbm.models import Dataset, Snapshot

if TYPE_CHECKING:
    from zbm.executor import Executor


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
    src_dataset: str,
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

    Uses: zfs send -I @common src_dataset@latest | [ssh] zfs recv dst_dataset
    """
    send_cmd = [
        "zfs", "send", "-I",
        f"@{common.name}",
        f"{src_dataset}@{latest.name}",
    ]
    recv_cmd = ["zfs", "recv", dst_dataset]

    if dry_run or verbose:
        import shlex
        print(f"  [send] {shlex.join(send_cmd)}")
        if hasattr(dst_executor, '_ssh_prefix'):
            # SSHExecutor
            print(f"  [recv] ssh ... {shlex.join(recv_cmd)}")
        else:
            print(f"  [recv] {shlex.join(recv_cmd)}")

    if dry_run:
        return

    send_proc = src_executor.popen(send_cmd, stdout=subprocess.PIPE)
    recv_proc = dst_executor.popen(recv_cmd, stdin=send_proc.stdout)
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
        import shlex
        print(f"  [destroy] {shlex.join(cmd)}")
    if dry_run:
        return
    executor.run(cmd)
