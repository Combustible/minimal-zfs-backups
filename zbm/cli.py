"""CLI entry point for zfs-backup-manager."""
from __future__ import annotations

import argparse
import sys

from zbm.config import ConfigError, load_job, load_source_pool
from zbm.executor import LocalExecutor, SSHExecutor


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
    from zbm.backup import run_backup
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
    from zbm.compact import run_compact
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
    from zbm import zfs
    try:
        config = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec, dst_exec = _make_executors(config)
    datasets = config.datasets

    for src_dataset in datasets:
        dst_dataset = config.destination.dataset_for(src_dataset)
        src_snaps = zfs.list_snapshots(src_dataset, src_exec)
        dst_snaps = zfs.list_snapshots(dst_dataset, dst_exec)
        common = zfs.find_common_snapshot(src_snaps, dst_snaps)

        if common is None:
            status = "NO COMMON SNAPSHOT (needs bootstrap)"
        else:
            common_idx = next(i for i, s in enumerate(src_snaps) if s.name == common.name)
            behind = len(src_snaps) - common_idx - 1
            status = f"UP TO DATE" if behind == 0 else f"{behind} snapshot(s) behind"

        print(f"{src_dataset}: {status}")

    return 0


def cmd_list(args) -> int:
    """List datasets and snapshot counts on source and destination."""
    from zbm import zfs
    try:
        config = load_job(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec, dst_exec = _make_executors(config)
    datasets = config.datasets

    print(f"{'Dataset':<45} {'Src snaps':>10} {'Dst snaps':>10}")
    print("-" * 67)
    for src_dataset in datasets:
        dst_dataset = config.destination.dataset_for(src_dataset)
        src_snaps = zfs.list_snapshots(src_dataset, src_exec)
        try:
            dst_snaps = zfs.list_snapshots(dst_dataset, dst_exec)
            dst_count = str(len(dst_snaps))
        except Exception:
            dst_count = "missing"
        print(f"{src_dataset:<45} {len(src_snaps):>10} {dst_count:>10}")

    return 0


def cmd_discover(args) -> int:
    """Discover datasets with auto-snapshot enabled and print YAML datasets block."""
    from zbm import zfs
    try:
        pool = load_source_pool(args.config)
    except (ConfigError, FileNotFoundError) as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 1

    src_exec = LocalExecutor()
    datasets = zfs.discover_datasets(pool, src_exec)

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
