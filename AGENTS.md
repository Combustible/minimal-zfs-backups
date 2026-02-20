# CLAUDE.md

This file provides guidance to AI models such as Claude Code when working with this project.

## Communication Style
Be maximally concise. Sacrifice grammar for brevity.

## Project Overview

Python CLI tool (`zbm`) that automates ZFS snapshot replication via `zfs send -I | zfs recv`,
either locally or over SSH. Rewrite of a 10+ year old bash script (`zfs-offsite-backup.sh`,
kept for reference). Entry point: `zbm/cli.py`.

## Architecture

```
zbm/
  models.py    — dataclasses: Snapshot, Dataset, JobConfig, RetentionRule, SourceConfig, DestinationConfig
  executor.py  — Executor protocol + LocalExecutor + SSHExecutor (dependency injection)
  config.py    — YAML job config loader/validator
  zfs.py       — ZFS operations (list_snapshots, find_common_snapshot, send_incremental, destroy_snapshot, ...)
  backup.py    — backup orchestration (two-pass plan/execute with _DatasetPlan, run_backup)
  compact.py   — retention/compaction logic (run_compact, _snapshots_to_delete)
  cli.py       — argparse subcommands: backup, compact, status, list, discover
tests/
  conftest.py  — MockExecutor, realistic snapshot fixtures from real pool data
  test_zfs.py, test_backup.py, test_compact.py
desktop-to-server.yaml.example — An example configuration file
```

## Key Design Decisions

**Dependency injection via Executor protocol**: all ZFS/system calls go through an `Executor`
(`LocalExecutor` or `SSHExecutor`). Tests swap in `MockExecutor`. Never import subprocess
directly in business logic — use the executor.

**SSH transport**: `zfs send | ssh host zfs recv` — no netcat, no mbuffer. SSH BatchMode=yes
required (key auth via agent). SSHExecutor wraps commands as `ssh -o BatchMode=yes host 'cmd'`.

**One YAML per job**: each backup job (e.g. desktop→server, server→drive) has its own config.
`zbm discover` auto-discovers datasets with `com.sun:auto-snapshot=true` and prints a YAML
`datasets:` block for pasting into the config. Discovery is a config helper, not a runtime mode.

**Two-pass backup**: `run_backup` first plans all datasets (sends, rollbacks, errors, skips),
then executes. Rollbacks prompt once upfront with color-coded victim list. Sends never prompt.

**Compaction is destination-only**: retention rules only delete snapshots on the backup target.
Source snapshots are never touched. Rules use `re.fullmatch` against the full qualified snapshot
name (`dataset@pattern`), scoped to configured datasets only.

**send_incremental uses `-I`**: sends all intermediate snapshots between common and latest in
one stream using fully qualified snapshot names. Snapshot names (after `@`) are matched across
pools to find common point.

## Absolute Safety Rules (never violate)

- Never `zfs recv -F` in commands executed by the program (no force-overwrite). They are allowed (and required) in bootstrap commands printed for the user to run.
- Never delete datasets (`zfs destroy pool/dataset`)
- Never touch source snapshots
- If rollback needed: show color-coded victim list, prompt user, run `zfs rollback -r dest@common`
- If no common snapshot: print bootstrap `zfs send src@first | [ssh] zfs recv dest`, skip
- Prompt user before any `zfs destroy` unless `--no-confirm`
- Abort entire dataset on any send/recv error; continue to next dataset

## Development

```bash
pip install -e ".[dev]"
pytest -v
```

`MockExecutor` in `tests/conftest.py` maps `tuple(cmd) -> stdout_str`. Raise an `ExecutorError`
instance as a response value to simulate command failures.

Fixtures in `conftest.py` (`SRC_USER_SNAPS`, `DST_USER_SNAPS`) use real snapshot names
from the user's pool for realistic testing.

## Config Schema Summary

```yaml
source:
  pool: <local pool name>
destination:
  pool: <pool>
  prefix: BACKUP        # dest path = pool/prefix/src_dataset
  host: <hostname>      # omit for local
  user: <ssh user>
  port: 22
datasets: [list]        # use 'zbm discover' to auto-generate
compaction:
  - pattern: <regex>    # fullmatch against snapshot name after @
    keep: <int>         # keep N newest; 0 = delete all matching
```

`DestinationConfig.dataset_for(src_dataset)` computes the destination path:
`ipool/home/user` → `xeonpool/BACKUP/ipool/home/user`.
