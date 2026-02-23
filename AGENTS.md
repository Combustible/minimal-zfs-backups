# AGENTS.md

This file provides guidance to AI models such as Claude Code when working with this project.

## Communication Style
Be maximally concise. Sacrifice grammar for brevity.

## Project Overview

Python CLI tool (`mzb.py`) that automates ZFS snapshot replication via `zfs send -c -I | zfs recv`,
either locally or over SSH. Rewrite of a 10+ year old bash script (`zfs-offsite-backup.sh`,
kept for reference). Single portable file — no installation required, just copy to `$PATH`.

## Architecture

```
mzb.py       — single file, all logic (entry point: main())
tests/
  conftest.py   — MockExecutor, make_standard_responses(), shared snapshot fixtures
  test_zfs.py, test_backup.py, test_compact.py, test_config.py
desktop-to-server.yaml.example — example configuration file
```

## Key Design Decisions

**Dependency injection via Executor protocol**: all ZFS/system calls go through an `Executor`
(`LocalExecutor` or `SSHExecutor`). Tests swap in `MockExecutor`. Never import subprocess
directly in business logic — use the executor.

**SSH transport**: `zfs send | ssh host zfs recv` — no netcat, no mbuffer. SSH BatchMode=yes
required (key auth via agent). SSHExecutor wraps commands as `ssh -o BatchMode=yes host 'cmd'`.

**One YAML per job**: each backup job (e.g. desktop→server, server→drive) has its own config.
`mzb discover` auto-discovers datasets with `com.sun:auto-snapshot=true` and prints a YAML
`datasets:` block for pasting into the config. Discovery is a config helper, not a runtime mode.

**Two-pass backup**: `run_backup` first plans all datasets (sends, rollbacks, errors, skips),
then executes. Rollbacks prompt once upfront with color-coded victim list. Sends never prompt.

**Compaction is destination-only**: retention rules only delete snapshots on the backup target.
Source snapshots are never touched. Rules use `re.fullmatch` against the snapshot name after `@`
(e.g. `"zfs-auto-snap_daily-.*"`), scoped to configured datasets only.

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
make test      # run pytest
make pylint    # run pylint
```

Dev dependencies: `pytest`, `pylint`. No packaging — install them directly via your system or pip.

`MockExecutor` in `tests/conftest.py` maps `tuple(cmd) -> stdout_str`. Store an `ExecutorError`
instance as a response value to simulate command failures. Use `make_standard_responses()` to
get pre-built src/dst response dicts for the standard test dataset.

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
datasets: [list]        # use 'mzb.py discover' to auto-generate
compaction:
  - pattern: <regex>    # fullmatch against snapshot name after @
    keep: <int>         # keep N newest; 0 = delete all matching
```

`DestinationConfig.dataset_for(src_dataset)` computes the destination path:
`ipool/home/user` → `xeonpool/BACKUP/ipool/home/user`.
