# minimalist-zfs-backups

Python tool for automating ZFS snapshot replication between pools, locally or over SSH.

Replaces a legacy bash script. Uses `zfs send -c -I | zfs recv` for efficient, compressed incremental transfers.

## Requirements

- Python 3.10+
- ZFS utilities (`zfs`, `zpool`) on both source and destination machines
- SSH agent with key access to any remote destination

## Install

```bash
# Optionally, create a virtual environment
python3 -m venv .venv && source .venv/bin/activate
# Install to virtual environment, or system environment if not using virtualenv
pip3 install -e .
```

This installs the `mzb` command into the active environment.

## Configuration

Each backup job is a YAML file. See `desktop-to-server.yaml.example` for a full example.

```yaml
source:
  pool: ipool                  # always local

destination:
  pool: xeonpool
  prefix: BACKUP               # dest datasets land at xeonpool/BACKUP/ipool/...
  host: server.local           # omit for local destination
  user: root                   # omit to use current SSH user
  port: 22

datasets:                      # explicit list of datasets to back up
  - ipool/home/user
  - ipool/noble
  - ipool/windows

compaction:                    # retention rules applied to destination only
  - pattern: "zfs-auto-snap_frequent-.*"
    keep: 0                    # delete all matching snapshots
  - pattern: "zfs-auto-snap_hourly-.*"
    keep: 4
  - pattern: "zfs-auto-snap_daily-.*"
    keep: 14
  - pattern: "zfs-auto-snap_weekly-.*"
    keep: 8
  - pattern: "zfs-auto-snap_monthly-.*"
    keep: 24
```

Compaction patterns use regex fullmatch — they must match the entire snapshot name after `@`.

Use `mzb discover job.yaml` to auto-discover datasets with `com.sun:auto-snapshot=true`
and print a `datasets:` block you can paste into your config.

## Usage

```bash
# Discover datasets for config bootstrapping
mzb discover job.yaml

# Show sync state (read-only)
mzb status  job.yaml
mzb list    job.yaml

# Preview what would be sent (no writes)
mzb backup  job.yaml --dry-run --verbose
mzb compact job.yaml --dry-run --verbose

# Run for real
mzb backup  job.yaml
mzb compact job.yaml

# Skip confirmation prompts (for cron/automation)
mzb backup  job.yaml --no-confirm
```

### Subcommands

| Command | Description |
|---------|-------------|
| `backup` | Send new snapshots from source to destination |
| `compact` | Prune snapshots on destination per retention rules |
| `status` | Show how many snapshots each dataset is behind |
| `list` | Show snapshot counts for source and destination |
| `discover` | Print `datasets:` YAML block from auto-snapshot property |

## Safety

- Never runs dangerous operations including `zfs recv -F`, prompts you to run those commands if they are needed.
- Never deletes datasets
- Compaction only touches destination snapshots
- Prompts before any `zfs destroy` (bypass with `--no-confirm`)
- If destination needs a rollback, shows which snapshots will be removed (color-coded) and prompts before proceeding
- If no common snapshot exists, prints bootstrap commands and skips

## Remote destinations

The tool runs only on the source machine. For remote destinations it pipes over SSH:

```
zfs send -c -I pool/dataset@common pool/dataset@latest | ssh user@host zfs recv dest
```

The `-c` flag sends blocks in their on-disk compressed form, avoiding a
needless decompress/recompress cycle and reducing bytes transferred over
the wire. It is a safe no-op when the source dataset is uncompressed.

Requires SSH BatchMode (key-based auth via agent, no password prompts).

For automated/unattended use, see **[HARDENING.md](HARDENING.md)** for how to
set up a dedicated non-root user with scoped ZFS permissions and a restricted
SSH key — no root access or `sudo` required.

## Bootstrap (first sync)

If a destination dataset doesn't exist yet, `mzb backup` will print the required command:

```bash
zfs send -c ipool/home/user@first-snap | ssh root@server zfs recv xeonpool/BACKUP/ipool/home/user
```

Before receiving, set desired properties on the destination dataset
(e.g. compression, atime, readonly, `com.sun:auto-snapshot=false`).

Run the bootstrap command manually, then subsequent `mzb backup` runs handle incremental updates.

## Development

```bash
pip install -e ".[dev]"
pytest             # run tests
pytest -v          # verbose output
```

Tests use `MockExecutor` to simulate ZFS command output — no actual ZFS required.
