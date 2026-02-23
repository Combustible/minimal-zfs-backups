# minimalist-zfs-backups

Minimalist Python tool `mzb.py` for automating ZFS snapshot replication between
pools, locally or over SSH. Calls `zfs send -c -I | zfs recv` for efficient,
compressed incremental transfers.

Many projects exist which help automate transfers of ZFS snapshots between
pools, locally and over SSH. They can do what this project does, and more!

However, these other projects also have a lot of features to "help" the user do
complicated data management tasks. This makes them complicated to set up, and
intimidating for a paranoid system administrator who is concerned with their
precious data.

This tool exists for those who want exactly these two things, and nothing else:
- Push incremental snapshots from a local pool to another, optionally over SSH
- Keep only some number of snapshots on the destination pool that match a
  user-configurable regex.

This tool will never create, delete, or forcibly overwrite datasets, you must
do this yourself. It has a `--dry-run` mode for everything. If configured per
[the hardening guide](HARDENING.md), it is restricted by the operating system
from performing any operation outside of this scope.

[mzb.py](mzb.py) is a single ~1000-line Python file that is stand-alone, easily
understood, and is entirely portable (it can be executed directly with no
installation required).

## Requirements

- Python 3.10+
- ZFS utilities (`zfs`, `zpool`) on both source and destination machines
- Python `pyyaml` package - i.e. `apt install python3-pyyaml` on many
  debian-based distros

## Install

Copy `mzb.py` into your "$PATH".

See the [the hardening guide](HARDENING.md) for guidance on configuring your
system securely, avoiding `root` and `sudo` entirely, as well as restricting
automation to only access the specific pools/datasets it is meant to.

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

Use `mzb.py discover job.yaml` to auto-discover datasets with `com.sun:auto-snapshot=true`
and print a `datasets:` block you can paste into your config.

## Usage

```bash
# Discover datasets for config bootstrapping
mzb.py discover job.yaml

# Show sync state (read-only)
mzb.py status  job.yaml

# Preview what would be sent (no writes)
mzb.py backup  job.yaml --dry-run --verbose
mzb.py compact job.yaml --dry-run --verbose

# Run for real
mzb.py backup  job.yaml
mzb.py compact job.yaml

# Skip confirmation prompts (for cron/automation)
mzb.py backup  job.yaml --no-confirm
```

### Subcommands

| Command | Description |
|---------|-------------|
| `backup` | Send new snapshots from source to destination |
| `compact` | Prune snapshots on destination per retention rules |
| `status` | Show how many snapshots each dataset is behind |
| `discover` | Print `datasets:` YAML block from auto-snapshot property |

## Safety

- Never runs dangerous operations including `zfs recv -F`, prompts you to run those commands if they are needed.
- Never deletes datasets
- Compaction only touches destination snapshots
- Prompts before any `zfs destroy pool/dataset@snapshot` (bypass with `--no-confirm`)
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

For automated/unattended use, see **[HARDENING.md](HARDENING.md)** for how to
set up a dedicated non-root user with scoped ZFS permissions and a restricted
SSH key — no root access or `sudo` required.

## Bootstrap (first sync)

If a destination dataset doesn't exist yet, `mzb.py backup` will print the
required command:

```bash
zfs send -c ipool/home/user@first-snap | ssh root@server zfs recv -F xeonpool/BACKUP/ipool/home/user
```

Once you run the bootstrap command yourself, then subsequent `mzb.py backup`
runs handle incremental updates.

## Development

```bash
make test      # run tests
make pylint    # run pylint
```

The tests require `pytest` to be installed. Tests use `MockExecutor` to
simulate ZFS command output — no actual ZFS required. Tests do not interact
with zfs data in any way - there are no tests that actually run `zfs` on your
system. They only validate that the script logic is internally consistent.
