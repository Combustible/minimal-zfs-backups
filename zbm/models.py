"""Data models for zfs-backup-manager."""
from __future__ import annotations

import re
from dataclasses import dataclass, field


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
