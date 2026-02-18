"""Load and validate YAML job configuration files."""
from __future__ import annotations

import re

import yaml

from zbm.models import (
    DestinationConfig,
    JobConfig,
    RetentionRule,
    SourceConfig,
)


class ConfigError(Exception):
    pass


def load_source_pool(path: str) -> str:
    """Load only the source pool name from a config file (for discover)."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ConfigError(f"Config must be a YAML mapping: {path}")
    src_raw = raw.get("source")
    if not src_raw or not src_raw.get("pool"):
        raise ConfigError("source.pool is required")
    return src_raw["pool"]


def load_job(path: str) -> JobConfig:
    with open(path) as f:
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
            raise ConfigError(f"Invalid regex in compaction pattern {pattern!r}: {e}")
        compaction.append(RetentionRule(pattern=pattern, keep=keep))

    return JobConfig(
        source=source,
        destination=destination,
        datasets=datasets,
        compaction=compaction,
    )
