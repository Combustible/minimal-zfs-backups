"""Load and validate YAML job configuration files."""
from __future__ import annotations

import yaml

from zbm.models import (
    DestinationConfig,
    JobConfig,
    RetentionRule,
    SourceConfig,
)


class ConfigError(Exception):
    pass


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
    destination = DestinationConfig(
        pool=dst_raw["pool"],
        prefix=dst_raw.get("prefix", "BACKUP"),
        host=dst_raw.get("host"),
        user=dst_raw.get("user"),
        port=int(dst_raw.get("port", 22)),
    )

    # --- datasets ---
    datasets_raw = raw.get("datasets", [])
    discover = bool(raw.get("discover", False))
    if not datasets_raw and not discover:
        raise ConfigError("Either 'datasets' list or 'discover: true' is required")
    datasets = [str(d) for d in datasets_raw]

    # --- compaction ---
    compaction = []
    for rule in raw.get("compaction", []):
        if "pattern" not in rule or "keep" not in rule:
            raise ConfigError("Each compaction rule needs 'pattern' and 'keep'")
        compaction.append(RetentionRule(pattern=rule["pattern"], keep=int(rule["keep"])))

    return JobConfig(
        source=source,
        destination=destination,
        datasets=datasets,
        discover=discover,
        compaction=compaction,
    )
