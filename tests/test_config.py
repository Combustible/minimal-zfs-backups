"""Tests for mzb.config module."""
from __future__ import annotations

import textwrap

import pytest

from mzb import ConfigError, load_job


def _write_config(tmp_path, yaml_text: str) -> str:
    p = tmp_path / "job.yaml"
    p.write_text(textwrap.dedent(yaml_text))
    return str(p)


def _minimal_yaml(**overrides) -> str:
    """Return a valid minimal config, with optional section overrides."""
    sections = {
        "source": "source:\n  pool: ipool",
        "destination": "destination:\n  pool: xeonpool\n  prefix: BACKUP",
        "datasets": "datasets:\n  - ipool/home/user",
        "compaction": "",
    }
    sections.update(overrides)
    return "\n".join(v for v in sections.values() if v)


class TestLoadJobValid:
    def test_minimal(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml())
        config = load_job(path)
        assert config.source.pool == "ipool"
        assert config.destination.pool == "xeonpool"
        assert config.datasets == ["ipool/home/user"]

    def test_with_compaction(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(
            compaction="compaction:\n  - pattern: 'zfs-auto-snap_daily-.*'\n    keep: 7"
        ))
        config = load_job(path)
        assert len(config.compaction) == 1
        assert config.compaction[0].keep == 7


class TestLoadJobInvalid:
    def test_negative_keep(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(
            compaction="compaction:\n  - pattern: '.*'\n    keep: -1"
        ))
        with pytest.raises(ConfigError, match="keep.*>= 0"):
            load_job(path)

    def test_invalid_regex(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(
            compaction="compaction:\n  - pattern: '[unclosed'\n    keep: 1"
        ))
        with pytest.raises(ConfigError, match="Invalid regex"):
            load_job(path)

    def test_null_dataset(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(
            datasets="datasets:\n  - ~"
        ))
        with pytest.raises(ConfigError, match="Invalid dataset"):
            load_job(path)

    def test_empty_string_dataset(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(
            datasets="datasets:\n  - ''"
        ))
        with pytest.raises(ConfigError, match="Invalid dataset"):
            load_job(path)

    def test_empty_prefix(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(
            destination="destination:\n  pool: xeonpool\n  prefix: ''"
        ))
        with pytest.raises(ConfigError, match="prefix must not be empty"):
            load_job(path)

    def test_missing_source_pool(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(source="source:\n  pool:"))
        with pytest.raises(ConfigError, match="source.pool"):
            load_job(path)

    def test_missing_datasets(self, tmp_path):
        path = _write_config(tmp_path, _minimal_yaml(datasets="datasets: []"))
        with pytest.raises(ConfigError, match="datasets"):
            load_job(path)
