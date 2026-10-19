"""dbt-duckdb installs extensions right after connecting, before per-cursor settings apply: the
baked extension directory must be in the connect-time config, or dbt downloads extensions from the
internet on every run (found on AWS once egress allowed HTTPS only)."""
import dataclasses

import yaml

from loader import gold, settings


def test_dbt_loads_extensions_only_from_the_baked_directory(tmp_path, monkeypatch):
    s = dataclasses.replace(settings.load(), duckdb_extension_dir="/opt/duckdb-extensions")
    monkeypatch.setattr(s.__class__, "principal", lambda self, name: ("id", "secret"))
    gold.write_profile(s, tmp_path)
    target = yaml.safe_load((tmp_path / "profiles.yml").read_text())["lakehouse_gold"]["outputs"]["local"]
    for key in ("config_options", "settings"):
        assert target[key]["extension_directory"] == "/opt/duckdb-extensions"
        assert target[key]["autoinstall_known_extensions"] is False
