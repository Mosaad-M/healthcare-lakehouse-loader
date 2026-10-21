"""The incremental dimensions' horizon (loader/gold.py history_horizons): a run applies a source's
batches only up to the earliest batch still on its way to Bronze, so a slow batch is never passed by
a later one, and a batch that lands behind the history already applied fails the run loudly.
No stack: the manifest and the dimension's table are stood in for.
"""
import functools
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from loader import gold
from loader.state import BRONZE_COMMITTED, LANDED, QUARANTINED, SILVER_COMMITTED, Batch
from tests.unit.test_gold_star import A, compiled, create_dim, lake, land, run_dim

NOW = datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc)
DIM = gold.Model("dim_provider", "phi", "", {"history_source": "provider_directory"})


def batch(batch_id: str, state: str, landed: datetime) -> Batch:
    return Batch(batch_id=batch_id, source="provider_directory", state=state, raw_key="k", content_sha256="c",
                 name_sha256="n", size=1, landed_at=landed.isoformat())


@pytest.fixture
def world(monkeypatch):
    """batches: the manifest for the source; applied: the dimension's newest applied landing time."""
    state = {"batches": [], "applied": None}

    class FakeManifest:
        def __init__(self, s):
            pass

        def for_source(self, source):
            assert source == "provider_directory"
            return sorted(state["batches"], key=lambda b: (b.landed_at, b.batch_id))

    class FakeCon:
        def execute(self, sql):
            assert "phi.gold.dim_provider" in sql
            return self

        def fetchone(self):
            return (state["applied"],)

    monkeypatch.setattr("loader.state.Manifest", FakeManifest)
    monkeypatch.setattr(gold.duck, "connect", lambda *a, **k: FakeCon())
    monkeypatch.setenv("LOADER_GOLD_SETTLE_SECONDS", "300")
    return state


def test_with_nothing_in_flight_the_horizon_is_now_less_the_settling_time(world):
    world["batches"] = [batch("a", SILVER_COMMITTED, NOW - timedelta(hours=2))]
    assert gold.history_horizons(None, [DIM], now=NOW) == {"provider_directory": (NOW - timedelta(minutes=5)).isoformat()}


def test_a_batch_still_on_its_way_to_bronze_holds_the_horizon_at_its_landing(world):
    slow = NOW - timedelta(hours=1)
    world["batches"] = [batch("a", BRONZE_COMMITTED, NOW - timedelta(hours=2)), batch("slow", LANDED, slow),
                        batch("later", SILVER_COMMITTED, NOW - timedelta(minutes=30)),
                        batch("bad", QUARANTINED, NOW - timedelta(hours=3))]
    world["applied"] = NOW - timedelta(hours=2)
    assert gold.history_horizons(None, [DIM], now=NOW) == {"provider_directory": slow.isoformat()}


def test_a_batch_in_flight_behind_the_applied_history_fails_the_run(world):
    world["batches"] = [batch("late", LANDED, NOW - timedelta(hours=3))]
    world["applied"] = NOW - timedelta(hours=2)
    with pytest.raises(gold.LateBatch, match="1 batch"):
        gold.history_horizons(None, [DIM], now=NOW)


def test_models_without_a_history_source_get_no_horizon(world):
    assert gold.history_horizons(None, [gold.Model("claims_monthly", "deid", "", {})], now=NOW) == {}


@functools.cache
def _compiled_with(horizon: str | None) -> dict[str, str]:
    with tempfile.TemporaryDirectory() as tmp:
        vars = {"history_horizon": {"provider_directory": horizon}} if horizon else None
        return {m.name: m.sql for m in gold.models(None, _profile(Path(tmp)), vars=vars)}


def _profile(tmp: Path) -> Path:
    import yaml

    profile = {"lakehouse_gold": {"target": "t", "outputs": {"t": {"type": "duckdb", "path": ":memory:"}}}}
    (tmp / "profiles.yml").write_text(yaml.safe_dump(profile))
    return tmp


def test_a_run_stops_at_its_horizon_and_the_next_run_continues(tmp_path):
    """Batches at or after the horizon wait: the run applies what lies before it, and a later run,
    with the slow batch loaded, applies the rest in order, as if nothing had waited."""
    deploy_sql = compiled(tmp_path, deploying=True)
    t1, t2, t3 = NOW - timedelta(hours=3), NOW - timedelta(hours=2), NOW - timedelta(hours=1)
    con = lake()
    create_dim(con, deploy_sql)
    land(con, "b1", t1, {A: ("Clinic A", "cardiology", "MA", True)})
    land(con, "b3", t3, {A: ("Clinic A", "pediatrics", "MA", True)})  # b2 (t2) is still on its way
    run_dim(con, _compiled_with(t2.isoformat()))
    specialties = "SELECT specialty FROM phi.gold.dim_provider ORDER BY valid_from"
    assert [r[0] for r in con.execute(specialties).fetchall()] == ["cardiology"]
    land(con, "b2", t2, {A: ("Clinic A", "oncology", "MA", True)})
    run_dim(con, _compiled_with(None))
    assert [r[0] for r in con.execute(specialties).fetchall()] == ["cardiology", "oncology", "pediatrics"]
