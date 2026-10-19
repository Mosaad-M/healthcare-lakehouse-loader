"""The maintenance planner: which actions a table's health and policies call for."""

from loader.maintenance import MIN_ORPHAN_AGE, Action, Health, plan

DEFAULTS = {
    "system.data-compaction": {"enable": True, "config": {"target-file-size-bytes": 134217728, "min-input-files": 5,
                                                          "delete-file-threshold": 3}},
    "system.snapshot-expiry": {"enable": True, "config": {"max-snapshot-age-hours": 168, "min-snapshots-to-keep": 5}},
    "system.metadata-compaction": {"enable": True, "config": {"max-manifests": 100}},
    "system.orphan-file-removal": {"enable": True, "max_orphan_file_age_in_days": 3},
}


def health(**changes) -> Health:
    base = dict(data_files=2, delete_files=0, records=1000, total_bytes=2 * 200_000_000, snapshots=3,
                oldest_snapshot_hours=1.0, manifests=3)
    return Health(**{**base, **changes})


def kinds(actions: list[Action]) -> list[str]:
    return [a.kind for a in actions]


def test_a_healthy_table_only_gets_orphan_removal():
    assert kinds(plan(health(), DEFAULTS)) == ["remove_orphans"]


def test_many_small_files_or_delete_files_trigger_compaction():
    assert "compact" in kinds(plan(health(data_files=40, total_bytes=40 * 100_000), DEFAULTS))
    assert "compact" in kinds(plan(health(delete_files=3), DEFAULTS))
    assert "compact" not in kinds(plan(health(delete_files=2), DEFAULTS))


def test_many_manifests_trigger_a_manifest_rewrite():
    assert "rewrite_manifests" in kinds(plan(health(manifests=101), DEFAULTS))


def test_snapshot_expiry_needs_both_count_and_age():
    assert "expire_snapshots" not in kinds(plan(health(snapshots=50, oldest_snapshot_hours=10), DEFAULTS))
    assert "expire_snapshots" not in kinds(plan(health(snapshots=4, oldest_snapshot_hours=500), DEFAULTS))
    assert "expire_snapshots" in kinds(plan(health(snapshots=50, oldest_snapshot_hours=500), DEFAULTS))


def test_disabled_policies_do_nothing():
    off = {k: {**v, "enable": False} for k, v in DEFAULTS.items()}
    assert plan(health(data_files=40, total_bytes=1, delete_files=9, manifests=500, snapshots=99,
                       oldest_snapshot_hours=999), off) == []


def test_the_orphan_window_floor_is_one_day():
    assert MIN_ORPHAN_AGE.days == 1
