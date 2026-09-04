"""Unit tests for the pure helpers in traffic_data_elt.databricks.neon_loader.

No psycopg, no PySpark, no live Neon — these dependencies are imported lazily
inside load_gold_to_neon / measure_storage, so the module imports and the pure
helpers run without them.
"""

from __future__ import annotations

import datetime

import pytest

from traffic_data_elt.databricks.neon_loader import (
    DEFAULT_COPY_BATCH_SIZE,
    LOAD_MODE_REPLACE_SNAPSHOT,
    LOAD_MODE_REPLACE_SOURCES,
    VALID_LOAD_MODES,
    NeonLoadResult,
    build_insert_sql,
    bytes_per_trajectory,
    copy_sql,
    evaluate_validation,
    iter_row_batches,
    project_full_storage,
)
from traffic_data_elt.databricks.schemas import serving_schema as ss

_NOW = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

# Fixture expectations live in the manifest; tests use them as explicit inputs.
_FIX_ROWS = 922
_FIX_FRAMES = 1_446_887


def _good_scalars(**over):
    base = {
        "row_count": _FIX_ROWS,
        "distinct_grain": _FIX_ROWS,
        "sum_frame_count": _FIX_FRAMES,
        "min_frame_count": 81,
        "bad_duration": 0,
        "bad_distance": 0,
        "bad_speed": 0,
        "bad_time_order": 0,
        "null_grain": 0,
    }
    base.update(over)
    return base


class TestLoadModes:
    def test_default_is_replace_sources(self):
        assert LOAD_MODE_REPLACE_SOURCES == "replace_sources"

    def test_snapshot_mode_exists(self):
        assert LOAD_MODE_REPLACE_SNAPSHOT == "replace_snapshot"

    def test_valid_modes(self):
        assert set(VALID_LOAD_MODES) == {"replace_sources", "replace_snapshot"}


class TestIterRowBatches:
    def test_exact_multiple(self):
        rows = [(i,) for i in range(20)]
        batches = list(iter_row_batches(rows, 10))
        assert [len(b) for b in batches] == [10, 10]

    def test_remainder_batch(self):
        rows = [(i,) for i in range(25)]
        batches = list(iter_row_batches(rows, 10))
        assert [len(b) for b in batches] == [10, 10, 5]

    def test_smaller_than_batch(self):
        rows = [(i,) for i in range(3)]
        assert [len(b) for b in iter_row_batches(rows, 10)] == [3]

    def test_empty(self):
        assert list(iter_row_batches([], 10)) == []

    def test_all_rows_preserved_and_ordered(self):
        rows = [(i,) for i in range(37)]
        flat = [r for b in iter_row_batches(rows, 8) for r in b]
        assert flat == rows

    def test_generator_input_bounded(self):
        # A generator is consumed lazily; batches must still be correct.
        gen = ((i,) for i in range(15))
        assert [len(b) for b in iter_row_batches(gen, 6)] == [6, 6, 3]

    def test_zero_batch_size_raises(self):
        with pytest.raises(ValueError):
            list(iter_row_batches([(1,)], 0))

    def test_default_batch_size_positive(self):
        assert DEFAULT_COPY_BATCH_SIZE > 0


class TestSql:
    def test_build_insert_sql(self):
        sql = build_insert_sql(ss.qualified_serving_table())
        assert sql.startswith("INSERT INTO serving.gold_trajectory_summary (")
        assert sql.count("%s") == 19

    def test_copy_sql(self):
        sql = copy_sql(ss.qualified_serving_table())
        assert sql.startswith("COPY serving.gold_trajectory_summary (")
        assert "FROM STDIN" in sql
        for c in ss.SERVING_COLUMN_NAMES:
            assert c in sql


class TestEvaluateValidationInvariants:
    """Invariants must hold regardless of fixture expectations (production mode)."""

    def test_invariants_pass_without_expectations(self):
        ok, detail = evaluate_validation(_good_scalars())
        assert ok
        # No fixture expectation checks present in production mode.
        assert "expected_row_count" not in detail
        assert "expected_frame_sum" not in detail

    def test_row_count_positive_required(self):
        ok, detail = evaluate_validation(_good_scalars(row_count=0, distinct_grain=0))
        assert not ok
        assert detail["row_count_positive"][1] is False

    def test_grain_uniqueness_required(self):
        ok, detail = evaluate_validation(_good_scalars(distinct_grain=900))
        assert not ok
        assert detail["grain_unique"][1] is False

    def test_frame_count_positive_required(self):
        ok, detail = evaluate_validation(_good_scalars(min_frame_count=0))
        assert not ok
        assert detail["frame_count_positive"][1] is False

    def test_bad_duration(self):
        ok, detail = evaluate_validation(_good_scalars(bad_duration=1))
        assert not ok and detail["duration_non_negative"][1] is False

    def test_bad_distance(self):
        ok, detail = evaluate_validation(_good_scalars(bad_distance=1))
        assert not ok and detail["distance_non_negative"][1] is False

    def test_bad_speed(self):
        ok, detail = evaluate_validation(_good_scalars(bad_speed=1))
        assert not ok and detail["speed_non_negative"][1] is False

    def test_bad_time_order(self):
        ok, detail = evaluate_validation(_good_scalars(bad_time_order=1))
        assert not ok and detail["time_order"][1] is False

    def test_null_grain(self):
        ok, detail = evaluate_validation(_good_scalars(null_grain=1))
        assert not ok and detail["grain_not_null"][1] is False

    def test_large_batch_passes_invariants(self):
        # A dataset far larger than the fixture must still pass invariants.
        big = _good_scalars(row_count=500_000, distinct_grain=500_000,
                            sum_frame_count=999_999_999, min_frame_count=1)
        ok, _ = evaluate_validation(big)
        assert ok


class TestEvaluateValidationFixtureExpectations:
    def test_expected_values_pass(self):
        ok, detail = evaluate_validation(
            _good_scalars(),
            expected_row_count=_FIX_ROWS,
            expected_frame_sum=_FIX_FRAMES,
        )
        assert ok
        assert detail["expected_row_count"][1] is True
        assert detail["expected_frame_sum"][1] is True

    def test_expected_row_count_mismatch_fails(self):
        ok, detail = evaluate_validation(
            _good_scalars(row_count=900, distinct_grain=900),
            expected_row_count=_FIX_ROWS,
        )
        assert not ok
        assert detail["expected_row_count"][1] is False

    def test_expected_frame_sum_mismatch_fails(self):
        ok, detail = evaluate_validation(
            _good_scalars(sum_frame_count=1),
            expected_frame_sum=_FIX_FRAMES,
        )
        assert not ok
        assert detail["expected_frame_sum"][1] is False


class TestStorageMath:
    def test_bytes_per_trajectory(self):
        assert bytes_per_trajectory(262144, 922) == 262144 / 922

    def test_bytes_per_trajectory_zero_rows(self):
        assert bytes_per_trajectory(262144, 0) == 0.0

    def test_project_full_storage_linear(self):
        assert project_full_storage(262144, 922, 500_000) == round(262144 / 922 * 500_000)

    def test_project_full_storage_zero_rows(self):
        assert project_full_storage(262144, 0) == 0

    def test_project_default_target_is_500k(self):
        assert project_full_storage(922, 922) == 500_000


class TestNeonLoadResult:
    def _res(self, sum_fc):
        return NeonLoadResult(
            serving_table="serving.gold_trajectory_summary",
            staging_table="serving.gold_trajectory_summary__staging_r1",
            run_id="r1",
            load_mode=LOAD_MODE_REPLACE_SOURCES,
            staged_row_count=_FIX_ROWS,
            published_row_count=_FIX_ROWS,
            sum_frame_count=sum_fc,
            distinct_grain=_FIX_ROWS,
            source_files=["pnemas.csv"],
            start_time=_NOW,
            end_time=_NOW,
            status="success",
        )

    def test_frames_conserved_true(self):
        assert self._res(_FIX_FRAMES).frames_conserved(_FIX_FRAMES) is True

    def test_frames_conserved_false(self):
        assert self._res(1_000_000).frames_conserved(_FIX_FRAMES) is False
