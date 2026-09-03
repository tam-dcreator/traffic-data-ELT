"""Unit tests for v2_cloud/databricks/gold_parity.py.

Pure-Python parity comparison logic — no Spark, PostgreSQL, or AWS required.
Covers:
- exact matching on categorical/count fields
- float tolerance on metric fields
- duplicate-key rejection
- missing-key detection on either side
- row-count comparison
"""

from __future__ import annotations

import pytest

from v2_cloud.databricks.gold_parity import (
    compare_trajectory_summaries,
    index_rows,
)


def _row(source_file="pnemas.csv", track_id=1, **overrides):
    base = {
        "source_file": source_file,
        "track_id": track_id,
        "vehicle_type": "car",
        "frame_count": 100,
        "start_time_s": 0.0,
        "end_time_s": 4.0,
        "duration_s": 4.0,
        "traveled_d_m": 48.85,
        "avg_speed_ms": 9.77,
        "max_speed_ms": 12.5,
        "min_speed_ms": 0.0,
        "avg_lon_acc_ms2": 0.01,
        "avg_lat_acc_ms2": -0.02,
        "max_lon_acc_ms2": 1.2,
        "max_lat_acc_ms2": 0.9,
        "start_lat": 37.977391,
        "start_lon": 23.737688,
        "end_lat": 37.978100,
        "end_lon": 23.738000,
    }
    base.update(overrides)
    return base


class TestIndexRows:
    def test_indexes_by_grain_key(self):
        idx = index_rows([_row(track_id=1), _row(track_id=2)])
        assert set(idx) == {("pnemas.csv", 1), ("pnemas.csv", 2)}

    def test_duplicate_key_raises(self):
        with pytest.raises(ValueError):
            index_rows([_row(track_id=1), _row(track_id=1)])

    def test_same_track_id_different_file_is_distinct(self):
        idx = index_rows([
            _row(source_file="a.csv", track_id=1),
            _row(source_file="b.csv", track_id=1),
        ])
        assert len(idx) == 2


class TestComparePassing:
    def test_identical_rows_pass(self):
        rows = [_row(track_id=1), _row(track_id=2)]
        result = compare_trajectory_summaries(rows, [dict(r) for r in rows])
        assert result.passed, result.summary()
        assert result.compared_keys == 2

    def test_float_within_tolerance_passes(self):
        v1 = [_row(duration_s=4.0)]
        v2 = [_row(duration_s=4.0 + 5e-7)]  # below 1e-6 tolerance
        result = compare_trajectory_summaries(v1, v2)
        assert result.passed, result.summary()

    def test_summary_says_passed(self):
        rows = [_row()]
        result = compare_trajectory_summaries(rows, [dict(rows[0])])
        assert "PASSED" in result.summary()


class TestCompareFailing:
    def test_float_beyond_tolerance_fails(self):
        v1 = [_row(duration_s=4.0)]
        v2 = [_row(duration_s=4.01)]  # 1e-2 >> tolerance
        result = compare_trajectory_summaries(v1, v2)
        assert not result.passed
        assert any("duration_s" in m for m in result.field_mismatches)

    def test_categorical_mismatch_fails(self):
        v1 = [_row(vehicle_type="car")]
        v2 = [_row(vehicle_type="taxi")]
        result = compare_trajectory_summaries(v1, v2)
        assert not result.passed
        assert any("vehicle_type" in m for m in result.field_mismatches)

    def test_frame_count_mismatch_fails_exactly(self):
        v1 = [_row(frame_count=100)]
        v2 = [_row(frame_count=101)]
        result = compare_trajectory_summaries(v1, v2)
        assert not result.passed
        assert any("frame_count" in m for m in result.field_mismatches)

    def test_frame_count_int_vs_float_integral_equal(self):
        # A bigint from Postgres vs a long-as-float should still match.
        v1 = [_row(frame_count=100)]
        v2 = [_row(frame_count=100.0)]
        result = compare_trajectory_summaries(v1, v2)
        assert result.passed, result.summary()

    def test_missing_in_v2_detected(self):
        v1 = [_row(track_id=1), _row(track_id=2)]
        v2 = [_row(track_id=1)]
        result = compare_trajectory_summaries(v1, v2)
        assert not result.passed
        assert ("pnemas.csv", 2) in result.missing_in_v2

    def test_missing_in_v1_detected(self):
        v1 = [_row(track_id=1)]
        v2 = [_row(track_id=1), _row(track_id=9)]
        result = compare_trajectory_summaries(v1, v2)
        assert not result.passed
        assert ("pnemas.csv", 9) in result.missing_in_v1

    def test_row_count_mismatch_fails(self):
        v1 = [_row(track_id=1), _row(track_id=2)]
        v2 = [_row(track_id=1)]
        result = compare_trajectory_summaries(v1, v2)
        assert result.v1_row_count == 2
        assert result.v2_row_count == 1
        assert not result.passed


class TestToleranceOverride:
    def test_custom_tolerance_applied(self):
        v1 = [_row(duration_s=4.0)]
        v2 = [_row(duration_s=4.001)]
        # Default tolerance fails; a looser explicit tolerance passes.
        assert not compare_trajectory_summaries(v1, v2).passed
        assert compare_trajectory_summaries(
            v1, v2, float_tolerance=1e-2
        ).passed
