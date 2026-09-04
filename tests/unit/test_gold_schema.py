"""Unit tests for v2_cloud/databricks/schemas/gold_schema.py.

Validates the Gold trajectory-summary schema contract without PySpark:
- Field names/order match the V1 int_vehicle_trajectory_summary output.
- Grain keys, non-nullable fields, float/exact field classification.
- Rounding constants mirror V1 staging.
- FLOAT_TOLERANCE is tight relative to input granularity.
"""

from __future__ import annotations

import pytest

from traffic_data_elt.databricks.schemas.gold_schema import (
    COORD_ROUND_DP,
    FLOAT_TOLERANCE,
    GOLD_EXACT_FIELDS,
    GOLD_FIELD_NAMES,
    GOLD_FLOAT_FIELDS,
    GOLD_GRAIN_KEYS,
    GOLD_NON_NULLABLE_FIELDS,
    KINEMATIC_ROUND_DP,
)

# The exact 19-column contract from V1 int_vehicle_trajectory_summary.
_EXPECTED = [
    "source_file",
    "track_id",
    "vehicle_type",
    "frame_count",
    "start_time_s",
    "end_time_s",
    "duration_s",
    "traveled_d_m",
    "avg_speed_ms",
    "max_speed_ms",
    "min_speed_ms",
    "avg_lon_acc_ms2",
    "avg_lat_acc_ms2",
    "max_lon_acc_ms2",
    "max_lat_acc_ms2",
    "start_lat",
    "start_lon",
    "end_lat",
    "end_lon",
]


class TestGoldFieldNames:
    def test_field_count_is_19(self):
        assert len(GOLD_FIELD_NAMES) == 19

    def test_field_order_matches_v1_contract(self):
        assert GOLD_FIELD_NAMES == _EXPECTED

    def test_no_duplicates(self):
        assert len(GOLD_FIELD_NAMES) == len(set(GOLD_FIELD_NAMES))


class TestGrainKeys:
    def test_grain_is_source_file_and_track_id(self):
        assert GOLD_GRAIN_KEYS == ["source_file", "track_id"]

    def test_grain_keys_are_gold_fields(self):
        for k in GOLD_GRAIN_KEYS:
            assert k in GOLD_FIELD_NAMES


class TestNonNullable:
    def test_all_fields_non_nullable(self):
        assert set(GOLD_NON_NULLABLE_FIELDS) == set(GOLD_FIELD_NAMES)


class TestFieldClassification:
    def test_float_and_exact_partition_all_fields(self):
        assert set(GOLD_FLOAT_FIELDS) | set(GOLD_EXACT_FIELDS) == set(GOLD_FIELD_NAMES)

    def test_float_and_exact_are_disjoint(self):
        assert set(GOLD_FLOAT_FIELDS).isdisjoint(set(GOLD_EXACT_FIELDS))

    def test_frame_count_is_exact(self):
        assert "frame_count" in GOLD_EXACT_FIELDS

    def test_categorical_fields_are_exact(self):
        assert "source_file" in GOLD_EXACT_FIELDS
        assert "track_id" in GOLD_EXACT_FIELDS
        assert "vehicle_type" in GOLD_EXACT_FIELDS

    def test_metric_fields_are_float(self):
        for f in ("duration_s", "avg_speed_ms", "max_lon_acc_ms2", "start_lat"):
            assert f in GOLD_FLOAT_FIELDS


class TestRoundingConstants:
    def test_coord_rounding_matches_v1_staging(self):
        assert COORD_ROUND_DP == 6

    def test_kinematic_rounding_matches_v1_staging(self):
        assert KINEMATIC_ROUND_DP == 4


class TestFloatTolerance:
    def test_tolerance_is_positive(self):
        assert FLOAT_TOLERANCE > 0

    def test_tolerance_tighter_than_input_granularity(self):
        # 1e-6 must be far below the coarsest rounding step (1e-4 kinematics).
        assert FLOAT_TOLERANCE <= 1e-6
        assert FLOAT_TOLERANCE < 10 ** (-KINEMATIC_ROUND_DP)


class TestLazySchemaImportGuard:
    def test_get_gold_schema_requires_pyspark(self):
        import sys

        if "pyspark" in sys.modules:
            pytest.skip("pyspark present (real or stubbed) in this environment")
        from traffic_data_elt.databricks.schemas.gold_schema import get_gold_schema

        with pytest.raises(ModuleNotFoundError):
            get_gold_schema()
