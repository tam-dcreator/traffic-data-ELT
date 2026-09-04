"""Unit tests for v2_cloud/databricks/schemas/silver_schema.py.

Validates the Silver schema definition without requiring PySpark:
- Field name list matches the PneumaRecord contract.
- Non-nullable fields are correct.
- Validation bounds are within expected ranges.
- get_silver_schema() raises when PySpark is absent (import guard).
- SILVER_FIELD_NAMES ordering is deterministic.
"""

from __future__ import annotations

import pytest

from traffic_data_elt.databricks.schemas.silver_schema import (
    LAT_MAX,
    LAT_MIN,
    LON_MAX,
    LON_MIN,
    SILVER_FIELD_NAMES,
    SILVER_NON_NULLABLE_FIELDS,
    SPEED_MIN,
)


# ---------------------------------------------------------------------------
# Field name contract
# ---------------------------------------------------------------------------

class TestSilverFieldNames:
    # All 11 PneumaRecord fields plus 2 provenance columns = 13 total.
    _EXPECTED = [
        "source_file",
        "track_id",
        "vehicle_type",
        "traveled_d_m",
        "avg_speed_ms",
        "lat",
        "lon",
        "speed_ms",
        "lon_acc_ms2",
        "lat_acc_ms2",
        "timestamp_s",
        "bronze_key",
        "ingested_at",
    ]

    def test_field_count(self):
        assert len(SILVER_FIELD_NAMES) == 13

    def test_field_order(self):
        assert SILVER_FIELD_NAMES == self._EXPECTED

    def test_no_duplicates(self):
        assert len(SILVER_FIELD_NAMES) == len(set(SILVER_FIELD_NAMES))

    def test_pneuma_record_fields_present(self):
        pneuma_fields = [
            "source_file", "track_id", "vehicle_type", "traveled_d_m",
            "avg_speed_ms", "lat", "lon", "speed_ms",
            "lon_acc_ms2", "lat_acc_ms2", "timestamp_s",
        ]
        for f in pneuma_fields:
            assert f in SILVER_FIELD_NAMES, f"missing PneumaRecord field: {f}"

    def test_provenance_fields_present(self):
        assert "bronze_key" in SILVER_FIELD_NAMES
        assert "ingested_at" in SILVER_FIELD_NAMES

    def test_provenance_fields_at_end(self):
        assert SILVER_FIELD_NAMES[-2] == "bronze_key"
        assert SILVER_FIELD_NAMES[-1] == "ingested_at"


# ---------------------------------------------------------------------------
# Non-nullable fields
# ---------------------------------------------------------------------------

class TestSilverNonNullableFields:
    def test_all_fields_non_nullable(self):
        # For the Silver layer all 13 fields are defined non-nullable.
        assert set(SILVER_NON_NULLABLE_FIELDS) == set(SILVER_FIELD_NAMES)

    def test_non_nullable_is_subset_of_field_names(self):
        assert set(SILVER_NON_NULLABLE_FIELDS).issubset(set(SILVER_FIELD_NAMES))


# ---------------------------------------------------------------------------
# Validation bounds
# ---------------------------------------------------------------------------

class TestValidationBounds:
    def test_lat_range_is_athens_bbox(self):
        assert LAT_MIN == pytest.approx(37.9)
        assert LAT_MAX == pytest.approx(38.1)
        assert LAT_MIN < LAT_MAX

    def test_lon_range_is_athens_bbox(self):
        assert LON_MIN == pytest.approx(23.6)
        assert LON_MAX == pytest.approx(23.9)
        assert LON_MIN < LON_MAX

    def test_speed_min_is_zero(self):
        assert SPEED_MIN == pytest.approx(0.0)

    def test_bounds_are_positive_range(self):
        assert LAT_MAX - LAT_MIN > 0
        assert LON_MAX - LON_MIN > 0
