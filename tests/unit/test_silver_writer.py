"""Unit tests for v2_cloud/databricks/silver_writer.py.

Tests pure-Python logic that does NOT require PySpark:
- _records_to_rows_batched: tuple shape, field order, provenance columns,
  batch boundary behaviour, empty input
- _parse_csv: parser invocation on a real temp CSV, vehicle count, frame count

The write_silver() function itself requires a live SparkSession and is
therefore covered by the integration test only.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest

from traffic_data_elt.extract.pneuma import PneumaRecord
from v2_cloud.databricks.silver_writer import (
    _parse_csv,
    _records_to_rows_batched,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_INGESTED_AT = datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
_BRONZE_KEY = "bronze/pneuma/test/pnemas-sample.zip"

_HEADER = (
    "track_id; type; traveled_d; avg_speed; lat; lon; speed; lon_acc; lat_acc; time\n"
)
_F1 = "37.977391; 23.737688; 4.9178; 0.0518; -0.0299; 0.000000"
_F2 = "37.977391; 23.737688; 5.0000; 0.0000;  0.0000; 0.040000"


def _make_record(track_id: int = 1) -> PneumaRecord:
    return PneumaRecord(
        source_file="pnemas.csv",
        track_id=track_id,
        vehicle_type="Car",
        traveled_d_m=48.85,
        avg_speed_ms=9.77,
        lat=37.977391,
        lon=23.737688,
        speed_ms=4.9178,
        lon_acc_ms2=0.0518,
        lat_acc_ms2=-0.0299,
        timestamp_s=0.0,
    )


def _write_csv(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "test.csv"
    p.write_text(_HEADER + body, encoding="utf-8")
    return p


def _two_frame_row(track_id: int = 1) -> str:
    return f"{track_id}; Car; 48.85; 9.77; {_F1}; {_F2};\n"


# ---------------------------------------------------------------------------
# _records_to_rows_batched
# ---------------------------------------------------------------------------

class TestRecordsToRowsBatched:
    def test_empty_input_yields_nothing(self):
        rows = list(_records_to_rows_batched(
            [], bronze_key=_BRONZE_KEY, ingested_at=_INGESTED_AT
        ))
        assert rows == []

    def test_single_record_shape(self):
        record = _make_record(track_id=42)
        rows = list(_records_to_rows_batched(
            [record], bronze_key=_BRONZE_KEY, ingested_at=_INGESTED_AT
        ))
        assert len(rows) == 1
        row = rows[0]
        # 11 PneumaRecord fields + 2 provenance = 13 total
        assert len(row) == 13

    def test_field_order_matches_silver_schema(self):
        from v2_cloud.databricks.schemas.silver_schema import SILVER_FIELD_NAMES

        record = _make_record()
        rows = list(_records_to_rows_batched(
            [record], bronze_key=_BRONZE_KEY, ingested_at=_INGESTED_AT
        ))
        row = rows[0]

        # Map positions by SILVER_FIELD_NAMES order
        idx = {name: i for i, name in enumerate(SILVER_FIELD_NAMES)}

        assert row[idx["source_file"]] == record.source_file
        assert row[idx["track_id"]] == record.track_id
        assert row[idx["vehicle_type"]] == record.vehicle_type
        assert row[idx["traveled_d_m"]] == pytest.approx(record.traveled_d_m)
        assert row[idx["lat"]] == pytest.approx(record.lat)
        assert row[idx["lon"]] == pytest.approx(record.lon)
        assert row[idx["speed_ms"]] == pytest.approx(record.speed_ms)
        assert row[idx["lon_acc_ms2"]] == pytest.approx(record.lon_acc_ms2)
        assert row[idx["lat_acc_ms2"]] == pytest.approx(record.lat_acc_ms2)
        assert row[idx["timestamp_s"]] == pytest.approx(record.timestamp_s)
        assert row[idx["bronze_key"]] == _BRONZE_KEY
        assert row[idx["ingested_at"]] == _INGESTED_AT

    def test_provenance_columns_applied_to_all_rows(self):
        records = [_make_record(i) for i in range(5)]
        rows = list(_records_to_rows_batched(
            records, bronze_key="bk/test.zip", ingested_at=_INGESTED_AT
        ))
        assert len(rows) == 5
        for row in rows:
            assert row[-2] == "bk/test.zip"    # bronze_key is second-to-last
            assert row[-1] == _INGESTED_AT      # ingested_at is last

    def test_multiple_records_correct_count(self):
        records = [_make_record(i) for i in range(100)]
        rows = list(_records_to_rows_batched(
            records, bronze_key=_BRONZE_KEY, ingested_at=_INGESTED_AT
        ))
        assert len(rows) == 100

    def test_batch_boundary_all_rows_emitted(self):
        """Rows crossing a batch boundary must all be emitted."""
        records = [_make_record(i) for i in range(7)]
        rows = list(_records_to_rows_batched(
            records,
            bronze_key=_BRONZE_KEY,
            ingested_at=_INGESTED_AT,
            batch_size=3,       # forces multiple batches (3 + 3 + 1)
        ))
        assert len(rows) == 7


# ---------------------------------------------------------------------------
# _parse_csv
# ---------------------------------------------------------------------------

class TestParseCsv:
    def test_single_vehicle_two_frames(self, tmp_path):
        csv_path = _write_csv(tmp_path, _two_frame_row(1))
        records, vehicle_count, rejected = _parse_csv(csv_path, "test.csv")
        assert len(records) == 2
        assert vehicle_count == 1
        assert rejected == 0

    def test_two_vehicles(self, tmp_path):
        body = _two_frame_row(1) + _two_frame_row(2)
        csv_path = _write_csv(tmp_path, body)
        records, vehicle_count, rejected = _parse_csv(csv_path, "test.csv")
        assert len(records) == 4
        assert vehicle_count == 2

    def test_source_file_stamped_on_records(self, tmp_path):
        csv_path = _write_csv(tmp_path, _two_frame_row(1))
        records, _, _ = _parse_csv(csv_path, "my_source.csv")
        for r in records:
            assert r.source_file == "my_source.csv"

    def test_row_limit_respected(self, tmp_path):
        body = _two_frame_row(1) + _two_frame_row(2) + _two_frame_row(3)
        csv_path = _write_csv(tmp_path, body)
        records, vehicle_count, _ = _parse_csv(csv_path, "test.csv", row_limit=1)
        assert vehicle_count == 1
        assert all(r.track_id == 1 for r in records)

    def test_empty_csv_no_records(self, tmp_path):
        csv_path = tmp_path / "empty.csv"
        csv_path.write_text(_HEADER, encoding="utf-8")
        records, vehicle_count, rejected = _parse_csv(csv_path, "empty.csv")
        assert records == []
        assert vehicle_count == 0


# ---------------------------------------------------------------------------
# AwsConfig.silver_key() — added as part of this milestone
# ---------------------------------------------------------------------------

class TestSilverKey:
    """Regression tests for AwsConfig.silver_key() added in task 1."""

    @pytest.fixture
    def cfg(self):
        from traffic_data_elt.config import AwsConfig
        return AwsConfig(region="eu-central-1", bucket="b", silver_prefix="silver")

    def test_simple_silver_key(self, cfg):
        assert cfg.silver_key("pneuma", "trajectories", "test") == (
            "silver/pneuma/trajectories/test"
        )

    def test_normalises_slashes(self, cfg):
        assert cfg.silver_key("pneuma/", "/test/") == "silver/pneuma/test"

    def test_custom_prefix(self):
        from traffic_data_elt.config import AwsConfig
        cfg = AwsConfig(region="r", bucket="b", silver_prefix="lake/silver")
        assert cfg.silver_key("a.parquet") == "lake/silver/a.parquet"

    def test_empty_raises(self):
        from traffic_data_elt.config import AwsConfig
        cfg = AwsConfig(region="r", bucket="b", silver_prefix="")
        with pytest.raises(ValueError):
            cfg.silver_key("")
