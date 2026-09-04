"""Unit tests for the V2 serving-table contract (serving_schema).

PySpark-free, psycopg-free — pure contract/DDL/naming logic.
"""

from __future__ import annotations

import pytest

from traffic_data_elt.databricks.schemas import serving_schema as ss
from traffic_data_elt.databricks.schemas.gold_schema import GOLD_FIELD_NAMES


class TestColumnContract:
    def test_column_count_derived_not_literal(self):
        # SERVING_COLUMN_COUNT must be derived from the contract (no drift).
        assert ss.SERVING_COLUMN_COUNT == len(ss._SERVING_COLUMNS)
        assert ss.SERVING_COLUMN_COUNT == len(ss.SERVING_COLUMN_NAMES)

    def test_column_count_matches_gold_contract(self):
        # The serving contract width equals the Gold contract width.
        assert ss.SERVING_COLUMN_COUNT == len(GOLD_FIELD_NAMES)

    def test_columns_match_gold_order(self):
        assert ss.SERVING_COLUMN_NAMES == GOLD_FIELD_NAMES

    def test_all_columns_not_null(self):
        assert set(ss.SERVING_NOT_NULL_COLUMNS) == set(ss.SERVING_COLUMN_NAMES)

    def test_grain_keys(self):
        assert ss.SERVING_GRAIN_KEYS == ["source_file", "track_id"]

    def test_no_fixture_constants_in_schema(self):
        # Fixture expectations were removed from the runtime schema module; they
        # live in the integration fixture manifest now, not here.
        assert not hasattr(ss, "EXPECTED_ROW_COUNT")
        assert not hasattr(ss, "EXPECTED_FRAME_SUM")

    def test_pg_types(self):
        types = {c: t for c, t, _ in ss._SERVING_COLUMNS}
        assert types["source_file"] == "text"
        assert types["track_id"] == "integer"
        assert types["frame_count"] == "bigint"
        assert types["duration_s"] == "double precision"


class TestSparkToPg:
    def test_known_mappings(self):
        assert ss.spark_to_pg_type("StringType") == "text"
        assert ss.spark_to_pg_type("IntegerType") == "integer"
        assert ss.spark_to_pg_type("LongType") == "bigint"
        assert ss.spark_to_pg_type("DoubleType") == "double precision"

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            ss.spark_to_pg_type("MapType")


class TestIdentifierSafety:
    def test_safe_ident_accepts_valid(self):
        assert ss._safe_ident("run_abc123") == "run_abc123"

    @pytest.mark.parametrize("bad", [
        "a; drop table x", "a-b", "a.b", "a b", "", "a'b", 'a"b', "a/*x*/",
    ])
    def test_safe_ident_rejects_injection(self, bad):
        with pytest.raises(ValueError):
            ss._safe_ident(bad)

    def test_staging_name_rejects_unsafe_run_id(self):
        with pytest.raises(ValueError):
            ss.staging_table_name("bad; drop")


class TestNaming:
    def test_qualified_serving_table(self):
        assert ss.qualified_serving_table() == "serving.gold_trajectory_summary"

    def test_staging_table_name_pattern(self):
        assert ss.staging_table_name("r12345678") == "gold_trajectory_summary__staging_r12345678"

    def test_qualified_staging_table(self):
        assert ss.qualified_staging_table("r1") == "serving.gold_trajectory_summary__staging_r1"

    def test_insert_columns_csv_ordered(self):
        csv = ss.insert_columns_csv()
        assert csv.split(", ") == ss.SERVING_COLUMN_NAMES


class TestDDL:
    def test_create_schema_sql(self):
        assert ss.create_schema_sql() == "CREATE SCHEMA IF NOT EXISTS serving;"

    def test_create_table_sql_has_all_columns_and_pk(self):
        sql = ss.create_table_sql(ss.qualified_serving_table(), with_pk=True)
        for col in ss.SERVING_COLUMN_NAMES:
            assert col in sql
        assert "PRIMARY KEY (source_file, track_id)" in sql
        assert sql.count("NOT NULL") == 19

    def test_create_table_sql_without_pk(self):
        sql = ss.create_table_sql(ss.qualified_serving_table(), with_pk=False)
        assert "PRIMARY KEY" not in sql

    def test_create_staging_table_sql_uses_staging_name(self):
        sql = ss.create_staging_table_sql("r1")
        assert "serving.gold_trajectory_summary__staging_r1" in sql
        assert "PRIMARY KEY (source_file, track_id)" in sql

    def test_drop_staging_table_sql(self):
        sql = ss.drop_staging_table_sql("r1")
        assert sql == "DROP TABLE IF EXISTS serving.gold_trajectory_summary__staging_r1;"


class TestValidationQueries:
    def test_expected_keys_present(self):
        q = ss.validation_queries(ss.qualified_serving_table())
        for key in (
            "row_count", "distinct_grain", "sum_frame_count", "min_frame_count",
            "bad_duration", "bad_distance", "bad_speed", "bad_time_order", "null_grain",
        ):
            assert key in q
            assert ss.qualified_serving_table() in q[key]

    def test_storage_queries_present(self):
        q = ss.storage_queries(ss.qualified_serving_table())
        for key in ("table_bytes", "index_bytes", "total_bytes", "database_bytes"):
            assert key in q
