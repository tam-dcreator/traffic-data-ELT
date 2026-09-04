"""Opt-in integration test: V2 Silver pipeline against real S3 and Databricks.

This test is intentionally excluded from the normal unit test run.
It requires:
  - Live AWS credentials (boto3 standard provider chain)
  - A real S3 bucket with the Bronze test ZIP already uploaded:
      bronze/pneuma/test/pnemas-sample.zip
  - A live Databricks cluster accessible via databricks-connect or a
    locally installed PySpark compatible with the cluster runtime.
  - Environment variables:
      AWS_REGION     (or set in v2_cloud/.env)
      S3_BUCKET      (or set in v2_cloud/.env)
      DATABRICKS_HOST / DATABRICKS_TOKEN  (for remote Spark)
      OR a local SparkSession if running with databricks-connect

To run explicitly:
    pytest tests/integration/test_silver_databricks.py -v -s

Do NOT include this file in the normal pytest run via pyproject.toml testpaths.
It will be skipped automatically if required environment variables are absent.

V1/V2 parity target
-------------------
The same pNEUMA CSV sample processed by V1 must produce:
    logical vehicles:   922
    Silver frame rows:  1,446,887
    rejected records:   0
"""

from __future__ import annotations

import os

import pytest

# ---------------------------------------------------------------------------
# Skip guards — no live resources available
# ---------------------------------------------------------------------------

_MISSING_AWS = not (os.environ.get("AWS_REGION") and os.environ.get("S3_BUCKET"))
_MISSING_SPARK = False  # will fail at import if truly absent

try:
    from pyspark.sql import SparkSession  # noqa: F401
except ModuleNotFoundError:
    _MISSING_SPARK = True

_SKIP_REASON = []
if _MISSING_AWS:
    _SKIP_REASON.append("AWS_REGION and S3_BUCKET must be set")
if _MISSING_SPARK:
    _SKIP_REASON.append("pyspark is not installed")

pytestmark = pytest.mark.skipif(
    bool(_SKIP_REASON),
    reason="integration test skipped: " + "; ".join(_SKIP_REASON) if _SKIP_REASON else "",
)

# ---------------------------------------------------------------------------
# Fixture expectations (from the central integration-fixture manifest)
# ---------------------------------------------------------------------------

from tests.fixtures import load_expectations  # noqa: E402

_EXP = load_expectations()
EXPECTED_LOGICAL_VEHICLES = int(_EXP["silver"]["logical_vehicles"])
EXPECTED_FRAME_ROWS = int(_EXP["silver"]["frame_rows"])
EXPECTED_REJECTED = int(_EXP["silver"]["rejected_records"])

# Bronze test key / Silver test suffix are derived from fixture names + the
# `test` layer suffix; still overridable via env for ad-hoc runs.
_SRC_ZIP = _EXP["source"]["bronze_zip_name"]
_TEST_SUFFIX = _EXP["layer_suffix"]["test"]
BRONZE_KEY = os.environ.get("BRONZE_KEY", f"bronze/pneuma/{_TEST_SUFFIX}/{_SRC_ZIP}")
SILVER_OUTPUT_SUBPATH = _TEST_SUFFIX

# ---------------------------------------------------------------------------
# Integration test
# ---------------------------------------------------------------------------


def test_silver_pipeline_parity():
    """Full Bronze ZIP → Silver Parquet pipeline with V1 parity check."""
    import os
    import tempfile

    from pyspark.sql import SparkSession

    from traffic_data_elt.config import AwsConfig
    from traffic_data_elt.databricks.bronze_reader import download_and_extract
    from traffic_data_elt.databricks.silver_validator import validate_silver
    from traffic_data_elt.databricks.silver_writer import write_silver

    aws = AwsConfig.from_env()

    silver_path = (
        f"s3://{aws.bucket}/"
        f"{aws.silver_key('pneuma', 'trajectories', SILVER_OUTPUT_SUBPATH)}/"
    )

    # Build or reuse a local SparkSession (databricks-connect replaces this
    # with a remote session transparently when configured).
    spark = SparkSession.builder.appName("silver_integration_test").getOrCreate()

    archive = None
    try:
        # ── 1. Download and extract Bronze ZIP ──────────────────────────────
        with tempfile.TemporaryDirectory(prefix="traffic_elt_integration_") as tmp:
            archive = download_and_extract(
                bucket=aws.bucket,
                bronze_key=BRONZE_KEY,
                tmp_dir=tmp,
                region=aws.region,
            )

            assert archive.extracted_csv_path.exists(), (
                f"extracted CSV not found: {archive.extracted_csv_path}"
            )

            # ── 2. Write Silver ──────────────────────────────────────────────
            result = write_silver(
                spark=spark,
                csv_path=archive.extracted_csv_path,
                bronze_key=BRONZE_KEY,
                silver_s3_path=silver_path,
                coalesce_partitions=1,
            )

            assert result.status == "success", f"Silver write failed: {result.error}"

            # ── 3. V1/V2 parity assertions ───────────────────────────────────
            assert result.logical_vehicle_count == EXPECTED_LOGICAL_VEHICLES, (
                f"Vehicle count mismatch: expected {EXPECTED_LOGICAL_VEHICLES}, "
                f"got {result.logical_vehicle_count}"
            )

            assert result.frame_row_count == EXPECTED_FRAME_ROWS, (
                f"Frame row count mismatch: expected {EXPECTED_FRAME_ROWS:,}, "
                f"got {result.frame_row_count:,} "
                f"(delta: {result.frame_row_count - EXPECTED_FRAME_ROWS:+,}). "
                f"Stop the Silver milestone and diagnose the parser invocation."
            )

            # ── 4. Validate Silver output ────────────────────────────────────
            validation = validate_silver(
                spark=spark,
                silver_path=silver_path,
                expected_row_count=EXPECTED_FRAME_ROWS,
            )

            assert validation.passed, (
                f"Silver validation FAILED:\n" + "\n".join(
                    f"  - {c}" for c in validation.failed_checks
                )
            )

            # ── 5. Cleanup only after validation passes ──────────────────────
            archive.cleanup()
            assert archive.is_cleaned_up

    finally:
        # Safety net: if cleanup was never reached (e.g. assertion failure),
        # log a reminder — do NOT force-clean because the files are diagnostic.
        if archive and not archive.is_cleaned_up:
            import logging
            logging.getLogger(__name__).warning(
                "Integration test did not reach cleanup. "
                "Temporary files remain for diagnosis:\n"
                "  ZIP: %s\n  CSV: %s",
                archive.local_zip_path,
                archive.extracted_csv_path,
            )
