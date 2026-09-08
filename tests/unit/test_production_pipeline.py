"""Unit tests for traffic_data_elt.databricks.production_pipeline.

No live Databricks / S3 / Neon. Covers Bronze object-name derivation (query
ignored), path resolution, Databricks job submission via injected runner, the
no-password parameter contracts, and dynamic Neon production validation.
"""

from __future__ import annotations

import json

import pytest

from traffic_data_elt.config import AwsConfig
from traffic_data_elt.databricks import production_pipeline as pp
from traffic_data_elt.databricks.bootstrap import CommandResult


class FakeRunner:
    def __init__(self, result: CommandResult):
        self.calls: list[list[str]] = []
        self.result = result

    def __call__(self, cmd: list[str]) -> CommandResult:
        self.calls.append(cmd)
        return self.result


def _aws() -> AwsConfig:
    return AwsConfig(region="us-east-2", bucket="traffic-data-v2-use2")


# ── derive_bronze_object_name ───────────────────────────────────────────────────

class TestDeriveBronzeObjectName:
    def test_ignores_query(self):
        assert pp.derive_bronze_object_name(
            "https://zenodo.org/records/1/files/pNEUMA_dataset.zip?download=1"
        ) == "pNEUMA_dataset.zip"

    def test_plain_path(self):
        assert pp.derive_bronze_object_name("https://x.com/a/b/archive.zip") == "archive.zip"

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            pp.derive_bronze_object_name("")

    def test_no_filename_raises(self):
        with pytest.raises(ValueError):
            pp.derive_bronze_object_name("https://x.com/")


# ── resolve_paths ───────────────────────────────────────────────────────────────

class TestResolvePaths:
    def test_production_roots(self):
        rp = pp.resolve_paths(_aws(), source_url="https://x.com/f/pNEUMA_dataset.zip?d=1")
        assert rp.bronze_key == "bronze/pneuma/pNEUMA_dataset.zip"
        assert rp.bronze_uri == "s3://traffic-data-v2-use2/bronze/pneuma/pNEUMA_dataset.zip"
        assert rp.silver_root == "s3://traffic-data-v2-use2/silver/pneuma/trajectories"
        assert rp.gold_root == "s3://traffic-data-v2-use2/gold/pneuma/trajectory_summary"

    def test_no_test_segment(self):
        rp = pp.resolve_paths(_aws(), source_url="https://x.com/f/a.zip")
        for v in rp.as_dict().values():
            assert "/test/" not in str(v)


# ── job submission ──────────────────────────────────────────────────────────────

class TestSubmitNotebookJob:
    def test_returns_run_id(self):
        r = FakeRunner(CommandResult(0, json.dumps({"run_id": 12345}), ""))
        run_id = pp.submit_notebook_job("DEFAULT", "job", "/nb", {"A": "b"}, runner=r)
        assert run_id == "12345"
        # payload passed as JSON with --no-wait
        assert any("--no-wait" in c for c in r.calls)

    def test_submit_failure_raises(self):
        r = FakeRunner(CommandResult(1, "", "boom"))
        with pytest.raises(pp.DatabricksJobError):
            pp.submit_notebook_job("DEFAULT", "job", "/nb", {}, runner=r)

    def test_get_run_state(self):
        r = FakeRunner(CommandResult(
            0, json.dumps({"state": {"life_cycle_state": "TERMINATED",
                                     "result_state": "SUCCESS"}}), ""))
        assert pp.get_run_state("DEFAULT", "1", runner=r) == ("TERMINATED", "SUCCESS")


class TestJobJson:
    def test_serverless_shape(self):
        payload = pp.build_notebook_job_json("nm", "/nb", {"K": "V"})
        assert payload["tasks"][0]["notebook_task"]["notebook_path"] == "/nb"
        assert payload["tasks"][0]["notebook_task"]["base_parameters"] == {"K": "V"}
        assert payload["environments"][0]["spec"]["client"] == "3"


# ── parameter contracts (no password) ───────────────────────────────────────────

class TestJobParams:
    def test_serving_has_no_password(self):
        params = pp.serving_job_params(
            "wheel", "s3://b/gold",
            neon_host="h", neon_port="5432", neon_db="d", neon_user="u",
            neon_sslmode="require", neon_secret_scope="v2-neon",
            neon_secret_key="db-password", neon_branch="production",
            allow_production_write=True,
        )
        assert "NEON_DB_PASSWORD" not in params
        assert params["NEON_SECRET_SCOPE"] == "v2-neon"
        assert params["ALLOW_PRODUCTION_WRITE"] == "true"
        assert params["LOAD_MODE"] == "replace_sources"

    def test_allow_production_defaults_false(self):
        params = pp.serving_job_params(
            "w", "g", neon_host="h", neon_port="5432", neon_db="d", neon_user="u",
            neon_sslmode="require", neon_secret_scope="s", neon_secret_key="k",
            neon_branch="dev",
        )
        assert params["ALLOW_PRODUCTION_WRITE"] == "false"

    def test_silver_params_no_fixture_counts(self):
        params = pp.silver_job_params(
            "w", "bkt", "bronze/pneuma/a.zip", "s3://b/silver",
            uc_catalog="workspace", uc_schema="default", uc_volume="v2_temp",
        )
        assert "EXPECTED_FRAME_ROWS" not in params
        assert "EXPECTED_ROW_COUNT" not in params

    def test_gold_params_no_fixture_counts(self):
        params = pp.gold_job_params("w", "s3://b/silver", "s3://b/gold")
        assert not any(k.startswith("EXPECTED_") for k in params)

    def test_secret_guard_rejects_password_key(self):
        with pytest.raises(ValueError):
            pp._assert_no_secrets({"NEON_DB_PASSWORD": "x"})


# ── Neon production validation (dynamic) ─────────────────────────────────────────

class TestEvaluateNeonProduction:
    def test_pass(self):
        v = pp.evaluate_neon_production(
            neon_row_count=100, neon_frame_sum=5000, distinct_grain=100,
            gold_row_count=100, gold_frame_sum=5000)
        assert v.passed

    def test_row_mismatch_fails(self):
        v = pp.evaluate_neon_production(
            neon_row_count=99, neon_frame_sum=5000, distinct_grain=99,
            gold_row_count=100, gold_frame_sum=5000)
        assert not v.passed
        assert "neon_rows_eq_gold" in v.failed_checks

    def test_frame_mismatch_fails(self):
        v = pp.evaluate_neon_production(
            neon_row_count=100, neon_frame_sum=4999, distinct_grain=100,
            gold_row_count=100, gold_frame_sum=5000)
        assert "neon_frames_eq_gold" in v.failed_checks

    def test_non_unique_grain_fails(self):
        v = pp.evaluate_neon_production(
            neon_row_count=100, neon_frame_sum=5000, distinct_grain=98,
            gold_row_count=100, gold_frame_sum=5000)
        assert "grain_unique" in v.failed_checks

    def test_empty_fails(self):
        v = pp.evaluate_neon_production(
            neon_row_count=0, neon_frame_sum=0, distinct_grain=0,
            gold_row_count=0, gold_frame_sum=0)
        assert "neon_rows_positive" in v.failed_checks


# ── inspect_bronze_object (idempotency) ─────────────────────────────────────────

class TestInspectBronzeObject:
    def test_reuse_valid_object(self):
        class FakeS3:
            def head_object(self, Bucket, Key):
                return {"ContentLength": 100}
        state = pp.inspect_bronze_object("b", "k", s3_client=FakeS3(), expected_length=100)
        assert state.exists and state.reuse

    def test_size_mismatch_reupload(self):
        class FakeS3:
            def head_object(self, Bucket, Key):
                return {"ContentLength": 50}
        state = pp.inspect_bronze_object("b", "k", s3_client=FakeS3(), expected_length=100)
        assert state.exists and not state.reuse

    def test_absent_object(self):
        from botocore.exceptions import ClientError

        class FakeS3:
            def head_object(self, Bucket, Key):
                raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        state = pp.inspect_bronze_object("b", "k", s3_client=FakeS3())
        assert not state.exists and not state.reuse
