"""Reusable orchestration logic for the V2 production pipeline.

This module holds the pure, unit-testable building blocks the production
Airflow DAG orchestrates, so the DAG file itself stays thin and the logic can
be tested without Airflow installed:

    * Bronze object-name derivation from the source URL (path only)
    * canonical S3 path resolution (delegates to :class:`AwsConfig`)
    * Databricks serverless job submission via the CLI (injectable runner)
    * Airflow → Databricks job-parameter contracts (never carrying a password)
    * Neon production data-plane validation against Gold metrics

No secrets are read or logged here.  The Neon password is provided to
Databricks exclusively via a Databricks secret scope (the notebook reads it);
the loader parameters built here reference the scope/key by name only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from urllib.parse import urlparse

from traffic_data_elt.config import AwsConfig
from traffic_data_elt.databricks.bootstrap import (
    CommandRunner,
    default_command_runner,
    _databricks_argv,
)
from traffic_data_elt.utils import get_logger

log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Bronze object naming (URL path only, query ignored)
# ---------------------------------------------------------------------------


def derive_bronze_object_name(source_url: str) -> str:
    """Return the archive filename from *source_url* using the PATH only.

    Query parameters (e.g. ``?download=1``) are ignored, and the real archive
    filename from the URL path is preserved.

    Example
    -------
    ``derive_bronze_object_name("https://x.com/records/1/file/dataset.zip?download=1")``
    returns ``"dataset.zip"``.

    Raises
    ------
    ValueError
        If no filename can be derived from the URL path.
    """
    if not source_url:
        raise ValueError("source_url is required to derive the Bronze object name")
    path = urlparse(source_url).path
    name = path.rstrip("/").split("/")[-1] if path else ""
    if not name:
        raise ValueError(
            f"cannot derive a Bronze object name from URL path {path!r}"
        )
    return name


# ---------------------------------------------------------------------------
# Resolved production paths
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ResolvedPaths:
    """Fully-resolved, non-secret S3 paths for a production run."""

    bucket: str
    region: str
    bronze_object_name: str
    bronze_key: str            # e.g. bronze/pneuma/pNEUMA_dataset.zip
    bronze_uri: str            # s3://<bucket>/<bronze_key>
    silver_root: str           # s3://<bucket>/silver/pneuma/trajectories
    gold_root: str             # s3://<bucket>/gold/pneuma/trajectory_summary

    def as_dict(self) -> dict[str, str]:
        return {
            "bucket": self.bucket,
            "region": self.region,
            "bronze_object_name": self.bronze_object_name,
            "bronze_key": self.bronze_key,
            "bronze_uri": self.bronze_uri,
            "silver_root": self.silver_root,
            "gold_root": self.gold_root,
        }


def resolve_paths(aws: AwsConfig, *, source_url: str | None = None) -> ResolvedPaths:
    """Resolve the production Bronze/Silver/Gold paths from *aws* config.

    The Bronze object name comes from *source_url* (or ``aws.zenodo_url``); the
    Bronze object lives at ``<bronze-layer>/<bronze-data>/<object-name>``.
    Silver/Gold roots are the canonical dataset roots (no ``/test/``).
    """
    url = source_url or aws.zenodo_url
    object_name = derive_bronze_object_name(url)
    bronze_key = aws.bronze_key(object_name)
    return ResolvedPaths(
        bucket=aws.bucket,
        region=aws.region,
        bronze_object_name=object_name,
        bronze_key=bronze_key,
        bronze_uri=aws.s3_uri(bronze_key),
        silver_root=aws.silver_root(),
        gold_root=aws.gold_root(),
    )


# ---------------------------------------------------------------------------
# Bronze idempotency (head-before-upload)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BronzeObjectState:
    """Existence/size metadata for a Bronze object (no signed URLs / creds)."""

    exists: bool
    size_bytes: int | None = None
    reuse: bool = False
    reason: str = ""


def inspect_bronze_object(
    bucket: str, key: str, *, s3_client=None, region: str | None = None,
    expected_length: int | None = None,
) -> BronzeObjectState:
    """HEAD the Bronze object and decide whether it can be reused.

    Reuse when the object exists with non-zero size and — when *expected_length*
    is known (Content-Length from the source) — the sizes match.  Never logs
    signed URLs or credentials.

    A retry of the ingest task must not re-transfer ~15 GB when a valid object
    already exists.
    """
    import boto3  # noqa: PLC0415
    from botocore.exceptions import ClientError  # noqa: PLC0415

    client = s3_client or boto3.client("s3", region_name=region)
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return BronzeObjectState(exists=False, reason="absent")
        raise
    size = int(head.get("ContentLength", 0))
    if size <= 0:
        return BronzeObjectState(exists=True, size_bytes=size, reuse=False,
                                 reason="zero-size object; will re-upload")
    if expected_length is not None and expected_length > 0 and size != expected_length:
        return BronzeObjectState(
            exists=True, size_bytes=size, reuse=False,
            reason=f"size {size} != expected {expected_length}; will re-upload",
        )
    return BronzeObjectState(exists=True, size_bytes=size, reuse=True,
                             reason="valid existing object; reused")


# ---------------------------------------------------------------------------
# Databricks serverless job submission (CLI, injectable runner)
# ---------------------------------------------------------------------------


def build_notebook_job_json(
    run_name: str,
    notebook_path: str,
    base_parameters: dict[str, str],
) -> dict:
    """Build a serverless single-notebook ``databricks jobs submit`` payload."""
    return {
        "run_name": run_name,
        "tasks": [
            {
                "task_key": run_name.replace(" ", "_")[:100],
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "base_parameters": base_parameters,
                },
                "environment_key": "default",
            }
        ],
        "environments": [
            {"environment_key": "default", "spec": {"client": "3"}}
        ],
    }


class DatabricksJobError(RuntimeError):
    """Raised when a Databricks job submission or run fails."""


def submit_notebook_job(
    profile: str,
    run_name: str,
    notebook_path: str,
    base_parameters: dict[str, str],
    *,
    runner: CommandRunner = default_command_runner,
) -> str:
    """Submit a serverless notebook job and return its ``run_id`` (as str).

    Uses ``databricks jobs submit --json <payload> --no-wait``.  Raises
    :class:`DatabricksJobError` on submission failure.
    """
    payload = build_notebook_job_json(run_name, notebook_path, base_parameters)
    res = runner(
        _databricks_argv(profile, "jobs", "submit", "--json", json.dumps(payload),
                         "--no-wait")
    )
    if not res.ok:
        raise DatabricksJobError(f"job submit failed: {res.stderr.strip()}")
    try:
        run_id = json.loads(res.stdout)["run_id"]
    except (json.JSONDecodeError, KeyError) as exc:
        raise DatabricksJobError(
            f"could not parse run_id from submit output: {exc}"
        ) from exc
    return str(run_id)


def get_run_state(
    profile: str, run_id: str, *, runner: CommandRunner = default_command_runner,
) -> tuple[str, str | None]:
    """Return ``(life_cycle_state, result_state)`` for a Databricks run."""
    res = runner(_databricks_argv(profile, "jobs", "get-run", str(run_id)))
    if not res.ok:
        raise DatabricksJobError(f"get-run failed: {res.stderr.strip()}")
    try:
        state = json.loads(res.stdout).get("state", {})
    except json.JSONDecodeError as exc:
        raise DatabricksJobError(f"could not parse run state: {exc}") from exc
    return state.get("life_cycle_state", ""), state.get("result_state")


# ---------------------------------------------------------------------------
# Airflow → Databricks parameter contracts (NO password)
# ---------------------------------------------------------------------------

# Keys that must NEVER appear in job parameters passed to Databricks.
_FORBIDDEN_PARAM_KEYS = frozenset({
    "NEON_DB_PASSWORD", "NEON_API_KEY", "DATABRICKS_TOKEN",
    "AWS_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID",
})


def _assert_no_secrets(params: dict[str, str]) -> dict[str, str]:
    leaked = sorted(k for k in params if k in _FORBIDDEN_PARAM_KEYS)
    if leaked:
        raise ValueError(f"refusing to pass secret parameters to Databricks: {leaked}")
    return params


def silver_job_params(
    wheel_path: str, bucket: str, bronze_key: str, silver_output_path: str,
    *, uc_catalog: str, uc_schema: str, uc_volume: str,
) -> dict[str, str]:
    """Build Silver notebook parameters (production: no fixture counts)."""
    return _assert_no_secrets({
        "WHEEL_PATH": wheel_path,
        "S3_BUCKET": bucket,
        "BRONZE_KEY": bronze_key,
        "SILVER_OUTPUT_PATH": silver_output_path,
        "UC_CATALOG": uc_catalog,
        "UC_SCHEMA": uc_schema,
        "UC_VOLUME": uc_volume,
    })


def gold_job_params(
    wheel_path: str, silver_input_path: str, gold_output_path: str,
) -> dict[str, str]:
    """Build Gold notebook parameters (production: no fixture counts)."""
    return _assert_no_secrets({
        "WHEEL_PATH": wheel_path,
        "SILVER_INPUT_PATH": silver_input_path,
        "GOLD_OUTPUT_PATH": gold_output_path,
    })


def serving_job_params(
    wheel_path: str, gold_input_path: str, *,
    neon_host: str, neon_port: str, neon_db: str, neon_user: str, neon_sslmode: str,
    neon_secret_scope: str, neon_secret_key: str, neon_branch: str,
    load_mode: str = "replace_sources", copy_batch_size: str = "10000",
    allow_production_write: bool = False,
) -> dict[str, str]:
    """Build serving (Gold→Neon) notebook parameters.

    The password is NEVER included — the notebook reads it from the Databricks
    secret scope/key referenced here by name only.  Production writes require
    ``allow_production_write=True`` (mapped to ``ALLOW_PRODUCTION_WRITE``).
    """
    return _assert_no_secrets({
        "WHEEL_PATH": wheel_path,
        "GOLD_INPUT_PATH": gold_input_path,
        "NEON_DB_HOST": neon_host,
        "NEON_DB_PORT": neon_port,
        "NEON_DB_NAME": neon_db,
        "NEON_DB_USER": neon_user,
        "NEON_DB_SSLMODE": neon_sslmode,
        "NEON_SECRET_SCOPE": neon_secret_scope,
        "NEON_SECRET_KEY": neon_secret_key,
        "NEON_BRANCH": neon_branch,
        "LOAD_MODE": load_mode,
        "NEON_COPY_BATCH_SIZE": copy_batch_size,
        "ALLOW_PRODUCTION_WRITE": "true" if allow_production_write else "false",
    })


# ---------------------------------------------------------------------------
# Neon production validation (data plane, dynamic — no fixture counts)
# ---------------------------------------------------------------------------


@dataclass
class NeonProductionValidation:
    """Result of validating Neon serving against Gold metrics."""

    neon_row_count: int
    neon_frame_sum: int
    distinct_grain: int
    gold_row_count: int
    gold_frame_sum: int
    passed_checks: list[str] = field(default_factory=list)
    failed_checks: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.failed_checks


def evaluate_neon_production(
    *, neon_row_count: int, neon_frame_sum: int, distinct_grain: int,
    gold_row_count: int, gold_frame_sum: int,
) -> NeonProductionValidation:
    """Dynamically validate Neon serving against Gold (production gates).

    No fixture-specific expected counts — only runtime relationships:
      * Neon row_count == Gold trajectory_count
      * Neon SUM(frame_count) == Gold SUM(frame_count)
      * grain unique (distinct_grain == neon_row_count)
      * row_count > 0
    """
    result = NeonProductionValidation(
        neon_row_count=neon_row_count, neon_frame_sum=neon_frame_sum,
        distinct_grain=distinct_grain, gold_row_count=gold_row_count,
        gold_frame_sum=gold_frame_sum,
    )

    def check(name: str, ok: bool) -> None:
        (result.passed_checks if ok else result.failed_checks).append(name)

    check("neon_rows_positive", neon_row_count > 0)
    check("neon_rows_eq_gold", neon_row_count == gold_row_count)
    check("neon_frames_eq_gold", neon_frame_sum == gold_frame_sum)
    check("grain_unique", distinct_grain == neon_row_count)
    return result
