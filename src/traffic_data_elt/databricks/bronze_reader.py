"""Bronze archive reader for V2 Databricks / Spark processing.

Responsibility
--------------
This module handles everything between S3 Bronze and the shared pNEUMA parser:

1. Locate the Bronze ZIP object on S3.
2. Copy it into the configured Unity Catalog managed volume run directory.
3. Inspect the ZIP contents.
4. Extract the target pNEUMA CSV into the run directory.
5. Expose the extracted file path to the caller (``silver_writer``).
6. Provide explicit, observable cleanup after Silver validation succeeds.

Temporary storage
-----------------
All working files are placed under a UC-managed volume path::

    /Volumes/<catalog>/<schema>/<volume>/runs/<run_id>/
    ├── source/
    │   └── <archive>.zip
    └── extracted/
        └── pnemas.csv

The volume path is configured via environment variables (see :func:`uc_volume_path`)
or supplied directly via the ``volume_base_path`` parameter.

This directory is **not** part of the durable architecture.  The caller
invokes :meth:`BronzeArchive.cleanup` only after Silver has been written and
validated, which removes both the ZIP and the extracted CSV and their
containing run directory.

Design constraints
------------------
- Extracted CSVs are **never** written back to S3 Bronze.
- Cleanup is caller-controlled so a failed run leaves files for diagnosis.
- Archive handling is separate from parser logic.
- ``tmp_dir`` parameter (legacy/testing) overrides the UC volume path so
  existing tests continue to pass without modification.
"""

from __future__ import annotations

import os
import uuid
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import boto3

from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

log = get_logger(__name__)

# A copy function fetches an S3 object to a local (POSIX) destination path.
# Signature: (bucket, key, dest_path) -> None
CopyFn = Callable[[str, str, str], None]

# Default UC volume path components — overridden by environment variables or
# the ``volume_base_path`` / ``tmp_dir`` parameters at call time.
_DEFAULT_UC_CATALOG = "workspace"
_DEFAULT_UC_SCHEMA = "default"
_DEFAULT_UC_VOLUME = "v2_temp"


class BronzeReaderError(RuntimeError):
    """Raised when the Bronze reader cannot complete a required step."""


# ---------------------------------------------------------------------------
# Copy-function adapters
# ---------------------------------------------------------------------------


def _boto3_copy_fn(client: "S3Client") -> CopyFn:
    """Return a copy function that downloads via a boto3 S3 client.

    Used for local development and the opt-in integration test where boto3
    credentials are available through the standard provider chain.
    """
    def _copy(bucket: str, key: str, dest_path: str) -> None:
        client.download_file(bucket, key, dest_path)

    return _copy


def dbutils_copy_fn(dbutils) -> CopyFn:  # noqa: ANN001 - dbutils is untyped
    """Return a copy function that uses ``dbutils.fs.cp`` for S3 access.

    On Databricks serverless compute boto3 has no credentials.  ``dbutils.fs``
    reads S3 through the Unity Catalog external-location / storage credential,
    so this adapter should be used inside Databricks notebooks::

        from traffic_data_elt.databricks.bronze_reader import download_and_extract, dbutils_copy_fn
        archive = download_and_extract(
            bucket=..., bronze_key=...,
            copy_fn=dbutils_copy_fn(dbutils),
        )

    UC volume paths (``/Volumes/...``) are addressed directly in the
    ``dbutils.fs`` namespace — **without** the ``file:`` scheme, which is
    blocked on serverless shared-UC compute.  Other local destinations fall
    back to the ``file:`` scheme.
    """
    def _copy(bucket: str, key: str, dest_path: str) -> None:
        src = f"s3://{bucket}/{key}"
        if dest_path.startswith("/Volumes/") or dest_path.startswith("dbfs:"):
            # UC volume / DBFS namespace path — use as-is.
            dst = dest_path
        elif dest_path.startswith("file:"):
            dst = dest_path
        else:
            dst = f"file:{dest_path}"
        dbutils.fs.cp(src, dst)

    return _copy


@dataclass
class BronzeArchive:
    """State object for a single downloaded-and-extracted Bronze archive.

    Do not construct directly; use :func:`download_and_extract`.

    Attributes
    ----------
    bucket:
        S3 bucket the ZIP was sourced from.
    bronze_key:
        S3 object key of the Bronze ZIP
        (e.g. ``bronze/pneuma/test/pnemas-sample.zip``).
    run_dir:
        Root run directory inside the UC volume (or legacy tmp dir).
        Removed entirely during :meth:`cleanup`.
    local_zip_path:
        Path to the downloaded ZIP inside ``run_dir/source/``.
    extracted_csv_path:
        Path to the extracted pNEUMA CSV inside ``run_dir/extracted/``.
    zip_member:
        The ZIP member name that was extracted (e.g. ``pnemas.csv``).
    """

    bucket: str
    bronze_key: str
    run_dir: Path
    local_zip_path: Path
    extracted_csv_path: Path
    zip_member: str
    _cleaned_up: bool = field(default=False, init=False, repr=False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def cleanup(self) -> None:
        """Remove the run directory and all contents from the volume.

        Should be called only after Silver has been written **and** validated.
        Safe to call multiple times (subsequent calls are no-ops and logged).

        Removes in order:
        1. Extracted CSV
        2. Downloaded ZIP
        3. ``extracted/`` sub-directory
        4. ``source/`` sub-directory
        5. Run root directory (if empty)
        """
        if self._cleaned_up:
            log.info("cleanup already completed for %s — skipping", self.bronze_key)
            return

        removed: list[str] = []

        # Remove files first
        for path in (self.extracted_csv_path, self.local_zip_path):
            if path.exists():
                path.unlink()
                removed.append(str(path))
                log.info("removed temporary file: %s", path)
            else:
                log.warning("cleanup: path not found (already removed?): %s", path)

        # Remove now-empty sub-directories
        for subdir in (
            self.extracted_csv_path.parent,
            self.local_zip_path.parent,
        ):
            _try_rmdir(subdir)

        # Remove run root directory
        _try_rmdir(self.run_dir)

        self._cleaned_up = True
        log.info(
            "cleanup complete for bronze_key=%s run_dir=%s — removed %d file(s)",
            self.bronze_key,
            self.run_dir,
            len(removed),
        )

    @property
    def is_cleaned_up(self) -> bool:
        """True after :meth:`cleanup` has been called successfully."""
        return self._cleaned_up


# ---------------------------------------------------------------------------
# UC volume path helpers
# ---------------------------------------------------------------------------


def uc_volume_path(
    catalog: str | None = None,
    schema: str | None = None,
    volume: str | None = None,
) -> Path:
    """Return the root path for the UC managed volume.

    Reads ``UC_CATALOG``, ``UC_SCHEMA``, ``UC_VOLUME`` from the environment
    if explicit values are not provided.

    Returns
    -------
    Path
        e.g. ``Path("/Volumes/workspace/default/v2_temp")``
    """
    cat = catalog or os.environ.get("UC_CATALOG", _DEFAULT_UC_CATALOG)
    sch = schema or os.environ.get("UC_SCHEMA", _DEFAULT_UC_SCHEMA)
    vol = volume or os.environ.get("UC_VOLUME", _DEFAULT_UC_VOLUME)
    return Path(f"/Volumes/{cat}/{sch}/{vol}")


# Volumes that must never be dropped by the temp-teardown helper: the wheel
# artifact volume is durable infrastructure, not per-run scratch space.
_PROTECTED_VOLUMES = frozenset({"v2_artifacts"})


def drop_temp_volume(
    spark,
    catalog: str | None = None,
    schema: str | None = None,
    volume: str | None = None,
) -> str:
    """Drop the temporary UC working volume (``v2_temp``) after a run.

    This is the reusable, packaged form of the teardown that was previously
    inlined in the serving notebook, so both the Databricks notebook and an
    Airflow-orchestrated task can call it identically.

    The catalog/schema/volume default from the ``UC_CATALOG`` / ``UC_SCHEMA`` /
    ``UC_VOLUME`` environment variables (same resolution as
    :func:`uc_volume_path`), so an Airflow DAG only needs to provide a Spark
    session; explicit arguments override the environment.

    Parameters
    ----------
    spark:
        Any object exposing a ``.sql(str)`` method — the Databricks notebook
        ``spark`` session, a Databricks Connect session, or an equivalent
        SQL-executor the DAG supplies. Required (there is no implicit session
        outside a notebook).
    catalog, schema, volume:
        UC volume coordinates. Default from the environment.

    Returns
    -------
    str
        The fully-qualified name of the dropped volume,
        e.g. ``"workspace.default.v2_temp"``.

    Raises
    ------
    ValueError
        If ``spark`` is ``None`` or the resolved volume is a protected
        (durable) volume such as ``v2_artifacts``.
    """
    if spark is None:
        raise ValueError(
            "drop_temp_volume requires a spark/SQL session; none was provided."
        )
    cat = catalog or os.environ.get("UC_CATALOG", _DEFAULT_UC_CATALOG)
    sch = schema or os.environ.get("UC_SCHEMA", _DEFAULT_UC_SCHEMA)
    vol = volume or os.environ.get("UC_VOLUME", _DEFAULT_UC_VOLUME)

    if vol in _PROTECTED_VOLUMES:
        raise ValueError(
            f"refusing to drop protected volume '{vol}': it is durable "
            f"infrastructure, not per-run scratch space."
        )

    fqn = f"{cat}.{sch}.{vol}"
    spark.sql(f"DROP VOLUME IF EXISTS {fqn}")
    log.info("dropped temporary volume %s (recreate before next Silver run)", fqn)
    return fqn


def make_run_dir(
    volume_root: Path,
    run_id: str | None = None,
) -> Path:
    """Create and return a run-specific subdirectory inside the volume.

    Parameters
    ----------
    volume_root:
        UC volume root path (from :func:`uc_volume_path`).
    run_id:
        Unique run identifier.  Defaults to a fresh UUID4.

    Returns
    -------
    Path
        The created run directory, e.g.
        ``/Volumes/workspace/default/v2_temp/runs/abc-123/``.
    """
    run_id = run_id or str(uuid.uuid4())
    run_dir = volume_root / "runs" / run_id
    (run_dir / "source").mkdir(parents=True, exist_ok=True)
    (run_dir / "extracted").mkdir(parents=True, exist_ok=True)
    log.info("run directory created: %s", run_dir)
    return run_dir


# ---------------------------------------------------------------------------
# Public factory function
# ---------------------------------------------------------------------------


def download_and_extract(
    bucket: str,
    bronze_key: str,
    *,
    volume_base_path: str | Path | None = None,
    tmp_dir: str | None = None,
    run_id: str | None = None,
    copy_fn: "CopyFn | None" = None,
    s3_client: "S3Client | None" = None,
    region: str | None = None,
) -> BronzeArchive:
    """Download a Bronze ZIP from S3 and extract its pNEUMA CSV member.

    Temporary files are placed inside the UC managed volume run directory::

        /Volumes/<catalog>/<schema>/<volume>/runs/<run_id>/
        ├── source/<archive>.zip
        └── extracted/pnemas.csv

    Copy mechanism
    --------------
    How the object is fetched from S3 is pluggable via ``copy_fn``:

    - **Default (boto3):** used for local development and the opt-in
      integration test, where boto3 credentials are available.
    - **Databricks serverless:** boto3 has no credentials on serverless
      compute.  Pass a ``copy_fn`` that uses ``dbutils.fs.cp`` (or Spark),
      which reads through the Unity Catalog external-location credential.
      See :func:`dbutils_copy_fn` for a ready-made adapter.

    Parameters
    ----------
    bucket:
        S3 bucket name.
    bronze_key:
        Full S3 object key of the Bronze ZIP
        (e.g. ``"bronze/pneuma/test/pnemas-sample.zip"``).
    volume_base_path:
        Override the UC volume root path.  When ``None``, resolved from
        ``UC_CATALOG`` / ``UC_SCHEMA`` / ``UC_VOLUME`` environment variables
        or their defaults.
    tmp_dir:
        Legacy override for the working directory root (used by tests to
        avoid touching the UC volume path).  Takes priority over
        ``volume_base_path`` when supplied.
    run_id:
        Unique identifier for this pipeline run.  Used as a sub-directory
        name inside the volume to prevent collisions.  Defaults to UUID4.
    copy_fn:
        Callable ``(bucket, key, dest_path) -> None`` that copies the S3
        object to ``dest_path``.  When ``None``, a boto3 downloader is used.
    s3_client:
        Optional pre-built boto3 S3 client (used only by the default boto3
        copy path).  When omitted a new client is created using the standard
        credential chain.
    region:
        AWS region passed to the boto3 client when ``s3_client`` is not
        provided.  Ignored if ``s3_client`` or ``copy_fn`` is supplied.

    Returns
    -------
    BronzeArchive
        State object containing local paths.  Call ``.cleanup()`` after
        Silver validation succeeds.

    Raises
    ------
    BronzeReaderError
        If the object cannot be downloaded, the archive cannot be read, or no
        eligible CSV member is found.
    """
    # ── Resolve working directory ────────────────────────────────────────────
    if tmp_dir is not None:
        # Legacy path: flat directory, no run_id sub-structure (tests).
        work_root = Path(tmp_dir)
        work_root.mkdir(parents=True, exist_ok=True)
        source_dir = work_root
        extracted_dir = work_root
        run_dir = work_root
    else:
        vol_root = Path(volume_base_path) if volume_base_path else uc_volume_path()
        run_dir = make_run_dir(vol_root, run_id=run_id)
        source_dir = run_dir / "source"
        extracted_dir = run_dir / "extracted"

    # ── Resolve copy mechanism ───────────────────────────────────────────────
    if copy_fn is None:
        client = s3_client or boto3.client("s3", region_name=region)
        copy_fn = _boto3_copy_fn(client)

    # ── 1. Copy ZIP from S3 → source/ ───────────────────────────────────────
    zip_filename = _zip_filename_from_key(bronze_key)
    local_zip_path = source_dir / zip_filename

    log.info("copying s3://%s/%s → %s", bucket, bronze_key, local_zip_path)
    try:
        copy_fn(bucket, bronze_key, str(local_zip_path))
    except Exception as exc:  # noqa: BLE001 - copy_fn may raise any backend error
        raise BronzeReaderError(
            f"failed to copy s3://{bucket}/{bronze_key}: {exc}"
        ) from exc

    if not local_zip_path.exists():
        raise BronzeReaderError(
            f"copy_fn completed but no file at {local_zip_path} "
            f"(s3://{bucket}/{bronze_key})"
        )

    log.info(
        "copy complete: %s (%d bytes)",
        local_zip_path,
        local_zip_path.stat().st_size,
    )

    # ── 2. Inspect ZIP contents ──────────────────────────────────────────────
    try:
        with zipfile.ZipFile(local_zip_path, "r") as zf:
            all_members = zf.namelist()
    except zipfile.BadZipFile as exc:
        raise BronzeReaderError(
            f"not a valid ZIP archive: {local_zip_path}: {exc}"
        ) from exc

    log.info("ZIP contains %d member(s): %s", len(all_members), all_members)

    # ── 3. Select CSV member ─────────────────────────────────────────────────
    csv_members = _select_csv_members(all_members)
    if not csv_members:
        raise BronzeReaderError(
            f"no .csv members found in {bronze_key}; members: {all_members}"
        )
    if len(csv_members) > 1:
        log.warning(
            "multiple CSV members found — selecting first: %s (all: %s)",
            csv_members[0],
            csv_members,
        )
    target_member = csv_members[0]
    log.info("selected ZIP member: %s", target_member)

    # ── 4. Extract CSV → extracted/ ──────────────────────────────────────────
    csv_filename = Path(target_member).name
    extracted_csv_path = extracted_dir / csv_filename

    log.info("extracting %s → %s", target_member, extracted_csv_path)
    try:
        with zipfile.ZipFile(local_zip_path, "r") as zf:
            extracted_to = zf.extract(target_member, path=str(extracted_dir))
            extracted_path = Path(extracted_to)
            if extracted_path != extracted_csv_path:
                extracted_path.rename(extracted_csv_path)
                _remove_empty_parents(extracted_path.parent, stop_at=extracted_dir)
    except (zipfile.BadZipFile, KeyError, OSError) as exc:
        raise BronzeReaderError(
            f"failed to extract {target_member} from {local_zip_path}: {exc}"
        ) from exc

    log.info(
        "extraction complete: %s (%d bytes)",
        extracted_csv_path,
        extracted_csv_path.stat().st_size,
    )

    return BronzeArchive(
        bucket=bucket,
        bronze_key=bronze_key,
        run_dir=run_dir,
        local_zip_path=local_zip_path,
        extracted_csv_path=extracted_csv_path,
        zip_member=target_member,
    )


def list_bronze_zip_members(
    bucket: str,
    bronze_key: str,
    *,
    s3_client: "S3Client | None" = None,
    region: str | None = None,
) -> list[str]:
    """Return the member names inside a Bronze ZIP without extracting.

    Downloads the full object into memory for inspection.  Suitable for the
    test ZIP (a few MB); for the full production archive a range-request
    optimisation would be preferred.

    Parameters
    ----------
    bucket, bronze_key, s3_client, region:
        As for :func:`download_and_extract`.

    Returns
    -------
    list[str]
        ZIP member names in archive order.

    Raises
    ------
    BronzeReaderError
        If the download or ZIP inspection fails.
    """
    import io

    from botocore.exceptions import BotoCoreError, ClientError

    client = s3_client or boto3.client("s3", region_name=region)

    log.info("inspecting ZIP members for s3://%s/%s", bucket, bronze_key)
    try:
        response = client.get_object(Bucket=bucket, Key=bronze_key)
        body = response["Body"].read()
    except (BotoCoreError, ClientError) as exc:
        raise BronzeReaderError(
            f"failed to download s3://{bucket}/{bronze_key}: {exc}"
        ) from exc

    try:
        with zipfile.ZipFile(io.BytesIO(body), "r") as zf:
            members = zf.namelist()
    except zipfile.BadZipFile as exc:
        raise BronzeReaderError(
            f"not a valid ZIP archive at s3://{bucket}/{bronze_key}: {exc}"
        ) from exc

    log.info("found %d member(s): %s", len(members), members)
    return members


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _zip_filename_from_key(s3_key: str) -> str:
    """Return the filename portion of an S3 key."""
    return Path(s3_key).name or "archive.zip"


def _select_csv_members(members: list[str]) -> list[str]:
    """Return ZIP members whose name ends with ``.csv`` (case-insensitive).

    Filters out macOS ``__MACOSX`` artefacts and hidden files.
    """
    return [
        m for m in members
        if m.lower().endswith(".csv")
        and not m.startswith("__MACOSX")
        and not Path(m).name.startswith(".")
    ]


def _try_rmdir(directory: Path) -> None:
    """Remove *directory* if it exists and is empty.  Logs but does not raise."""
    try:
        if directory.exists():
            directory.rmdir()
            log.info("removed empty directory: %s", directory)
    except OSError as exc:
        log.warning("could not remove directory %s: %s", directory, exc)


def _remove_empty_parents(directory: Path, *, stop_at: Path) -> None:
    """Remove empty parent directories up to (but not including) *stop_at*."""
    current = directory
    while current != stop_at and current.exists():
        try:
            current.rmdir()
            log.debug("removed empty directory: %s", current)
            current = current.parent
        except OSError:
            break
