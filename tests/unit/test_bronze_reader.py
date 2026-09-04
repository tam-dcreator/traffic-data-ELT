"""Unit tests for v2_cloud/databricks/bronze_reader.py.

Tests the pure-Python logic without AWS connectivity or actual ZIP archives:
- _zip_filename_from_key: key → filename extraction
- _select_csv_members: CSV member filtering (macOS artefacts, hidden files)
- uc_volume_path: UC volume path resolution and env var override
- make_run_dir: run directory structure creation
- BronzeArchive.cleanup: removes files and run dir, sets flag, handles missing
- BronzeArchive.cleanup: idempotent second call
- BronzeArchive.is_cleaned_up property
- download_and_extract: integration over a real in-memory ZIP (no S3)
- list_bronze_zip_members: member listing from in-memory ZIP (no S3)

All S3 calls are mocked via unittest.mock.
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from traffic_data_elt.databricks.bronze_reader import (
    BronzeArchive,
    BronzeReaderError,
    _select_csv_members,
    _zip_filename_from_key,
    download_and_extract,
    drop_temp_volume,
    list_bronze_zip_members,
    make_run_dir,
    uc_volume_path,
)


# ---------------------------------------------------------------------------
# _zip_filename_from_key
# ---------------------------------------------------------------------------

class TestZipFilenameFromKey:
    def test_simple_key(self):
        assert _zip_filename_from_key("bronze/test/sample.zip") == "sample.zip"

    def test_nested_key(self):
        assert _zip_filename_from_key("a/b/c/d.zip") == "d.zip"

    def test_flat_key(self):
        assert _zip_filename_from_key("archive.zip") == "archive.zip"

    def test_empty_key_fallback(self):
        # An empty or directory-like key falls back to "archive.zip".
        result = _zip_filename_from_key("")
        assert result  # non-empty


# ---------------------------------------------------------------------------
# _select_csv_members
# ---------------------------------------------------------------------------

class TestSelectCsvMembers:
    def test_returns_csv_members(self):
        members = ["README.txt", "data.csv", "notes.md"]
        assert _select_csv_members(members) == ["data.csv"]

    def test_case_insensitive(self):
        members = ["DATA.CSV", "other.TXT"]
        assert _select_csv_members(members) == ["DATA.CSV"]

    def test_filters_macosx_artefacts(self):
        members = ["__MACOSX/._data.csv", "data.csv"]
        assert _select_csv_members(members) == ["data.csv"]

    def test_filters_hidden_files(self):
        members = [".hidden.csv", "visible.csv"]
        assert _select_csv_members(members) == ["visible.csv"]

    def test_empty_input(self):
        assert _select_csv_members([]) == []

    def test_no_csv_members(self):
        assert _select_csv_members(["a.txt", "b.json"]) == []

    def test_multiple_csv_members(self):
        members = ["a.csv", "b.csv", "c.txt"]
        result = _select_csv_members(members)
        assert result == ["a.csv", "b.csv"]

    def test_nested_path_csv(self):
        members = ["subdir/data.csv"]
        assert _select_csv_members(members) == ["subdir/data.csv"]


# ---------------------------------------------------------------------------
# uc_volume_path
# ---------------------------------------------------------------------------

class TestUcVolumePath:
    def test_defaults_from_env(self, monkeypatch):
        monkeypatch.setenv("UC_CATALOG", "mycat")
        monkeypatch.setenv("UC_SCHEMA", "mysch")
        monkeypatch.setenv("UC_VOLUME", "myvol")
        result = uc_volume_path()
        assert str(result) == "/Volumes/mycat/mysch/myvol"

    def test_explicit_args_override_env(self, monkeypatch):
        monkeypatch.setenv("UC_CATALOG", "should-be-ignored")
        result = uc_volume_path(catalog="c", schema="s", volume="v")
        assert str(result) == "/Volumes/c/s/v"

    def test_default_fallbacks_without_env(self, monkeypatch):
        for var in ("UC_CATALOG", "UC_SCHEMA", "UC_VOLUME"):
            monkeypatch.delenv(var, raising=False)
        result = uc_volume_path()
        assert "workspace" in str(result)
        assert "default" in str(result)
        assert "v2_temp" in str(result)

    def test_returns_path_object(self):
        result = uc_volume_path(catalog="c", schema="s", volume="v")
        assert isinstance(result, Path)


# ---------------------------------------------------------------------------
# drop_temp_volume
# ---------------------------------------------------------------------------

class TestDropTempVolume:
    def test_issues_drop_sql_with_explicit_coords(self):
        spark = MagicMock()
        fqn = drop_temp_volume(spark, catalog="c", schema="s", volume="v2_temp")
        assert fqn == "c.s.v2_temp"
        spark.sql.assert_called_once_with("DROP VOLUME IF EXISTS c.s.v2_temp")

    def test_defaults_from_env(self, monkeypatch):
        monkeypatch.setenv("UC_CATALOG", "mycat")
        monkeypatch.setenv("UC_SCHEMA", "mysch")
        monkeypatch.setenv("UC_VOLUME", "mytemp")
        spark = MagicMock()
        fqn = drop_temp_volume(spark)
        assert fqn == "mycat.mysch.mytemp"
        spark.sql.assert_called_once_with("DROP VOLUME IF EXISTS mycat.mysch.mytemp")

    def test_default_fallbacks_without_env(self, monkeypatch):
        for var in ("UC_CATALOG", "UC_SCHEMA", "UC_VOLUME"):
            monkeypatch.delenv(var, raising=False)
        spark = MagicMock()
        fqn = drop_temp_volume(spark)
        assert fqn == "workspace.default.v2_temp"

    def test_refuses_protected_artifact_volume(self):
        spark = MagicMock()
        with pytest.raises(ValueError, match="protected volume"):
            drop_temp_volume(spark, catalog="workspace", schema="default", volume="v2_artifacts")
        spark.sql.assert_not_called()

    def test_refuses_protected_volume_via_env(self, monkeypatch):
        monkeypatch.setenv("UC_VOLUME", "v2_artifacts")
        spark = MagicMock()
        with pytest.raises(ValueError, match="protected volume"):
            drop_temp_volume(spark)
        spark.sql.assert_not_called()

    def test_requires_spark_session(self):
        with pytest.raises(ValueError, match="requires a spark"):
            drop_temp_volume(None, catalog="c", schema="s", volume="v2_temp")


# ---------------------------------------------------------------------------
# make_run_dir
# ---------------------------------------------------------------------------

class TestMakeRunDir:
    def test_creates_source_and_extracted(self, tmp_path):
        run_dir = make_run_dir(tmp_path, run_id="test-run")
        assert (run_dir / "source").exists()
        assert (run_dir / "extracted").exists()

    def test_run_id_in_path(self, tmp_path):
        run_dir = make_run_dir(tmp_path, run_id="abc-123")
        assert "abc-123" in str(run_dir)

    def test_default_run_id_is_unique(self, tmp_path):
        dir1 = make_run_dir(tmp_path)
        dir2 = make_run_dir(tmp_path)
        assert dir1 != dir2

    def test_nested_under_runs(self, tmp_path):
        run_dir = make_run_dir(tmp_path, run_id="r1")
        assert run_dir.parent.name == "runs"


# ---------------------------------------------------------------------------
# BronzeArchive.cleanup
# ---------------------------------------------------------------------------

class TestBronzeArchiveCleanup:
    def _make_archive(self, tmp_path: Path) -> BronzeArchive:
        run_dir = tmp_path / "runs" / "test-run"
        source_dir = run_dir / "source"
        extracted_dir = run_dir / "extracted"
        source_dir.mkdir(parents=True)
        extracted_dir.mkdir(parents=True)
        zip_path = source_dir / "archive.zip"
        csv_path = extracted_dir / "data.csv"
        zip_path.write_text("zip content")
        csv_path.write_text("csv content")
        return BronzeArchive(
            bucket="b",
            bronze_key="bronze/test/archive.zip",
            run_dir=run_dir,
            local_zip_path=zip_path,
            extracted_csv_path=csv_path,
            zip_member="data.csv",
        )

    def test_removes_both_files(self, tmp_path):
        archive = self._make_archive(tmp_path)
        archive.cleanup()
        assert not archive.local_zip_path.exists()
        assert not archive.extracted_csv_path.exists()

    def test_sets_cleaned_up_flag(self, tmp_path):
        archive = self._make_archive(tmp_path)
        assert not archive.is_cleaned_up
        archive.cleanup()
        assert archive.is_cleaned_up

    def test_idempotent_second_call(self, tmp_path):
        archive = self._make_archive(tmp_path)
        archive.cleanup()
        # Second call must not raise even though files are gone.
        archive.cleanup()
        assert archive.is_cleaned_up

    def test_tolerates_missing_files(self, tmp_path):
        archive = self._make_archive(tmp_path)
        archive.local_zip_path.unlink()  # pre-delete one file
        # Should not raise.
        archive.cleanup()
        assert archive.is_cleaned_up


# ---------------------------------------------------------------------------
# download_and_extract (mocked S3)
# ---------------------------------------------------------------------------

def _make_zip_bytes(member_name: str, content: str = "track_id;type\n1;Car\n") -> bytes:
    """Build a minimal in-memory ZIP with one text member."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(member_name, content)
    return buf.getvalue()


class TestDownloadAndExtract:
    def test_happy_path(self, tmp_path):
        zip_bytes = _make_zip_bytes("pnemas.csv")
        mock_client = MagicMock()
        mock_client.download_file.side_effect = (
            lambda bucket, key, dest: Path(dest).write_bytes(zip_bytes)
        )

        archive = download_and_extract(
            bucket="my-bucket",
            bronze_key="bronze/test/pnemas-sample.zip",
            tmp_dir=str(tmp_path),
            s3_client=mock_client,
        )

        assert archive.bucket == "my-bucket"
        assert archive.bronze_key == "bronze/test/pnemas-sample.zip"
        assert archive.zip_member == "pnemas.csv"
        assert archive.extracted_csv_path.exists()
        assert archive.extracted_csv_path.name == "pnemas.csv"
        assert not archive.is_cleaned_up

    def test_download_failure_raises(self, tmp_path):
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.download_file.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
        )

        with pytest.raises(BronzeReaderError, match="failed to copy"):
            download_and_extract(
                bucket="b",
                bronze_key="bronze/missing.zip",
                tmp_dir=str(tmp_path),
                s3_client=mock_client,
            )

    def test_no_csv_member_raises(self, tmp_path):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("README.txt", "nothing here")
        zip_bytes = buf.getvalue()

        mock_client = MagicMock()
        mock_client.download_file.side_effect = (
            lambda bucket, key, dest: Path(dest).write_bytes(zip_bytes)
        )

        with pytest.raises(BronzeReaderError, match="no .csv members"):
            download_and_extract(
                bucket="b",
                bronze_key="bronze/no-csv.zip",
                tmp_dir=str(tmp_path),
                s3_client=mock_client,
            )

    def test_bad_zip_raises(self, tmp_path):
        mock_client = MagicMock()
        mock_client.download_file.side_effect = (
            lambda bucket, key, dest: Path(dest).write_bytes(b"not a zip")
        )

        with pytest.raises(BronzeReaderError, match="not a valid ZIP"):
            download_and_extract(
                bucket="b",
                bronze_key="bronze/bad.zip",
                tmp_dir=str(tmp_path),
                s3_client=mock_client,
            )

    def test_cleanup_after_extraction(self, tmp_path):
        zip_bytes = _make_zip_bytes("data.csv")
        mock_client = MagicMock()
        mock_client.download_file.side_effect = (
            lambda bucket, key, dest: Path(dest).write_bytes(zip_bytes)
        )

        archive = download_and_extract(
            bucket="b",
            bronze_key="bronze/test/data.zip",
            tmp_dir=str(tmp_path),
            s3_client=mock_client,
        )

        archive.cleanup()
        assert not archive.extracted_csv_path.exists()
        assert not archive.local_zip_path.exists()

    def test_custom_copy_fn_used(self, tmp_path):
        """A supplied copy_fn is used instead of boto3."""
        zip_bytes = _make_zip_bytes("pnemas.csv")
        calls = []

        def fake_copy(bucket, key, dest):
            calls.append((bucket, key, dest))
            Path(dest).write_bytes(zip_bytes)

        archive = download_and_extract(
            bucket="b",
            bronze_key="bronze/test/pnemas-sample.zip",
            tmp_dir=str(tmp_path),
            copy_fn=fake_copy,
        )

        assert len(calls) == 1
        assert calls[0][0] == "b"
        assert calls[0][1] == "bronze/test/pnemas-sample.zip"
        assert archive.extracted_csv_path.exists()

    def test_copy_fn_missing_file_raises(self, tmp_path):
        """If copy_fn returns but writes no file, a clear error is raised."""
        def noop_copy(bucket, key, dest):
            pass  # does not write anything

        with pytest.raises(BronzeReaderError, match="no file at"):
            download_and_extract(
                bucket="b",
                bronze_key="bronze/test/x.zip",
                tmp_dir=str(tmp_path),
                copy_fn=noop_copy,
            )

    def test_uc_volume_path_structure(self, tmp_path):
        """When volume_base_path is used, files go into source/ and extracted/ subdirs."""
        zip_bytes = _make_zip_bytes("pnemas.csv")
        mock_client = MagicMock()
        mock_client.download_file.side_effect = (
            lambda bucket, key, dest: Path(dest).write_bytes(zip_bytes)
        )

        archive = download_and_extract(
            bucket="b",
            bronze_key="bronze/test/pnemas-sample.zip",
            volume_base_path=str(tmp_path),
            run_id="test-run-001",
            s3_client=mock_client,
        )

        assert archive.local_zip_path.parent.name == "source"
        assert archive.extracted_csv_path.parent.name == "extracted"
        assert "test-run-001" in str(archive.run_dir)
        assert archive.extracted_csv_path.name == "pnemas.csv"
        assert archive.extracted_csv_path.exists()


# ---------------------------------------------------------------------------
# list_bronze_zip_members (mocked S3)
# ---------------------------------------------------------------------------

class TestListBronzeZipMembers:
    def test_returns_member_names(self):
        zip_bytes = _make_zip_bytes("pnemas.csv")
        mock_client = MagicMock()
        mock_client.get_object.return_value = {"Body": MagicMock(read=lambda: zip_bytes)}

        members = list_bronze_zip_members(
            bucket="b",
            bronze_key="bronze/test/pnemas-sample.zip",
            s3_client=mock_client,
        )

        assert "pnemas.csv" in members

    def test_s3_error_raises(self):
        from botocore.exceptions import ClientError

        mock_client = MagicMock()
        mock_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}, "GetObject"
        )

        with pytest.raises(BronzeReaderError, match="failed to download"):
            list_bronze_zip_members(
                bucket="b",
                bronze_key="bronze/missing.zip",
                s3_client=mock_client,
            )


# ---------------------------------------------------------------------------
# dbutils_copy_fn adapter
# ---------------------------------------------------------------------------

class TestDbutilsCopyFn:
    def test_volume_path_used_without_file_scheme(self):
        """UC volume paths must NOT get a file: prefix (blocked on serverless)."""
        from traffic_data_elt.databricks.bronze_reader import dbutils_copy_fn

        mock_dbutils = MagicMock()
        copy = dbutils_copy_fn(mock_dbutils)
        copy("mybucket", "bronze/test/x.zip", "/Volumes/c/s/v/runs/r1/source/x.zip")

        mock_dbutils.fs.cp.assert_called_once_with(
            "s3://mybucket/bronze/test/x.zip",
            "/Volumes/c/s/v/runs/r1/source/x.zip",
        )

    def test_preserves_existing_file_scheme(self):
        from traffic_data_elt.databricks.bronze_reader import dbutils_copy_fn

        mock_dbutils = MagicMock()
        copy = dbutils_copy_fn(mock_dbutils)
        copy("b", "k", "file:/already/scheme.zip")

        mock_dbutils.fs.cp.assert_called_once_with(
            "s3://b/k", "file:/already/scheme.zip"
        )

    def test_non_volume_local_path_gets_file_scheme(self):
        from traffic_data_elt.databricks.bronze_reader import dbutils_copy_fn

        mock_dbutils = MagicMock()
        copy = dbutils_copy_fn(mock_dbutils)
        copy("b", "k", "/tmp/local/x.zip")

        mock_dbutils.fs.cp.assert_called_once_with(
            "s3://b/k", "file:/tmp/local/x.zip"
        )
