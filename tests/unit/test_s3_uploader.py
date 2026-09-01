"""Unit tests for S3Uploader (src/traffic_data_elt/load/s3_uploader.py).

Covers:
- Successful upload result metadata (bucket, key, bytes, etag)
- Bronze key construction via config
- Streaming (no full-source buffering)
- S3 upload failure handling
- abort_incomplete_uploads cleanup behaviour
- ETag HEAD handling

All AWS boundaries are mocked — no real credentials required.
"""

from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from traffic_data_elt.config import AwsConfig
from traffic_data_elt.load.s3_uploader import (
    S3Uploader,
    S3UploadError,
    UploadResult,
    _CountingReader,
)


def _cfg() -> AwsConfig:
    return AwsConfig(region="eu-central-1", bucket="test-bucket", bronze_prefix="bronze")


def _client_error(op: str = "UploadPart") -> ClientError:
    return ClientError(
        {"Error": {"Code": "500", "Message": "boom"}}, op
    )


# ---------------------------------------------------------------------------
# upload_stream — success
# ---------------------------------------------------------------------------


class TestUploadSuccess:
    def test_returns_result_metadata(self):
        client = MagicMock()
        client.head_object = MagicMock(return_value={"ETag": '"abc123"'})

        def fake_upload(fileobj, bucket, key, ExtraArgs=None, Config=None):
            # Drain the stream as boto3 would, so the counter advances.
            while fileobj.read(4):
                pass

        client.upload_fileobj = MagicMock(side_effect=fake_upload)

        uploader = S3Uploader(_cfg(), client=client)
        body = BytesIO(b"0123456789")
        result = uploader.upload_stream(body, "test", "sample.csv")

        assert isinstance(result, UploadResult)
        assert result.bucket == "test-bucket"
        assert result.key == "bronze/test/sample.csv"
        assert result.bytes_transferred == 10
        assert result.status == "success"
        assert result.etag == "abc123"

    def test_uses_bronze_key(self):
        client = MagicMock()
        client.head_object = MagicMock(return_value={"ETag": '"x"'})
        client.upload_fileobj = MagicMock()

        uploader = S3Uploader(_cfg(), client=client)
        uploader.upload_stream(BytesIO(b"d"), "pneuma", "2018", "a.csv")

        args = client.upload_fileobj.call_args.args
        # args: (fileobj, bucket, key)
        assert args[1] == "test-bucket"
        assert args[2] == "bronze/pneuma/2018/a.csv"

    def test_content_type_passed(self):
        client = MagicMock()
        client.head_object = MagicMock(return_value={"ETag": '"x"'})
        client.upload_fileobj = MagicMock()

        uploader = S3Uploader(_cfg(), client=client)
        uploader.upload_stream(BytesIO(b"d"), "a.zip", content_type="application/zip")

        kwargs = client.upload_fileobj.call_args.kwargs
        assert kwargs["ExtraArgs"] == {"ContentType": "application/zip"}

    def test_missing_etag_returns_none(self):
        client = MagicMock()
        client.head_object = MagicMock(return_value={})
        client.upload_fileobj = MagicMock()

        uploader = S3Uploader(_cfg(), client=client)
        result = uploader.upload_stream(BytesIO(b"d"), "a.csv")
        assert result.etag is None

    def test_head_failure_returns_none_etag(self):
        client = MagicMock()
        client.head_object = MagicMock(side_effect=_client_error("HeadObject"))
        client.upload_fileobj = MagicMock()

        uploader = S3Uploader(_cfg(), client=client)
        result = uploader.upload_stream(BytesIO(b"d"), "a.csv")
        assert result.status == "success"
        assert result.etag is None


# ---------------------------------------------------------------------------
# upload_stream — failure
# ---------------------------------------------------------------------------


class TestUploadFailure:
    def test_client_error_raises_s3uploaderror(self):
        client = MagicMock()
        client.upload_fileobj = MagicMock(side_effect=_client_error())

        uploader = S3Uploader(_cfg(), client=client)
        with pytest.raises(S3UploadError, match="failed to upload"):
            uploader.upload_stream(BytesIO(b"data"), "a.csv")

    def test_failure_does_not_call_head(self):
        client = MagicMock()
        client.upload_fileobj = MagicMock(side_effect=_client_error())
        client.head_object = MagicMock()

        uploader = S3Uploader(_cfg(), client=client)
        with pytest.raises(S3UploadError):
            uploader.upload_stream(BytesIO(b"data"), "a.csv")
        client.head_object.assert_not_called()


# ---------------------------------------------------------------------------
# abort_incomplete_uploads
# ---------------------------------------------------------------------------


class TestAbortIncomplete:
    def test_aborts_listed_uploads(self):
        client = MagicMock()
        client.list_multipart_uploads = MagicMock(
            return_value={
                "Uploads": [
                    {"Key": "bronze/test/a.csv", "UploadId": "u1"},
                    {"Key": "bronze/test/b.csv", "UploadId": "u2"},
                ]
            }
        )
        client.abort_multipart_upload = MagicMock()

        uploader = S3Uploader(_cfg(), client=client)
        count = uploader.abort_incomplete_uploads("bronze/test/")
        assert count == 2
        assert client.abort_multipart_upload.call_count == 2

    def test_no_uploads_returns_zero(self):
        client = MagicMock()
        client.list_multipart_uploads = MagicMock(return_value={})
        uploader = S3Uploader(_cfg(), client=client)
        assert uploader.abort_incomplete_uploads("bronze/test/") == 0

    def test_list_failure_returns_zero(self):
        client = MagicMock()
        client.list_multipart_uploads = MagicMock(side_effect=_client_error("ListMultipartUploads"))
        uploader = S3Uploader(_cfg(), client=client)
        assert uploader.abort_incomplete_uploads("bronze/test/") == 0

    def test_individual_abort_failure_is_skipped(self):
        client = MagicMock()
        client.list_multipart_uploads = MagicMock(
            return_value={
                "Uploads": [
                    {"Key": "bronze/test/a.csv", "UploadId": "u1"},
                    {"Key": "bronze/test/b.csv", "UploadId": "u2"},
                ]
            }
        )
        client.abort_multipart_upload = MagicMock(
            side_effect=[_client_error("AbortMultipartUpload"), None]
        )
        uploader = S3Uploader(_cfg(), client=client)
        # One fails, one succeeds → count of 1
        assert uploader.abort_incomplete_uploads("bronze/test/") == 1


# ---------------------------------------------------------------------------
# _CountingReader
# ---------------------------------------------------------------------------


class TestCountingReader:
    def test_counts_bytes_and_delegates(self):
        reader = _CountingReader(BytesIO(b"abcdef"))
        assert reader.read(3) == b"abc"
        assert reader.bytes_read == 3
        assert reader.read(-1) == b"def"
        assert reader.bytes_read == 6

    def test_empty_read(self):
        reader = _CountingReader(BytesIO(b""))
        assert reader.read(10) == b""
        assert reader.bytes_read == 0
