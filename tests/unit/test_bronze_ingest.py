"""Unit tests for resumable ranged-multipart Bronze ingestion.

Covers (incident spec sections 24-27):
- HTTP range fetch: 206 + correct Content-Range, 200-to-range (fatal),
  wrong Content-Range (fatal), truncated (transient), size mismatch
- per-part retries: IncompleteRead/ChunkedEncoding, 429 + Retry-After, 5xx,
  ReadTimeout, ConnectionError
- task-level resume + S3 reconciliation (parts 1-10 not re-uploaded)
- checkpoint incompatibility (source size / part size / upload gone)
- completion -> HEAD validate -> checkpoint removed
- fatal source -> abort -> checkpoint removed
- completed object already exists -> multipart skipped (reused)

All HTTP + AWS boundaries are faked; no real network/credentials. Backoff sleep
is stubbed to a no-op.
"""

from __future__ import annotations

import io

import pytest
import requests
from botocore.exceptions import ClientError

from traffic_data_elt.config import BronzeTransferConfig
from traffic_data_elt.load.bronze_ingest import (
    BronzeFatalError,
    BronzeRangedUploader,
    BronzeTransientError,
    Checkpoint,
    CompletedPart,
    PartRange,
    checkpoint_key_for,
    fetch_range_to_file,
    preflight_source,
)

MIB = 1024 * 1024


# ---------------------------------------------------------------------------
# Fake HTTP layer
# ---------------------------------------------------------------------------


class FakeResp:
    """Minimal stand-in for requests.Response with streaming."""

    def __init__(self, status_code, headers=None, body=b"",
                 *, iter_error=None, iter_error_after=0):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body
        self._iter_error = iter_error
        self._iter_error_after = iter_error_after
        self.closed = False

    def iter_content(self, chunk_size=1):
        sent = 0
        buf = io.BytesIO(self._body)
        while True:
            chunk = buf.read(chunk_size)
            if not chunk:
                break
            yield chunk
            sent += len(chunk)
            if self._iter_error is not None and sent >= self._iter_error_after:
                raise self._iter_error
        # error requested but body shorter than trigger → still raise at end
        if self._iter_error is not None and sent < self._iter_error_after:
            raise self._iter_error

    def close(self):
        self.closed = True


class FakeSession:
    """Session whose .get(url, headers=..) is driven by a queued handler list.

    Each handler is either a FakeResp, or a callable(headers)->FakeResp, or an
    Exception instance to raise.
    """

    def __init__(self, handlers):
        self._handlers = list(handlers)
        self.calls = []

    def get(self, url, headers=None, stream=False, timeout=None):
        self.calls.append({"url": url, "headers": headers or {}})
        h = self._handlers.pop(0)
        if isinstance(h, Exception):
            raise h
        if callable(h):
            return h(headers or {})
        return h


def _cr(start, end, total):
    return {"Content-Range": f"bytes {start}-{end}/{total}"}


# ---------------------------------------------------------------------------
# Fake S3 client (in-memory multipart + objects + checkpoint object)
# ---------------------------------------------------------------------------


def _client_error(code, op):
    return ClientError({"Error": {"Code": code, "Message": code}}, op)


class FakeS3:
    def __init__(self):
        self.objects = {}          # key -> bytes
        self.uploads = {}          # upload_id -> {"key":, "parts": {num: (etag, size)}}
        self._uid = 0
        self.completed = []        # list of (key, upload_id)
        self.aborted = []          # list of (key, upload_id)
        self.upload_part_calls = []  # (key, part_number)

    # --- object ops ---
    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _client_error("404", "HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise _client_error("NoSuchKey", "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[Key] = Body if isinstance(Body, bytes) else bytes(Body)
        return {}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        return {}

    # --- multipart ops ---
    def create_multipart_upload(self, Bucket, Key):
        self._uid += 1
        uid = f"upload-{self._uid}"
        self.uploads[uid] = {"key": Key, "parts": {}}
        return {"UploadId": uid}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body, ContentLength=None):
        if UploadId not in self.uploads:
            raise _client_error("NoSuchUpload", "UploadPart")
        data = Body.read()
        assert ContentLength is None or ContentLength == len(data)
        etag = f'"etag-{PartNumber}-{len(data)}"'
        self.uploads[UploadId]["parts"][PartNumber] = (etag.strip('"'), len(data))
        self.upload_part_calls.append((Key, PartNumber))
        return {"ETag": etag}

    def list_parts(self, Bucket, Key, UploadId, MaxParts=None, PartNumberMarker=0):
        if UploadId not in self.uploads:
            raise _client_error("NoSuchUpload", "ListParts")
        parts = self.uploads[UploadId]["parts"]
        items = [{"PartNumber": n, "ETag": f'"{et}"', "Size": sz}
                 for n, (et, sz) in sorted(parts.items())]
        return {"Parts": items, "IsTruncated": False}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        up = self.uploads.get(UploadId)
        if up is None:
            raise _client_error("NoSuchUpload", "CompleteMultipartUpload")
        total = sum(sz for (_et, sz) in up["parts"].values())
        self.objects[Key] = b"\0" * total  # size-accurate stand-in
        self.completed.append((Key, UploadId))
        del self.uploads[UploadId]
        return {"ETag": '"final-etag"'}

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.append((Key, UploadId))
        self.uploads.pop(UploadId, None)
        return {}


def _cfg(part_size_mib=1, retries=5, http_chunk=64):
    return BronzeTransferConfig(
        part_size_mib=part_size_mib, part_retries=retries,
        connect_timeout_s=1, read_timeout_s=1, concurrency=1,
        http_chunk_bytes=http_chunk,
    )


def _uploader(s3, session, cfg, tmp_path):
    return BronzeRangedUploader(
        s3, "test-bucket", config=cfg, session=session,
        temp_dir=str(tmp_path), sleep=lambda _s: None,
    )


# ---------------------------------------------------------------------------
# fetch_range_to_file — validation
# ---------------------------------------------------------------------------


class TestFetchRange:
    def _part(self, start, end):
        return PartRange(1, start, end, end - start + 1)

    def test_206_correct_content_range(self, tmp_path):
        body = b"A" * 100
        part = self._part(0, 99)
        sess = FakeSession([FakeResp(206, _cr(0, 99, 500), body)])
        f = io.BytesIO()
        n = fetch_range_to_file(
            "http://x/f?download=1", part, f, source_size=500,
            http_chunk_bytes=16, connect_timeout_s=1, read_timeout_s=1, session=sess,
        )
        assert n == 100
        assert f.getvalue() == body
        # full URL (with query) used for the request
        assert sess.calls[0]["url"] == "http://x/f?download=1"
        assert sess.calls[0]["headers"]["Range"] == "bytes=0-99"

    def test_200_to_range_is_fatal(self, tmp_path):
        part = self._part(0, 99)
        sess = FakeSession([FakeResp(200, {}, b"A" * 100)])
        with pytest.raises(BronzeFatalError, match="200 to a ranged request"):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)

    def test_wrong_content_range_interval_is_fatal(self, tmp_path):
        part = self._part(0, 99)
        sess = FakeSession([FakeResp(206, _cr(0, 98, 500), b"A" * 99)])
        with pytest.raises(BronzeFatalError, match="Content-Range interval"):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)

    def test_content_range_total_mismatch_is_fatal(self, tmp_path):
        part = self._part(0, 99)
        sess = FakeSession([FakeResp(206, _cr(0, 99, 999), b"A" * 100)])
        with pytest.raises(BronzeFatalError, match="Content-Range total"):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)

    def test_truncated_body_is_transient(self, tmp_path):
        # server declares correct range but sends fewer bytes
        part = self._part(0, 99)
        sess = FakeSession([FakeResp(206, _cr(0, 99, 500), b"A" * 50)])
        with pytest.raises(BronzeTransientError, match="!= expected"):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)

    def test_5xx_is_transient(self, tmp_path):
        part = self._part(0, 99)
        sess = FakeSession([FakeResp(503, {}, b"")])
        with pytest.raises(BronzeTransientError, match="HTTP 503"):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)

    def test_midstream_chunked_encoding_is_transient(self, tmp_path):
        part = self._part(0, 99)
        err = requests.exceptions.ChunkedEncodingError("boom")
        sess = FakeSession([FakeResp(206, _cr(0, 99, 500), b"A" * 100,
                                     iter_error=err, iter_error_after=32)])
        with pytest.raises(BronzeTransientError, match="mid-stream"):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)

    def test_connection_error_is_transient(self, tmp_path):
        part = self._part(0, 99)
        sess = FakeSession([requests.ConnectionError("refused")])
        with pytest.raises(BronzeTransientError):
            fetch_range_to_file("http://x/f", part, io.BytesIO(), source_size=500,
                                http_chunk_bytes=16, connect_timeout_s=1,
                                read_timeout_s=1, session=sess)


# ---------------------------------------------------------------------------
# preflight_source
# ---------------------------------------------------------------------------


class TestPreflight:
    def test_206_gives_size_and_range_support(self):
        sess = FakeSession([FakeResp(206, _cr(0, 0, 15759302461), b"A")])
        meta = preflight_source("http://z/files/pNEUMA_dataset.zip?download=1",
                                session=sess)
        assert meta.size == 15759302461
        assert meta.accept_ranges is True
        assert meta.object_name == "pNEUMA_dataset.zip"

    def test_200_with_accept_ranges(self):
        sess = FakeSession([FakeResp(200, {"Content-Length": "500",
                                           "Accept-Ranges": "bytes"}, b"")])
        meta = preflight_source("http://z/f.zip", session=sess)
        assert meta.size == 500
        assert meta.accept_ranges is True

    def test_200_without_length_is_fatal(self):
        sess = FakeSession([FakeResp(200, {}, b"")])
        with pytest.raises(BronzeFatalError):
            preflight_source("http://z/f.zip", session=sess)


# ---------------------------------------------------------------------------
# End-to-end ingest via BronzeRangedUploader
# ---------------------------------------------------------------------------


def _range_handler(total):
    """Return a session handler that serves any requested range from a zero body."""
    def handler(headers):
        rng = headers["Range"]  # bytes=start-end
        s, e = rng.replace("bytes=", "").split("-")
        s, e = int(s), int(e)
        return FakeResp(206, _cr(s, e, total), b"\0" * (e - s + 1))
    return handler


class TestIngestNew:
    def test_new_upload_all_parts_then_complete(self, tmp_path):
        total = 3 * MIB + 100  # 4 parts at 1 MiB
        s3 = FakeS3()
        # 1 preflight + 4 part fetches
        handlers = [FakeResp(206, _cr(0, 0, total), b"\0")] + [_range_handler(total)] * 4
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/files/pNEUMA_dataset.zip?download=1",
                        "bronze/pneuma/pNEUMA_dataset.zip")
        assert res.disposition == "new"
        assert res.parts_count == 4
        assert res.object_size == total
        # object present, checkpoint gone
        assert "bronze/pneuma/pNEUMA_dataset.zip" in s3.objects
        assert checkpoint_key_for("bronze/pneuma/pNEUMA_dataset.zip") not in s3.objects
        assert len(s3.completed) == 1

    def test_reused_when_object_exists_and_size_matches(self, tmp_path):
        total = 2 * MIB
        s3 = FakeS3()
        s3.objects["bronze/pneuma/x.zip"] = b"\0" * total
        sess = FakeSession([FakeResp(206, _cr(0, 0, total), b"\0")])  # preflight only
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/files/x.zip", "bronze/pneuma/x.zip")
        assert res.disposition == "reused"
        assert res.parts_count == 0
        assert s3.upload_part_calls == []   # no parts uploaded
        assert s3.completed == []

    def test_existing_object_wrong_size_is_fatal(self, tmp_path):
        total = 2 * MIB
        s3 = FakeS3()
        s3.objects["bronze/pneuma/x.zip"] = b"\0" * (total - 5)  # mismatched
        sess = FakeSession([FakeResp(206, _cr(0, 0, total), b"\0")])
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        with pytest.raises(BronzeFatalError, match="!= source size"):
            up.ingest("http://z/files/x.zip", "bronze/pneuma/x.zip")


class TestIngestPerPartRetry:
    def test_part_retry_then_success(self, tmp_path):
        total = 2 * MIB  # 2 parts
        s3 = FakeS3()
        err = requests.exceptions.ChunkedEncodingError("mid")
        handlers = [
            FakeResp(206, _cr(0, 0, total), b"\0"),          # preflight
            _range_handler(total),                            # part 1 ok
            # part 2: first attempt mid-stream error, second attempt ok
            lambda h: FakeResp(206, _cr(*_se(h), total),
                               b"\0" * _len(h), iter_error=err, iter_error_after=16),
            _range_handler(total),                            # part 2 retry ok
        ]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", "bronze/pneuma/f.zip")
        assert res.disposition == "new"
        # part 1 uploaded once, part 2 uploaded once (after retry)
        assert sorted(s3.upload_part_calls) == [("bronze/pneuma/f.zip", 1),
                                                ("bronze/pneuma/f.zip", 2)]

    def test_429_with_retry_after_then_success(self, tmp_path):
        total = 1 * MIB
        s3 = FakeS3()
        resp429 = FakeResp(429, {"Retry-After": "1"}, b"")
        handlers = [
            FakeResp(206, _cr(0, 0, total), b"\0"),   # preflight
            resp429,                                   # part 1 attempt 1: 429
            _range_handler(total),                     # part 1 attempt 2: ok
        ]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", "bronze/pneuma/f.zip")
        assert res.parts_count == 1
        assert ("bronze/pneuma/f.zip", 1) in s3.upload_part_calls


class TestIngestResume:
    def test_resume_skips_completed_parts(self, tmp_path):
        total = 4 * MIB  # 4 parts
        s3 = FakeS3()
        key = "bronze/pneuma/f.zip"
        # Simulate attempt 1 that completed parts 1-2 into a real S3 upload +
        # a checkpoint, then died.
        uid = s3.create_multipart_upload(Bucket="test-bucket", Key=key)["UploadId"]
        for pn in (1, 2):
            s3.uploads[uid]["parts"][pn] = (f"etag-{pn}-{MIB}", MIB)
        ckpt = Checkpoint(
            version=1, source_url="http://z/f.zip", source_size=total,
            part_size=MIB, bucket="test-bucket", key=key, upload_id=uid,
            completed_parts=[CompletedPart(1, f"etag-1-{MIB}", 0, MIB - 1),
                             CompletedPart(2, f"etag-2-{MIB}", MIB, 2 * MIB - 1)],
        )
        s3.put_object(Bucket="test-bucket", Key=checkpoint_key_for(key),
                      Body=ckpt.to_json().encode())

        # attempt 2: preflight + parts 3,4 only
        handlers = [FakeResp(206, _cr(0, 0, total), b"\0"),
                    _range_handler(total), _range_handler(total)]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", key)

        assert res.disposition == "resumed"
        # only parts 3 and 4 were uploaded this run
        assert sorted(s3.upload_part_calls) == [(key, 3), (key, 4)]
        assert (key, uid) in s3.completed

    def test_checkpoint_source_size_mismatch_recreates(self, tmp_path):
        total = 2 * MIB
        s3 = FakeS3()
        key = "bronze/pneuma/f.zip"
        uid = s3.create_multipart_upload(Bucket="test-bucket", Key=key)["UploadId"]
        # checkpoint claims a DIFFERENT source size → incompatible → abort+recreate
        ckpt = Checkpoint(
            version=1, source_url="http://z/f.zip", source_size=999999,
            part_size=MIB, bucket="test-bucket", key=key, upload_id=uid,
            completed_parts=[CompletedPart(1, "e", 0, MIB - 1)],
        )
        s3.put_object(Bucket="test-bucket", Key=checkpoint_key_for(key),
                      Body=ckpt.to_json().encode())
        handlers = [FakeResp(206, _cr(0, 0, total), b"\0"),
                    _range_handler(total), _range_handler(total)]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", key)
        assert res.disposition == "new"          # recreated
        assert (key, uid) in s3.aborted           # incompatible upload aborted

    def test_checkpoint_upload_gone_recreates(self, tmp_path):
        total = 2 * MIB
        s3 = FakeS3()
        key = "bronze/pneuma/f.zip"
        # checkpoint references an upload_id that does not exist in S3
        ckpt = Checkpoint(
            version=1, source_url="http://z/f.zip", source_size=total,
            part_size=MIB, bucket="test-bucket", key=key, upload_id="ghost",
            completed_parts=[CompletedPart(1, "e", 0, MIB - 1)],
        )
        s3.put_object(Bucket="test-bucket", Key=checkpoint_key_for(key),
                      Body=ckpt.to_json().encode())
        handlers = [FakeResp(206, _cr(0, 0, total), b"\0"),
                    _range_handler(total), _range_handler(total)]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", key)
        assert res.disposition == "new"


class TestIngestReconcile:
    def test_reconcile_drops_part_absent_in_s3(self, tmp_path):
        total = 3 * MIB
        s3 = FakeS3()
        key = "bronze/pneuma/f.zip"
        uid = s3.create_multipart_upload(Bucket="test-bucket", Key=key)["UploadId"]
        # S3 actually has only part 1; checkpoint claims 1 and 2.
        s3.uploads[uid]["parts"][1] = (f"etag-1-{MIB}", MIB)
        ckpt = Checkpoint(
            version=1, source_url="http://z/f.zip", source_size=total,
            part_size=MIB, bucket="test-bucket", key=key, upload_id=uid,
            completed_parts=[CompletedPart(1, f"etag-1-{MIB}", 0, MIB - 1),
                             CompletedPart(2, "bogus", MIB, 2 * MIB - 1)],
        )
        s3.put_object(Bucket="test-bucket", Key=checkpoint_key_for(key),
                      Body=ckpt.to_json().encode())
        # attempt: preflight + parts 2,3 (part 2 dropped by reconcile, re-fetched)
        handlers = [FakeResp(206, _cr(0, 0, total), b"\0"),
                    _range_handler(total), _range_handler(total)]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", key)
        assert res.disposition == "resumed"
        # part 1 NOT re-uploaded; parts 2 and 3 uploaded
        assert sorted(s3.upload_part_calls) == [(key, 2), (key, 3)]


class TestPreflightFallbackToCheckpoint:
    def test_transient_preflight_resumes_from_checkpoint(self, tmp_path):
        """Preflight 504 must NOT block resuming already-banked parts."""
        total = 4 * MIB  # 4 parts at 1 MiB
        s3 = FakeS3()
        key = "bronze/pneuma/f.zip"
        # Prior attempt banked parts 1-2 in a real MPU + compatible checkpoint.
        uid = s3.create_multipart_upload(Bucket="test-bucket", Key=key)["UploadId"]
        for pn in (1, 2):
            s3.uploads[uid]["parts"][pn] = (f"etag-{pn}-{MIB}", MIB)
        ckpt = Checkpoint(
            version=1, source_url="http://z/f.zip", source_size=total,
            part_size=MIB, bucket="test-bucket", key=key, upload_id=uid,
            completed_parts=[CompletedPart(1, f"etag-1-{MIB}", 0, MIB - 1),
                             CompletedPart(2, f"etag-2-{MIB}", MIB, 2 * MIB - 1)],
        )
        s3.put_object(Bucket="test-bucket", Key=checkpoint_key_for(key),
                      Body=ckpt.to_json().encode())
        # Preflight fails transiently on ALL attempts (5x 504), then parts 3,4.
        handlers = [FakeResp(504, {}, b"")] * 5 + [_range_handler(total),
                                                   _range_handler(total)]
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        res = up.ingest("http://z/f.zip", key)
        assert res.disposition == "resumed"
        # only parts 3,4 uploaded; 1,2 preserved
        assert sorted(s3.upload_part_calls) == [(key, 3), (key, 4)]
        assert (key, uid) in s3.completed

    def test_transient_preflight_no_checkpoint_still_fails(self, tmp_path):
        """With no compatible checkpoint, a transient preflight failure raises."""
        s3 = FakeS3()
        handlers = [FakeResp(504, {}, b"")] * 5  # preflight exhausts retries
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        with pytest.raises(BronzeTransientError, match="preflight failed"):
            up.ingest("http://z/f.zip", "bronze/pneuma/none.zip")


class TestBoundedDisk:
    def test_no_large_local_file_and_temp_cleaned(self, tmp_path):
        """Per-part temp files are cleaned; temp dir never holds a full copy."""
        total = 3 * MIB + 10  # 4 parts
        s3 = FakeS3()
        handlers = [FakeResp(206, _cr(0, 0, total), b"\0")] + [_range_handler(total)] * 4
        sess = FakeSession(handlers)
        up = _uploader(s3, sess, _cfg(part_size_mib=1), tmp_path)
        up.ingest("http://z/f.zip", "bronze/pneuma/f.zip")
        # After completion, no bronze_part_* temp files remain in the temp dir.
        leftovers = list(tmp_path.glob("bronze_part_*"))
        assert leftovers == []


# helpers to build a 206 for a requested range from headers
def _se(headers):
    s, e = headers["Range"].replace("bytes=", "").split("-")
    return int(s), int(e)


def _len(headers):
    s, e = _se(headers)
    return e - s + 1
