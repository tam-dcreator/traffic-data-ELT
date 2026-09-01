"""Unit tests for HttpStreamExtractor (src/traffic_data_elt/extract/zenodo.py).

Covers:
- Streaming does not require a complete local file
- Chunked iteration and file-like read interface
- HTTP failure handling (status codes, connection errors)
- Retry behaviour on transient connect errors
- Content-length metadata exposure
- _safe_url credential stripping
"""

from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock

import pytest
import requests

from traffic_data_elt.extract.zenodo import (
    HttpStreamError,
    HttpStreamExtractor,
    _safe_url,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fake_response(
    data: bytes = b"hello world",
    status_code: int = 200,
    content_length: int | None = None,
    chunk_size: int | None = None,
) -> MagicMock:
    """Build a mock requests.Response with iter_content support."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    headers: dict[str, str] = {}
    if content_length is not None:
        headers["Content-Length"] = str(content_length)
    resp.headers = headers

    def iter_content(chunk_size=None):
        cs = chunk_size or len(data)
        buf = BytesIO(data)
        while True:
            chunk = buf.read(cs)
            if not chunk:
                break
            yield chunk

    resp.iter_content = iter_content
    resp.close = MagicMock()
    return resp


def _fake_session(response: MagicMock | None = None, exc: Exception | None = None):
    """Build a mock session whose .get() returns *response* or raises *exc*."""
    session = MagicMock()
    if exc:
        session.get = MagicMock(side_effect=exc)
    else:
        session.get = MagicMock(return_value=response)
    return session


# ---------------------------------------------------------------------------
# iter_chunks
# ---------------------------------------------------------------------------


class TestIterChunks:
    def test_yields_all_data(self):
        data = b"ABCDEFGHIJ"
        resp = _fake_response(data=data)
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", chunk_bytes=4, session=session)
        chunks = list(ext.iter_chunks())
        assert b"".join(chunks) == data

    def test_does_not_buffer_full_source(self):
        """Each chunk is yielded independently; no full accumulation."""
        data = b"X" * 100
        resp = _fake_response(data=data)
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", chunk_bytes=10, session=session)
        chunk_sizes = [len(c) for c in ext.iter_chunks()]
        assert all(s <= 10 for s in chunk_sizes)
        assert sum(chunk_sizes) == 100

    def test_content_length_exposed(self):
        resp = _fake_response(data=b"hi", content_length=2)
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", session=session)
        assert ext.content_length is None  # before open
        list(ext.iter_chunks())
        assert ext.content_length == 2

    def test_missing_content_length(self):
        resp = _fake_response(data=b"hi")
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", session=session)
        list(ext.iter_chunks())
        assert ext.content_length is None


# ---------------------------------------------------------------------------
# open() / file-like reader
# ---------------------------------------------------------------------------


class TestFilelikeReader:
    def test_read_all(self):
        resp = _fake_response(data=b"abcdef")
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", chunk_bytes=3, session=session)
        with ext.open() as reader:
            data = reader.read(-1)
        assert data == b"abcdef"

    def test_read_in_fixed_sizes(self):
        resp = _fake_response(data=b"0123456789")
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", chunk_bytes=4, session=session)
        with ext.open() as reader:
            parts = []
            while True:
                chunk = reader.read(3)
                if not chunk:
                    break
                parts.append(chunk)
        assert b"".join(parts) == b"0123456789"

    def test_bytes_read_counter(self):
        resp = _fake_response(data=b"hello")
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/f", session=session)
        with ext.open() as reader:
            reader.read(3)
            assert reader.bytes_read == 3
            reader.read(-1)
            assert reader.bytes_read == 5


# ---------------------------------------------------------------------------
# HTTP error handling
# ---------------------------------------------------------------------------


class TestHttpErrorHandling:
    def test_4xx_raises(self):
        resp = _fake_response(status_code=404)
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/missing", session=session)
        with pytest.raises(HttpStreamError, match="HTTP 404"):
            list(ext.iter_chunks())

    def test_5xx_raises(self):
        resp = _fake_response(status_code=503)
        session = _fake_session(resp)
        ext = HttpStreamExtractor("http://example.com/down", session=session)
        with pytest.raises(HttpStreamError, match="HTTP 503"):
            list(ext.iter_chunks())


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------


class TestRetry:
    def test_retries_on_connection_error(self):
        """Connect errors are retried; final failure raises HttpStreamError."""
        session = _fake_session(exc=requests.ConnectionError("refused"))
        ext = HttpStreamExtractor(
            "http://example.com/f", max_retries=2, session=session
        )
        with pytest.raises(HttpStreamError, match="3 attempt"):
            list(ext.iter_chunks())
        assert session.get.call_count == 3

    def test_succeeds_after_retry(self):
        resp = _fake_response(data=b"ok")
        session = MagicMock()
        session.get = MagicMock(
            side_effect=[requests.ConnectionError("oops"), resp]
        )
        ext = HttpStreamExtractor(
            "http://example.com/f", max_retries=1, session=session
        )
        data = b"".join(ext.iter_chunks())
        assert data == b"ok"
        assert session.get.call_count == 2


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


class TestValidation:
    def test_empty_url_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            HttpStreamExtractor("")

    def test_zero_chunk_raises(self):
        with pytest.raises(ValueError, match="positive"):
            HttpStreamExtractor("http://x.com/f", chunk_bytes=0)


# ---------------------------------------------------------------------------
# _safe_url
# ---------------------------------------------------------------------------


class TestSafeUrl:
    def test_strips_query_string(self):
        assert _safe_url("https://zenodo.org/record/123?token=secret") == "https://zenodo.org/record/123"

    def test_strips_credentials(self):
        assert _safe_url("https://user:pass@host.com/path") == "https://host.com/path"

    def test_preserves_path(self):
        assert _safe_url("https://cdn.example.com/data/archive.zip") == "https://cdn.example.com/data/archive.zip"
