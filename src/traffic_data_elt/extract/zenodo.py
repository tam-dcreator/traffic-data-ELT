"""HTTP streaming extractor for remote source archives.

Streams a remote HTTP resource incrementally so that large objects (such as
the pNEUMA archive) can be uploaded to S3 without ever holding the complete
object in memory or writing it to local disk.

Design
------
- ``HttpStreamExtractor`` opens a streaming HTTP GET and exposes two reusable
  interfaces over the response body:

  * :meth:`iter_chunks` — a byte-chunk iterator.
  * :meth:`open` — a context manager yielding a file-like object
    (:class:`_HttpStreamReader`) with a ``read(size)`` method, suitable for
    ``boto3`` ``upload_fileobj`` and multipart transfers.

- The class is source-agnostic: it works for a small HTTP test file as well
  as the eventual Zenodo archive.  ``ZenodoStreamExtractor`` is a thin alias.

- Credentials and query strings are never logged.  Only the scheme, host, and
  path are logged for observability.
"""

from __future__ import annotations

from types import TracebackType
from typing import IO, Iterator, Mapping, Self
from urllib.parse import urlparse

import requests

from traffic_data_elt.utils import get_logger

log = get_logger(__name__)

_DEFAULT_CHUNK_BYTES = 1 * 1024 * 1024  # 1 MiB
_DEFAULT_TIMEOUT_S = 30.0


class HttpStreamError(RuntimeError):
    """Raised when the remote HTTP source cannot be streamed."""


def _safe_url(url: str) -> str:
    """Return a log-safe representation of *url* (scheme://host/path only).

    Strips query strings and any embedded credentials so tokens are never
    written to logs.
    """
    try:
        parsed = urlparse(url)
        host = parsed.hostname or ""
        return f"{parsed.scheme}://{host}{parsed.path}"
    except Exception:
        return "<unparseable-url>"


class _HttpStreamReader(IO[bytes]):
    """Minimal read-only file-like wrapper around a chunk iterator.

    Presents a ``read(size)`` interface backed by an underlying byte-chunk
    iterator.  Only the small amount of data required to satisfy each read is
    buffered, so memory usage stays bounded regardless of total object size.

    This is intentionally minimal — it implements only what ``boto3``'s
    managed upload requires (``read``), plus ``close``/context-manager
    support.
    """

    def __init__(self, chunks: Iterator[bytes]) -> None:
        self._chunks = chunks
        self._buffer = bytearray()
        self._eof = False
        self._closed = False
        self._bytes_read = 0

    @property
    def bytes_read(self) -> int:
        """Total number of bytes returned to callers so far."""
        return self._bytes_read

    def read(self, size: int = -1) -> bytes:
        """Read up to *size* bytes.  ``size < 0`` reads until EOF.

        Reading until EOF still pulls one chunk at a time, but accumulates the
        full result — callers that pass ``-1`` on a large object defeat the
        streaming guarantee, so prefer a positive size for large sources.
        """
        if self._closed:
            raise ValueError("read on closed stream")

        if size is None or size < 0:
            # Drain remaining chunks.
            while not self._eof:
                self._fill(1)
                if not self._buffer:
                    break
                # Pull everything available.
                self._drain_all()
            data = bytes(self._buffer)
            self._buffer.clear()
            self._bytes_read += len(data)
            return data

        while len(self._buffer) < size and not self._eof:
            self._fill(size)

        data = bytes(self._buffer[:size])
        del self._buffer[:size]
        self._bytes_read += len(data)
        return data

    def _fill(self, _hint: int) -> None:
        """Pull the next chunk into the buffer, or mark EOF."""
        try:
            chunk = next(self._chunks)
        except StopIteration:
            self._eof = True
            return
        if chunk:
            self._buffer.extend(chunk)

    def _drain_all(self) -> None:
        for chunk in self._chunks:
            if chunk:
                self._buffer.extend(chunk)
        self._eof = True

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def close(self) -> None:
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()


class HttpStreamExtractor:
    """Stream a remote HTTP resource incrementally.

    Parameters
    ----------
    url:
        Fully-qualified HTTP(S) URL of the source resource.
    chunk_bytes:
        Size of each streamed chunk in bytes.  Defaults to 1 MiB.
    timeout_s:
        Per-request timeout (connect/read) in seconds.
    max_retries:
        Number of times to retry establishing the streaming connection on
        transient errors.  Applies only to connection setup, not to mid-stream
        interruptions (a mid-stream failure raises, so the caller — e.g. the
        S3 uploader — can abort and retry the whole transfer cleanly).
    headers:
        Optional extra request headers.
    session:
        Optional pre-configured :class:`requests.Session` (useful for tests
        and connection reuse).
    """

    def __init__(
        self,
        url: str,
        *,
        chunk_bytes: int = _DEFAULT_CHUNK_BYTES,
        timeout_s: float = _DEFAULT_TIMEOUT_S,
        max_retries: int = 2,
        headers: Mapping[str, str] | None = None,
        session: requests.Session | None = None,
    ) -> None:
        if not url:
            raise ValueError("url must be a non-empty string")
        if chunk_bytes <= 0:
            raise ValueError("chunk_bytes must be positive")
        self._url = url
        self._chunk_bytes = chunk_bytes
        self._timeout_s = timeout_s
        self._max_retries = max(0, max_retries)
        self._headers = dict(headers) if headers else {}
        self._session = session
        self._response: requests.Response | None = None
        self._content_length: int | None = None

    @property
    def content_length(self) -> int | None:
        """Content-Length in bytes if the server reported it, else ``None``.

        Only populated after the stream has been opened.
        """
        return self._content_length

    # ------------------------------------------------------------------
    # Streaming interfaces
    # ------------------------------------------------------------------

    def iter_chunks(self) -> Iterator[bytes]:
        """Yield the response body in ``chunk_bytes``-sized byte chunks.

        Opens the connection lazily on first iteration.  Validates the HTTP
        status before yielding any data.
        """
        response = self._open_response()
        safe = _safe_url(self._url)
        log.info("streaming source url=%s content_length=%s", safe, self._content_length)
        try:
            for chunk in response.iter_content(chunk_size=self._chunk_bytes):
                if chunk:
                    yield chunk
        except requests.RequestException as exc:
            raise HttpStreamError(f"stream interrupted for {safe}: {exc}") from exc
        finally:
            response.close()
            self._response = None

    def open(self) -> _HttpStreamReader:
        """Return a file-like reader over the streamed body.

        The reader exposes ``read(size)`` and is suitable for
        ``boto3.upload_fileobj``.  Use as a context manager to ensure the
        underlying connection is released::

            with HttpStreamExtractor(url).open() as body:
                s3.upload_fileobj(body, bucket, key)
        """
        return _HttpStreamReader(self.iter_chunks())

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _open_response(self) -> requests.Response:
        """Establish the streaming GET with retry on transient setup errors."""
        safe = _safe_url(self._url)
        session = self._session or requests
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                response = session.get(
                    self._url,
                    stream=True,
                    timeout=self._timeout_s,
                    headers=self._headers or None,
                )
            except requests.RequestException as exc:
                last_exc = exc
                log.warning(
                    "connection attempt %d/%d failed for url=%s: %s",
                    attempt + 1,
                    self._max_retries + 1,
                    safe,
                    exc,
                )
                continue

            # Validate status.
            if response.status_code >= 400:
                status = response.status_code
                response.close()
                raise HttpStreamError(
                    f"HTTP {status} while streaming {safe}"
                )

            self._response = response
            cl = response.headers.get("Content-Length")
            self._content_length = int(cl) if cl and cl.isdigit() else None
            return response

        raise HttpStreamError(
            f"failed to open stream for {safe} after "
            f"{self._max_retries + 1} attempt(s): {last_exc}"
        ) from last_exc


# Convenience alias — the eventual production source is a Zenodo archive URL,
# but the implementation is a generic HTTP streamer.
ZenodoStreamExtractor = HttpStreamExtractor
