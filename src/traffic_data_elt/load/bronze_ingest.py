"""Resumable ranged-multipart Bronze ingestion.

Streams a large remote source archive (e.g. the ~15.76 GB pNEUMA ZIP) into S3
Bronze as a sequence of bounded HTTP Range GET -> S3 UploadPart operations, with
durable per-upload checkpointing so that:

  * a mid-stream network failure only costs the CURRENT part (not the whole
    multi-hour transfer),
  * an Airflow task/process retry RESUMES from the next missing part rather than
    restarting from byte zero,
  * a completed, validated Bronze object is idempotently reused,
  * memory stays bounded (never a full-part or full-archive bytes object), and
  * no full local copy of the archive is ever written to disk.

Pipeline::

    Zenodo (HTTP Range GET, 206)
        -> bounded per-part spooled buffer (<= ~1 part on disk, deleted after)
        -> S3 UploadPart
        -> checkpoint completed part (small S3 metadata object)
        -> next range
        -> CompleteMultipartUpload
        -> HEAD validation
        -> checkpoint cleanup

This module is split into:

  * pure range planning (:func:`plan_parts`, :class:`PartRange`) — no I/O,
  * HTTP range fetching with strict 206/Content-Range validation,
  * explicit S3 multipart control + checkpoint object,
  * the :class:`BronzeRangedUploader` orchestrator.

Nothing here logs credentials, signed URLs, cookies, or Authorization headers.
"""

from __future__ import annotations

import json
import math
import os
import random
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import requests

from traffic_data_elt.config import BronzeTransferConfig
from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_s3 import S3Client

log = get_logger(__name__)

# Checkpoint format version — bump if the on-disk/S3 schema changes.
_CHECKPOINT_VERSION = 1
# Suffix appended to the Bronze key to form the checkpoint control object key.
_CHECKPOINT_SUFFIX = ".mpu-checkpoint.json"


# ---------------------------------------------------------------------------
# Error taxonomy (drives retry vs resume vs fail-safe decisions)
# ---------------------------------------------------------------------------


class BronzeIngestError(RuntimeError):
    """Base class for Bronze ingestion errors."""


class BronzeTransientError(BronzeIngestError):
    """A transient failure for the CURRENT part — retry that part.

    Examples: connection reset, read timeout, 429, 5xx, partial ranged
    response, IncompleteRead.
    """


class BronzeFatalError(BronzeIngestError):
    """A non-recoverable condition — fail safely, do NOT publish Bronze.

    Examples: range not supported unexpectedly (200 to a ranged request),
    source size changed, wrong Content-Range, checkpoint belongs to a different
    source, invalid source metadata.
    """


@dataclass(frozen=True)
class PartRange:
    """A single S3 multipart part / HTTP byte range.

    Attributes
    ----------
    part_number:
        1-based S3 multipart part number (S3 requires part numbers 1..10000).
    start_byte:
        First byte offset of the range (inclusive, 0-based).
    end_byte:
        Last byte offset of the range (inclusive) — the value used directly in
        the HTTP ``Range: bytes=<start>-<end>`` header.
    expected_length:
        Number of bytes in this part (``end_byte - start_byte + 1``).
    """

    part_number: int
    start_byte: int
    end_byte: int
    expected_length: int

    @property
    def http_range_header(self) -> str:
        """Return the exact ``bytes=<start>-<end>`` HTTP Range header value."""
        return f"bytes={self.start_byte}-{self.end_byte}"

    @property
    def content_range_total(self) -> str:
        """The ``<start>-<end>`` interval as it appears in a 206 Content-Range."""
        return f"{self.start_byte}-{self.end_byte}"


def plan_parts(source_size: int, part_size_bytes: int) -> list[PartRange]:
    """Return the ordered list of :class:`PartRange` covering *source_size*.

    Pure function — no I/O. Part numbers start at 1. Every part is exactly
    *part_size_bytes* except the final part, which is truncated to the real end
    of the source. Contiguous and gapless: part *n* ends exactly one byte before
    part *n+1* begins, and the last part ends at ``source_size - 1``.

    Parameters
    ----------
    source_size:
        Total size of the source object in bytes (must be > 0).
    part_size_bytes:
        Target part size in bytes (must be > 0). S3 requires >= 5 MiB for all
        but the last part; that policy is enforced by the caller/config, not
        here, so this stays a pure arithmetic helper.

    Returns
    -------
    list[PartRange]
        One entry per part, in ascending part-number order.

    Raises
    ------
    ValueError
        If *source_size* or *part_size_bytes* is not positive, or if the plan
        would exceed the S3 hard limit of 10,000 parts.
    """
    if source_size <= 0:
        raise ValueError(f"source_size must be positive, got {source_size}")
    if part_size_bytes <= 0:
        raise ValueError(f"part_size_bytes must be positive, got {part_size_bytes}")

    part_count = math.ceil(source_size / part_size_bytes)
    if part_count > 10_000:
        raise ValueError(
            f"plan requires {part_count} parts, exceeding the S3 limit of 10000; "
            f"increase part_size_bytes"
        )

    parts: list[PartRange] = []
    for i in range(part_count):
        start = i * part_size_bytes
        end = min(start + part_size_bytes, source_size) - 1
        parts.append(
            PartRange(
                part_number=i + 1,
                start_byte=start,
                end_byte=end,
                expected_length=end - start + 1,
            )
        )
    return parts


def expected_part_count(source_size: int, part_size_bytes: int) -> int:
    """Return the number of parts a *source_size* / *part_size_bytes* plan yields."""
    if source_size <= 0 or part_size_bytes <= 0:
        raise ValueError("source_size and part_size_bytes must be positive")
    return math.ceil(source_size / part_size_bytes)


# ---------------------------------------------------------------------------
# Helpers: log-safe URL + canonical filename
# ---------------------------------------------------------------------------


def safe_url(url: str) -> str:
    """Return a log-safe ``scheme://host/path`` (no query string / credentials)."""
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.hostname or ''}{p.path}"
    except Exception:  # noqa: BLE001 - log-safety guard must never raise
        return "<unparseable-url>"


def derive_object_name(source_url: str) -> str:
    """Return the archive filename from the URL PATH only (query ignored).

    ``derive_object_name('https://z/records/1/files/pNEUMA_dataset.zip?download=1')``
    → ``'pNEUMA_dataset.zip'``. The full URL (including ``?download=1``) is used
    for the HTTP request; only the S3 key derivation strips the query.
    """
    if not source_url:
        raise ValueError("source_url is required")
    path = urlparse(source_url).path
    name = path.rstrip("/").split("/")[-1] if path else ""
    if not name:
        raise ValueError(f"cannot derive object name from URL path {path!r}")
    return name


def _human(n: float) -> str:
    """Human-readable bytes (GiB/MiB/KiB)."""
    for unit, div in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if n >= div:
            return f"{n / div:.2f}{unit}"
    return f"{int(n)}B"


# ---------------------------------------------------------------------------
# Source metadata preflight (no archive download)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SourceMetadata:
    """Non-secret source metadata resolved before any body transfer."""

    url: str
    size: int
    object_name: str
    accept_ranges: bool
    last_modified: str | None = None
    etag: str | None = None


def preflight_source(
    source_url: str,
    *,
    connect_timeout_s: float = 30.0,
    read_timeout_s: float = 60.0,
    session: requests.Session | None = None,
) -> SourceMetadata:
    """Resolve source size / filename / range support WITHOUT downloading it.

    Strategy: a ``Range: bytes=0-0`` GET. A range-supporting server answers
    ``206`` with ``Content-Range: bytes 0-0/<total>``, which yields the total
    size and proves range support in one cheap request (and matches how Zenodo
    range support was verified). Falls back to reading ``Content-Length`` +
    ``Accept-Ranges`` if the server answers ``200``.

    Raises
    ------
    BronzeFatalError
        If size cannot be determined or the response is invalid.
    BronzeTransientError
        On transient network errors (caller may retry the preflight).
    """
    sess = session or requests
    safe = safe_url(source_url)
    try:
        resp = sess.get(
            source_url,
            headers={"Range": "bytes=0-0"},
            stream=True,
            timeout=(connect_timeout_s, read_timeout_s),
        )
    except (requests.ConnectionError, requests.Timeout) as exc:
        raise BronzeTransientError(f"preflight network error for {safe}: {exc}") from exc
    except requests.RequestException as exc:
        raise BronzeFatalError(f"preflight failed for {safe}: {exc}") from exc

    try:
        status = resp.status_code
        headers = resp.headers
        object_name = derive_object_name(source_url)

        if status == 206:
            content_range = headers.get("Content-Range", "")
            # Expected form: "bytes 0-0/<total>"
            total = _parse_content_range_total(content_range)
            if total is None or total <= 0:
                raise BronzeFatalError(
                    f"preflight 206 with unusable Content-Range {content_range!r} for {safe}"
                )
            return SourceMetadata(
                url=source_url, size=total, object_name=object_name,
                accept_ranges=True,
                last_modified=headers.get("Last-Modified"),
                etag=_clean_etag(headers.get("ETag")),
            )

        if status == 200:
            # Server ignored Range. Only usable if it declares a length; range
            # support is then whatever Accept-Ranges says (often none).
            cl = headers.get("Content-Length")
            if not (cl and cl.isdigit() and int(cl) > 0):
                raise BronzeFatalError(
                    f"preflight 200 without usable Content-Length for {safe}"
                )
            accept = headers.get("Accept-Ranges", "").lower() == "bytes"
            return SourceMetadata(
                url=source_url, size=int(cl), object_name=object_name,
                accept_ranges=accept,
                last_modified=headers.get("Last-Modified"),
                etag=_clean_etag(headers.get("ETag")),
            )

        if status in (429, 500, 502, 503, 504):
            raise BronzeTransientError(f"preflight HTTP {status} for {safe}")
        raise BronzeFatalError(f"preflight HTTP {status} for {safe}")
    finally:
        resp.close()


def _parse_content_range_total(content_range: str) -> int | None:
    """Parse the total size from a ``Content-Range: bytes <s>-<e>/<total>`` value."""
    # Form: "bytes 0-0/15759302461"
    if not content_range or "/" not in content_range:
        return None
    total = content_range.rsplit("/", 1)[-1].strip()
    return int(total) if total.isdigit() else None


def _parse_content_range_interval(content_range: str) -> tuple[int, int, int] | None:
    """Parse ``(start, end, total)`` from a full ``Content-Range`` header value."""
    # Form: "bytes 536870912-1073741823/15759302461"
    try:
        _, rest = content_range.split(" ", 1)
        interval, total = rest.split("/", 1)
        start_s, end_s = interval.split("-", 1)
        return int(start_s), int(end_s), int(total)
    except (ValueError, AttributeError):
        return None


def _clean_etag(etag: str | None) -> str | None:
    return etag.strip('"') if isinstance(etag, str) else None


# ---------------------------------------------------------------------------
# HTTP range fetch (strict validation) into a bounded per-part temp file
# ---------------------------------------------------------------------------


def fetch_range_to_file(
    source_url: str,
    part: PartRange,
    dest_file,
    *,
    source_size: int,
    http_chunk_bytes: int,
    connect_timeout_s: float,
    read_timeout_s: float,
    session: requests.Session | None = None,
) -> int:
    """Fetch one *part* via HTTP Range GET into *dest_file* (a writable binary file).

    Streams the response body in bounded ``http_chunk_bytes`` chunks so a full
    part is never held in a Python ``bytes`` object — memory stays at roughly one
    chunk regardless of part size. The bytes land in *dest_file* (a per-part temp
    file), which is the only place a full part exists, and only one at a time.

    Strict validation (per the incident spec):
      * status MUST be ``206`` — a ``200`` to a ranged request STOPS as fatal
        (the server ignored the range; silently accepting it would corrupt the
        multipart assembly),
      * ``Content-Range`` interval MUST equal the requested ``start-end`` and its
        total MUST equal *source_size*,
      * bytes actually written MUST equal ``part.expected_length``.

    Returns
    -------
    int
        Bytes written (== ``part.expected_length`` on success).

    Raises
    ------
    BronzeTransientError
        Transient network failures (timeouts, connection/chunked/protocol
        errors, incomplete reads, 429, 5xx) — the caller retries this part.
    BronzeFatalError
        Range not honoured (200), wrong Content-Range, or size mismatch.
    """
    sess = session or requests
    safe = safe_url(source_url)
    headers = {"Range": part.http_range_header}
    try:
        resp = sess.get(
            source_url, headers=headers, stream=True,
            timeout=(connect_timeout_s, read_timeout_s),
        )
    except (requests.ConnectionError, requests.Timeout) as exc:
        raise BronzeTransientError(
            f"part {part.part_number}: connect/read error: {exc}"
        ) from exc
    except requests.RequestException as exc:
        raise BronzeTransientError(
            f"part {part.part_number}: request error: {exc}"
        ) from exc

    try:
        status = resp.status_code
        if status == 200:
            raise BronzeFatalError(
                f"part {part.part_number}: server returned 200 to a ranged request "
                f"(range not honoured) for {safe}; refusing to treat as success"
            )
        if status in (429, 500, 502, 503, 504):
            raise BronzeTransientError(f"part {part.part_number}: HTTP {status}")
        if status != 206:
            raise BronzeFatalError(f"part {part.part_number}: unexpected HTTP {status}")

        # Validate Content-Range exactly matches the requested interval + total.
        cr = resp.headers.get("Content-Range", "")
        parsed = _parse_content_range_interval(cr)
        if parsed is None:
            raise BronzeFatalError(
                f"part {part.part_number}: missing/invalid Content-Range {cr!r}"
            )
        got_start, got_end, got_total = parsed
        if (got_start, got_end) != (part.start_byte, part.end_byte):
            raise BronzeFatalError(
                f"part {part.part_number}: Content-Range interval {got_start}-{got_end} "
                f"!= requested {part.start_byte}-{part.end_byte}"
            )
        if got_total != source_size:
            raise BronzeFatalError(
                f"part {part.part_number}: Content-Range total {got_total} "
                f"!= preflight source size {source_size} (source changed?)"
            )

        # Stream body in bounded chunks into the destination file.
        written = 0
        try:
            for chunk in resp.iter_content(chunk_size=http_chunk_bytes):
                if chunk:
                    dest_file.write(chunk)
                    written += len(chunk)
        except (requests.ConnectionError, requests.Timeout,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.StreamConsumedError) as exc:
            raise BronzeTransientError(
                f"part {part.part_number}: mid-stream error after {written} bytes: {exc}"
            ) from exc
        except requests.RequestException as exc:
            raise BronzeTransientError(
                f"part {part.part_number}: mid-stream request error after "
                f"{written} bytes: {exc}"
            ) from exc

        if written != part.expected_length:
            # Truncated / over-long ranged response — transient; retry the part.
            raise BronzeTransientError(
                f"part {part.part_number}: received {written} bytes != expected "
                f"{part.expected_length} (truncated response)"
            )
        return written
    finally:
        resp.close()


def _retry_sleep_seconds(attempt: int, retry_after: float | None) -> float:
    """Exponential backoff with jitter; honour a server ``Retry-After`` if larger.

    *attempt* is 1-based. Base backoff = ``2**(attempt-1)`` seconds, capped, with
    full jitter. If the server supplied a ``Retry-After``, use the max of it and
    the computed backoff.
    """
    base = min(2 ** (attempt - 1), 30)
    jittered = random.uniform(0, base)
    if retry_after is not None and retry_after > jittered:
        return retry_after
    return jittered


def _retry_after_seconds(exc: BaseException) -> float | None:
    """Extract a ``Retry-After`` header (seconds) from a requests error, if any."""
    resp = getattr(exc, "response", None)
    if resp is None:
        return None
    val = resp.headers.get("Retry-After") if hasattr(resp, "headers") else None
    if val and str(val).isdigit():
        return float(val)
    return None


# ---------------------------------------------------------------------------
# Checkpoint (small S3 control object; metadata only — no bytes, no secrets)
# ---------------------------------------------------------------------------


@dataclass
class CompletedPart:
    """A part already uploaded to S3 (for CompleteMultipartUpload + resume)."""

    part_number: int
    etag: str
    start: int
    end: int

    def to_dict(self) -> dict:
        return {"part_number": self.part_number, "etag": self.etag,
                "start": self.start, "end": self.end}

    @classmethod
    def from_dict(cls, d: dict) -> CompletedPart:
        return cls(part_number=int(d["part_number"]), etag=str(d["etag"]),
                   start=int(d["start"]), end=int(d["end"]))


@dataclass
class Checkpoint:
    """Durable resume metadata for one in-progress Bronze multipart upload.

    Persisted as a small JSON object in S3 next to the Bronze key. Contains ONLY
    non-secret identifiers and per-part ETags — never source bytes, never
    credentials.
    """

    version: int
    source_url: str          # log-safe (query stripped) identity
    source_size: int
    part_size: int
    bucket: str
    key: str
    upload_id: str
    source_last_modified: str | None = None
    source_etag: str | None = None
    completed_parts: list[CompletedPart] = field(default_factory=list)
    updated_at: str = ""

    def to_json(self) -> str:
        return json.dumps({
            "version": self.version,
            "source_url": self.source_url,
            "source_size": self.source_size,
            "part_size": self.part_size,
            "bucket": self.bucket,
            "key": self.key,
            "upload_id": self.upload_id,
            "source_last_modified": self.source_last_modified,
            "source_etag": self.source_etag,
            "completed_parts": [p.to_dict() for p in
                                sorted(self.completed_parts, key=lambda x: x.part_number)],
            "updated_at": self.updated_at,
        }, indent=2)

    @classmethod
    def from_json(cls, text: str) -> Checkpoint:
        d = json.loads(text)
        return cls(
            version=int(d["version"]), source_url=d["source_url"],
            source_size=int(d["source_size"]), part_size=int(d["part_size"]),
            bucket=d["bucket"], key=d["key"], upload_id=d["upload_id"],
            source_last_modified=d.get("source_last_modified"),
            source_etag=d.get("source_etag"),
            completed_parts=[CompletedPart.from_dict(p)
                             for p in d.get("completed_parts", [])],
            updated_at=d.get("updated_at", ""),
        )

    def completed_part_numbers(self) -> set[int]:
        return {p.part_number for p in self.completed_parts}


def checkpoint_key_for(bronze_key: str) -> str:
    """Return the S3 key of the checkpoint control object for *bronze_key*."""
    return f"{bronze_key}{_CHECKPOINT_SUFFIX}"


class CheckpointStore:
    """Read/write/delete the checkpoint control object in S3.

    The checkpoint lives at ``<bronze_key>.mpu-checkpoint.json``. It is small
    JSON metadata; storing it in S3 (not XCom / local disk) makes resume work
    across Airflow worker/process restarts.
    """

    def __init__(self, client: S3Client, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def load(self, bronze_key: str) -> Checkpoint | None:
        from botocore.exceptions import ClientError
        ckey = checkpoint_key_for(bronze_key)
        try:
            obj = self._client.get_object(Bucket=self._bucket, Key=ckey)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchKey", "404", "NotFound"):
                return None
            raise
        text = obj["Body"].read().decode("utf-8")
        return Checkpoint.from_json(text)

    def save(self, ckpt: Checkpoint) -> None:
        ckpt.updated_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        ckey = checkpoint_key_for(ckpt.key)
        self._client.put_object(
            Bucket=self._bucket, Key=ckey,
            Body=ckpt.to_json().encode("utf-8"),
            ContentType="application/json",
        )

    def delete(self, bronze_key: str) -> None:
        ckey = checkpoint_key_for(bronze_key)
        self._client.delete_object(Bucket=self._bucket, Key=ckey)


# ---------------------------------------------------------------------------
# Result + orchestrator
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BronzeIngestResult:
    """Outcome metadata for a Bronze ingestion (safe to return via XCom)."""

    bucket: str
    key: str
    source_size: int
    object_size: int
    disposition: str        # "reused" | "resumed" | "new"
    parts_count: int
    duration_s: float


class BronzeRangedUploader:
    """Resumable ranged-multipart Bronze uploader.

    Orchestrates: completed-object idempotency, checkpoint load + S3
    reconciliation (S3 is authoritative), create/resume multipart, per-part
    ranged fetch → UploadPart with independent retries, checkpoint after each
    part, CompleteMultipartUpload, final HEAD validation, checkpoint cleanup.

    All AWS calls go through an injected boto3 client; HTTP through an injected
    ``requests.Session`` — both mockable for unit tests with no live services.

    Max local disk footprint: ~one part (the current per-part temp file), never
    the full archive. Max memory: ~one ``http_chunk_bytes`` chunk.
    """

    def __init__(
        self,
        client: S3Client,
        bucket: str,
        *,
        config: BronzeTransferConfig | None = None,
        session: requests.Session | None = None,
        temp_dir: str | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = client
        self._bucket = bucket
        self._cfg = config or BronzeTransferConfig()
        self._session = session or requests.Session()
        self._temp_dir = temp_dir
        self._sleep = sleep
        self._store = CheckpointStore(client, bucket)

    # -- public API ---------------------------------------------------------

    def ingest(self, source_url: str, bronze_key: str) -> BronzeIngestResult:
        """Ingest *source_url* into ``s3://<bucket>/<bronze_key>`` resumably."""
        started = time.monotonic()
        safe = safe_url(source_url)
        log.info("Bronze ingest start key=%s source=%s", bronze_key, safe)

        # 1. Preflight source metadata (no download).
        #    If preflight is transiently unavailable (e.g. Zenodo 504) BUT a
        #    compatible resumable checkpoint already exists — meaning a prior
        #    attempt successfully preflighted this exact source and banked parts
        #    — fall back to the checkpoint's known metadata so a transient
        #    preflight outage cannot block resuming already-completed work.
        try:
            meta = self._preflight_with_retry(source_url)
        except BronzeTransientError:
            meta = self._source_meta_from_checkpoint(source_url, bronze_key)
            if meta is None:
                raise  # no safe fallback — re-raise the transient preflight error
            log.warning(
                "preflight transiently unavailable; resuming from checkpoint "
                "metadata (source_size=%d) for key=%s", meta.size, bronze_key,
            )
        if not meta.accept_ranges:
            raise BronzeFatalError(
                f"source does not support HTTP range requests: {safe}"
            )

        # 2. Completed-object idempotency: reuse a valid existing object.
        existing = self._head_size(bronze_key)
        if existing is not None:
            if existing == meta.size:
                log.info("Bronze reuse key=%s size=%d (matches source)", bronze_key, existing)
                # A completed object must not carry a live resume checkpoint.
                self._safe_delete_checkpoint(bronze_key)
                return BronzeIngestResult(
                    self._bucket, bronze_key, meta.size, existing,
                    "reused", 0, time.monotonic() - started,
                )
            raise BronzeFatalError(
                f"existing object size {existing} != source size {meta.size} "
                f"for key={bronze_key}; refusing to overwrite/misidentify"
            )

        # 3. Plan parts.
        parts = plan_parts(meta.size, self._cfg.part_size_bytes)

        # 4. Resume an existing compatible checkpoint, else create a new upload.
        ckpt, disposition = self._resume_or_create(bronze_key, meta, parts)

        # 5. Upload missing parts.
        done = ckpt.completed_part_numbers()
        total_parts = len(parts)
        completed_bytes = sum(p.expected_length for p in parts
                              if p.part_number in done)
        for part in parts:
            if part.part_number in done:
                continue
            etag = self._upload_one_part(
                source_url, meta, part, ckpt, total_parts,
                completed_bytes, started,
            )
            ckpt.completed_parts.append(
                CompletedPart(part.part_number, etag, part.start_byte, part.end_byte)
            )
            self._store.save(ckpt)
            completed_bytes += part.expected_length

        # 6. Complete the multipart upload (parts sorted by number).
        self._complete(ckpt)

        # 7. Validate the finished object.
        final_size = self._head_size(bronze_key)
        if final_size != meta.size:
            raise BronzeFatalError(
                f"completed object size {final_size} != source size {meta.size} "
                f"for key={bronze_key}"
            )

        # 8. Remove the checkpoint — no live resume for a completed object.
        self._safe_delete_checkpoint(bronze_key)

        duration = time.monotonic() - started
        log.info(
            "Bronze ingest complete key=%s size=%d parts=%d disposition=%s duration=%.1fs",
            bronze_key, final_size, total_parts, disposition, duration,
        )
        return BronzeIngestResult(
            self._bucket, bronze_key, meta.size, final_size,
            disposition, total_parts, duration,
        )

    # -- preflight / head ---------------------------------------------------

    def _preflight_with_retry(self, source_url: str) -> SourceMetadata:
        attempts = self._cfg.part_retries
        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return preflight_source(
                    source_url,
                    connect_timeout_s=self._cfg.connect_timeout_s,
                    read_timeout_s=self._cfg.read_timeout_s,
                    session=self._session,
                )
            except BronzeTransientError as exc:
                last = exc
                if attempt < attempts:
                    self._sleep(_retry_sleep_seconds(attempt, _retry_after_seconds(exc)))
        raise BronzeTransientError(f"preflight failed after {attempts} attempts: {last}")

    def _source_meta_from_checkpoint(
        self, source_url: str, bronze_key: str,
    ) -> SourceMetadata | None:
        """Reconstruct source metadata from a compatible existing checkpoint.

        Used only as a fallback when live preflight is transiently unavailable.
        Returns ``None`` (no fallback) unless a checkpoint exists that:

          * targets this exact bucket/key,
          * shares this source URL identity (log-safe form),
          * uses the current configured part size, and
          * still has a live multipart upload in S3.

        In that case the source size was already validated by the preflight of a
        prior attempt (it is what the checkpoint's parts were planned against),
        so it is safe to resume without a fresh preflight. Range support is
        implied — the completed parts were fetched via ranged 206 responses.
        """
        ckpt = self._store.load(bronze_key)
        if ckpt is None:
            return None
        if ckpt.bucket != self._bucket or ckpt.key != bronze_key:
            return None
        if ckpt.source_url != safe_url(source_url):
            return None
        if ckpt.part_size != self._cfg.part_size_bytes:
            return None
        if not self._multipart_exists(ckpt.key, ckpt.upload_id):
            return None
        return SourceMetadata(
            url=source_url, size=ckpt.source_size,
            object_name=derive_object_name(source_url),
            accept_ranges=True,
            last_modified=ckpt.source_last_modified,
            etag=ckpt.source_etag,
        )

    def _head_size(self, key: str) -> int | None:
        from botocore.exceptions import ClientError
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
        return int(head.get("ContentLength", 0))

    # -- resume / reconcile -------------------------------------------------

    def _resume_or_create(
        self, bronze_key: str, meta: SourceMetadata, parts: list[PartRange],
    ) -> tuple[Checkpoint, str]:
        """Return (checkpoint, disposition) — resume if compatible, else create."""
        ckpt = self._store.load(bronze_key)
        if ckpt is not None:
            incompatible = self._checkpoint_incompatibility(ckpt, meta, bronze_key)
            if incompatible is not None:
                # Non-recoverable: abort the incompatible upload + drop checkpoint.
                log.warning(
                    "incompatible checkpoint for key=%s: %s; aborting + recreating",
                    bronze_key, incompatible,
                )
                self._safe_abort(ckpt.key, ckpt.upload_id)
                self._safe_delete_checkpoint(bronze_key)
                ckpt = None
            elif not self._multipart_exists(ckpt.key, ckpt.upload_id):
                log.warning(
                    "checkpoint upload_id no longer exists in S3 for key=%s; recreating",
                    bronze_key,
                )
                self._safe_delete_checkpoint(bronze_key)
                ckpt = None
            else:
                # Reconcile checkpoint against authoritative S3 ListParts.
                self._reconcile(ckpt)
                log.info(
                    "Bronze resume key=%s upload_id=... completed_parts=%d/%d",
                    bronze_key, len(ckpt.completed_parts), len(parts),
                )
                return ckpt, "resumed"

        # Create a fresh multipart upload.
        upload_id = self._client.create_multipart_upload(
            Bucket=self._bucket, Key=bronze_key,
        )["UploadId"]
        ckpt = Checkpoint(
            version=_CHECKPOINT_VERSION,
            source_url=safe_url(meta.url),
            source_size=meta.size,
            part_size=self._cfg.part_size_bytes,
            bucket=self._bucket,
            key=bronze_key,
            upload_id=upload_id,
            source_last_modified=meta.last_modified,
            source_etag=meta.etag,
            completed_parts=[],
        )
        self._store.save(ckpt)
        log.info("Bronze new upload key=%s parts=%d part_size=%s",
                 bronze_key, len(parts), _human(self._cfg.part_size_bytes))
        return ckpt, "new"

    def _checkpoint_incompatibility(
        self, ckpt: Checkpoint, meta: SourceMetadata, bronze_key: str,
    ) -> str | None:
        """Return a reason string if *ckpt* is incompatible, else ``None``."""
        if ckpt.version != _CHECKPOINT_VERSION:
            return f"version {ckpt.version} != {_CHECKPOINT_VERSION}"
        if ckpt.bucket != self._bucket or ckpt.key != bronze_key:
            return "bucket/key mismatch"
        if ckpt.source_url != safe_url(meta.url):
            return "source_url identity mismatch"
        if ckpt.source_size != meta.size:
            return f"source_size {ckpt.source_size} != {meta.size}"
        if ckpt.part_size != self._cfg.part_size_bytes:
            return f"part_size {ckpt.part_size} != {self._cfg.part_size_bytes}"
        if meta.etag and ckpt.source_etag and meta.etag != ckpt.source_etag:
            return "source etag changed"
        return None

    def _reconcile(self, ckpt: Checkpoint) -> None:
        """Reconcile checkpoint completed-parts with authoritative S3 ListParts.

        S3 is the source of truth for what actually landed. Keep only parts that
        S3 confirms, matching ETags; drop any the checkpoint claims but S3 does
        not have (or whose ETag differs) so they are re-uploaded.
        """
        s3_parts = self._list_parts(ckpt.key, ckpt.upload_id)  # {num: etag}
        reconciled: list[CompletedPart] = []
        for p in ckpt.completed_parts:
            s3_etag = s3_parts.get(p.part_number)
            if s3_etag is not None and s3_etag == p.etag:
                reconciled.append(p)
            else:
                log.warning(
                    "reconcile: dropping part %d (checkpoint etag vs S3 mismatch/absent)",
                    p.part_number,
                )
        # Also adopt any parts S3 has that the checkpoint missed (belt & braces).
        known = {p.part_number for p in reconciled}
        for num, etag in s3_parts.items():
            if num not in known:
                # We do not know the byte range for an unexpected S3 part; only
                # adopt if it lines up with the plan. Recompute from part_size.
                start = (num - 1) * ckpt.part_size
                end = min(start + ckpt.part_size, ckpt.source_size) - 1
                reconciled.append(CompletedPart(num, etag, start, end))
        ckpt.completed_parts = sorted(reconciled, key=lambda x: x.part_number)
        self._store.save(ckpt)

    # -- s3 multipart primitives -------------------------------------------

    def _multipart_exists(self, key: str, upload_id: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self._client.list_parts(Bucket=self._bucket, Key=key,
                                    UploadId=upload_id, MaxParts=1)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchUpload", "404", "NotFound"):
                return False
            raise

    def _list_parts(self, key: str, upload_id: str) -> dict[int, str]:
        """Return ``{part_number: etag}`` for all uploaded parts (paginated)."""
        parts: dict[int, str] = {}
        marker = 0
        while True:
            resp = self._client.list_parts(
                Bucket=self._bucket, Key=key, UploadId=upload_id,
                PartNumberMarker=marker,
            )
            for p in resp.get("Parts", []) or []:
                parts[int(p["PartNumber"])] = _clean_etag(p.get("ETag")) or ""
            if resp.get("IsTruncated"):
                marker = int(resp.get("NextPartNumberMarker", 0))
                continue
            break
        return parts

    def _upload_one_part(
        self, source_url: str, meta: SourceMetadata, part: PartRange,
        ckpt: Checkpoint, total_parts: int, completed_bytes: int, started: float,
    ) -> str:
        """Fetch one ranged part into a bounded temp file and UploadPart, w/ retries."""
        attempts = self._cfg.part_retries
        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            # One bounded temp file per part; deleted immediately after success
            # or failure. Never the full archive on disk.
            # delete=False + explicit finally cleanup: the file is written by the
            # HTTP fetch, then re-read (seek 0) for upload_part, then unlinked in
            # ALL paths below. A `with` block would close/delete too early.
            tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
                prefix="bronze_part_", suffix=".tmp", dir=self._temp_dir, delete=False,
            )
            tmp_path = tmp.name
            try:
                p_started = time.monotonic()
                tmp.seek(0)
                tmp.truncate(0)
                written = fetch_range_to_file(
                    source_url, part, tmp,
                    source_size=meta.size,
                    http_chunk_bytes=self._cfg.http_chunk_bytes,
                    connect_timeout_s=self._cfg.connect_timeout_s,
                    read_timeout_s=self._cfg.read_timeout_s,
                    session=self._session,
                )
                tmp.flush()
                tmp.seek(0)
                resp = self._client.upload_part(
                    Bucket=self._bucket, Key=ckpt.key, UploadId=ckpt.upload_id,
                    PartNumber=part.part_number, Body=tmp,
                    ContentLength=written,
                )
                etag = _clean_etag(resp.get("ETag")) or ""
                self._log_progress(
                    ckpt.key, part, total_parts, completed_bytes + written,
                    meta.size, started, attempt,
                    "resume" if ckpt.completed_parts else "new",
                    time.monotonic() - p_started,
                )
                return etag
            except BronzeFatalError:
                raise  # non-recoverable; do not retry
            except (BronzeTransientError, requests.RequestException,
                    OSError) as exc:
                last = exc
                log.warning(
                    "part %d/%d attempt %d/%d failed: %s",
                    part.part_number, total_parts, attempt, attempts, exc,
                )
                if attempt < attempts:
                    self._sleep(_retry_sleep_seconds(attempt, _retry_after_seconds(exc)))
            finally:
                try:
                    tmp.close()
                finally:
                    if os.path.exists(tmp_path):
                        os.unlink(tmp_path)
        raise BronzeTransientError(
            f"part {part.part_number} failed after {attempts} attempts: {last}"
        )

    def _complete(self, ckpt: Checkpoint) -> None:
        parts_payload = [
            {"ETag": p.etag, "PartNumber": p.part_number}
            for p in sorted(ckpt.completed_parts, key=lambda x: x.part_number)
        ]
        self._client.complete_multipart_upload(
            Bucket=self._bucket, Key=ckpt.key, UploadId=ckpt.upload_id,
            MultipartUpload={"Parts": parts_payload},
        )

    def _safe_abort(self, key: str, upload_id: str) -> None:
        from botocore.exceptions import BotoCoreError, ClientError
        try:
            self._client.abort_multipart_upload(
                Bucket=self._bucket, Key=key, UploadId=upload_id,
            )
        except (BotoCoreError, ClientError) as exc:
            log.warning("abort_multipart_upload failed key=%s: %s", key, exc)

    def _safe_delete_checkpoint(self, bronze_key: str) -> None:
        from botocore.exceptions import BotoCoreError, ClientError
        try:
            self._store.delete(bronze_key)
        except (BotoCoreError, ClientError) as exc:
            log.warning("checkpoint delete failed key=%s: %s", bronze_key, exc)

    def _log_progress(
        self, key: str, part: PartRange, total_parts: int, completed_bytes: int,
        total_bytes: int, started: float, attempt: int, mode: str,
        part_seconds: float,
    ) -> None:
        elapsed = max(time.monotonic() - started, 1e-6)
        pct = 100.0 * completed_bytes / total_bytes if total_bytes else 0.0
        rate = completed_bytes / elapsed
        log.info(
            "Bronze progress key=%s part=%d/%d completed=%s/%s progress=%.1f%% "
            "rate=%s/s elapsed=%.0fs part_time=%.0fs attempt=%d mode=%s",
            key, part.part_number, total_parts,
            _human(completed_bytes), _human(total_bytes), pct,
            _human(rate), elapsed, part_seconds, attempt, mode,
        )
