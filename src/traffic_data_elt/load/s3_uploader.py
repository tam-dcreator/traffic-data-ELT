"""S3 uploader for the V2 Bronze layer.

Uploads a streaming source to an existing S3 bucket under the Bronze prefix.
Uses ``boto3``'s managed transfer (``upload_fileobj`` + ``TransferConfig``),
which performs multipart uploads automatically for large objects while keeping
memory bounded.

Design
------
- Accepts any file-like object exposing ``read(size)`` — e.g. the
  :class:`~traffic_data_elt.extract.zenodo._HttpStreamReader`.  The complete
  source is never buffered in memory or staged on local disk.
- boto3's managed uploader aborts its own in-progress multipart upload if the
  transfer raises, so partial objects are not left behind.  As an extra safety
  net for interrupted uploads that predate this run, a helper is provided to
  abort stale multipart uploads for a key prefix.
- Never creates the bucket.  Never logs credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import IO, TYPE_CHECKING

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import BotoCoreError, ClientError

from traffic_data_elt.config import AwsConfig
from traffic_data_elt.utils import get_logger

if TYPE_CHECKING:  # pragma: no cover - typing only
    from mypy_boto3_s3 import S3Client

log = get_logger(__name__)


class S3UploadError(RuntimeError):
    """Raised when an S3 upload cannot be completed."""


@dataclass(frozen=True)
class UploadResult:
    """Outcome metadata for a completed (or attempted) upload."""

    bucket: str
    key: str
    bytes_transferred: int
    status: str
    etag: str | None = None


class S3Uploader:
    """Upload streaming sources to S3 under the Bronze prefix.

    Parameters
    ----------
    aws:
        AWS/S3 configuration (region, bucket, prefix, multipart tuning).
    client:
        Optional pre-built boto3 S3 client.  When omitted, a client is created
        from the configured region using boto3's standard credential chain.
    """

    def __init__(self, aws: AwsConfig, client: S3Client | None = None) -> None:
        self._aws = aws
        self._client = client or boto3.client("s3", region_name=aws.region)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def upload_stream(
        self,
        body: IO[bytes],
        *key_parts: str,
        content_type: str | None = None,
    ) -> UploadResult:
        """Stream *body* to ``bronze/<key_parts...>`` in the configured bucket.

        Parameters
        ----------
        body:
            File-like object with a ``read(size)`` method.  Streamed to S3 in
            multipart chunks without full buffering.
        key_parts:
            Path segments appended under the Bronze prefix to form the object
            key.
        content_type:
            Optional MIME type stored as object metadata.

        Returns
        -------
        UploadResult
            Bucket, key, bytes transferred, status, and ETag (when available).

        Raises
        ------
        S3UploadError
            If the transfer fails.  boto3 aborts its own partial multipart
            upload before this is raised.
        """
        key = self._aws.bronze_key(*key_parts)
        bucket = self._aws.bucket

        transfer_config = TransferConfig(
            multipart_threshold=self._aws.multipart_threshold_bytes,
            multipart_chunksize=self._aws.multipart_chunk_bytes,
            use_threads=True,
        )

        counter = _CountingReader(body)
        extra_args: dict[str, str] = {}
        if content_type:
            extra_args["ContentType"] = content_type

        log.info("uploading to s3 bucket=%s key=%s", bucket, key)
        try:
            self._client.upload_fileobj(
                counter,
                bucket,
                key,
                ExtraArgs=extra_args or None,
                Config=transfer_config,
            )
        except (BotoCoreError, ClientError) as exc:
            # boto3 aborts its own in-progress multipart upload on failure.
            log.error(
                "upload failed bucket=%s key=%s bytes=%d: %s",
                bucket,
                key,
                counter.bytes_read,
                exc,
            )
            raise S3UploadError(
                f"failed to upload to s3://{bucket}/{key}: {exc}"
            ) from exc

        etag = self._head_etag(bucket, key)
        log.info(
            "upload complete bucket=%s key=%s bytes=%d etag=%s",
            bucket,
            key,
            counter.bytes_read,
            etag,
        )
        return UploadResult(
            bucket=bucket,
            key=key,
            bytes_transferred=counter.bytes_read,
            status="success",
            etag=etag,
        )

    def abort_incomplete_uploads(self, key_prefix: str) -> int:
        """Abort stale multipart uploads under *key_prefix*.

        Useful for cleaning up after a previously interrupted transfer.  Returns
        the number of uploads aborted.  Best-effort: individual abort failures
        are logged and skipped rather than raised.
        """
        bucket = self._aws.bucket
        aborted = 0
        try:
            resp = self._client.list_multipart_uploads(
                Bucket=bucket, Prefix=key_prefix
            )
        except (BotoCoreError, ClientError) as exc:
            log.warning("could not list multipart uploads for %s: %s", key_prefix, exc)
            return 0

        for upload in resp.get("Uploads", []) or []:
            key = upload.get("Key")
            upload_id = upload.get("UploadId")
            if not key or not upload_id:
                continue
            try:
                self._client.abort_multipart_upload(
                    Bucket=bucket, Key=key, UploadId=upload_id
                )
                aborted += 1
                log.info("aborted stale multipart upload key=%s", key)
            except (BotoCoreError, ClientError) as exc:
                log.warning("failed to abort multipart upload key=%s: %s", key, exc)

        return aborted

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _head_etag(self, bucket: str, key: str) -> str | None:
        """Return the object ETag via HEAD, or ``None`` if unavailable."""
        try:
            head = self._client.head_object(Bucket=bucket, Key=key)
        except (BotoCoreError, ClientError) as exc:
            log.warning("could not HEAD s3://%s/%s: %s", bucket, key, exc)
            return None
        etag = head.get("ETag")
        return etag.strip('"') if isinstance(etag, str) else None


class _CountingReader:
    """Wrap a file-like object and count bytes read through it.

    Delegates ``read`` to the wrapped object so boto3's managed transfer sees a
    normal stream, while tracking total bytes transferred for result metadata.
    Does not buffer the stream.
    """

    def __init__(self, wrapped: IO[bytes]) -> None:
        self._wrapped = wrapped
        self._bytes_read = 0

    @property
    def bytes_read(self) -> int:
        return self._bytes_read

    def read(self, size: int = -1) -> bytes:
        data = self._wrapped.read(size)
        if data:
            self._bytes_read += len(data)
        return data
