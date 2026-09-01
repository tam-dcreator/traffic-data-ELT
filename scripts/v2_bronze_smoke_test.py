#!/usr/bin/env python3
"""Opt-in smoke test: stream a small HTTP object into S3 Bronze.

This is a developer-facing integration check.  It requires real AWS access and
is intentionally NOT part of the unit test suite.  It validates the end-to-end
path:

    small HTTP object -> HttpStreamExtractor -> S3Uploader -> bronze/test/

Usage
-----
    # Required environment (AWS credentials via the standard boto3 chain):
    set AWS_REGION and S3_BUCKET in v2_cloud/.env
    
    Or

    export AWS_REGION=eu-central-1
    export S3_BUCKET=your-existing-bucket

    # Optional overrides:
    export SMOKE_TEST_URL=https://example.com/small-file.txt
    export S3_BRONZE_PREFIX=bronze

    python scripts/v2_bronze_smoke_test.py

The script:
  1. Streams the source URL without staging it fully on disk.
  2. Uploads it to ``bronze/test/<filename>`` in the configured bucket.
  3. Verifies via HEAD that the object exists and has non-zero size.

It exits non-zero on any failure.  It does not download the object back.
"""

from __future__ import annotations

import os
import sys
from urllib.parse import urlparse

# Small, stable, public default source (~10 KB) for a lightweight check.
# Override with SMOKE_TEST_URL to use a different small file.
_DEFAULT_URL = "https://raw.githubusercontent.com/python/cpython/main/README.rst"


def _filename_from_url(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or "smoke_test.bin"
    return name


def main() -> int:
    from traffic_data_elt.config import AwsConfig
    from traffic_data_elt.extract import HttpStreamExtractor
    from traffic_data_elt.load import S3Uploader
    from traffic_data_elt.utils import get_logger

    log = get_logger("v2_bronze_smoke_test")

    url = os.environ.get("SMOKE_TEST_URL", _DEFAULT_URL)

    try:
        aws = AwsConfig.from_env()
    except EnvironmentError as exc:
        log.error("configuration error: %s", exc)
        log.error("set AWS_REGION and S3_BUCKET before running the smoke test")
        return 2

    filename = _filename_from_url(url)
    extractor = HttpStreamExtractor(url, chunk_bytes=aws.http_chunk_bytes)
    uploader = S3Uploader(aws)

    # ── Stream and upload ────────────────────────────────────────────────────
    try:
        with extractor.open() as body:
            result = uploader.upload_stream(body, "test", filename)
    except Exception as exc:  # noqa: BLE001 - top-level script boundary
        log.error("smoke test failed during upload: %s", exc)
        return 1

    log.info(
        "uploaded: s3://%s/%s (%d bytes, etag=%s)",
        result.bucket,
        result.key,
        result.bytes_transferred,
        result.etag,
    )

    # ── Verify object exists with non-zero size ──────────────────────────────
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError

    client = boto3.client("s3", region_name=aws.region)
    try:
        head = client.head_object(Bucket=result.bucket, Key=result.key)
    except (BotoCoreError, ClientError) as exc:
        log.error("verification HEAD failed: %s", exc)
        return 1

    size = int(head.get("ContentLength", 0))
    if size <= 0:
        log.error("verification failed: object has zero size")
        return 1

    log.info("verification OK: object exists, size=%d bytes", size)
    if result.bytes_transferred and size != result.bytes_transferred:
        log.warning(
            "size mismatch: uploaded=%d, s3=%d",
            result.bytes_transferred,
            size,
        )

    log.info("smoke test PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
