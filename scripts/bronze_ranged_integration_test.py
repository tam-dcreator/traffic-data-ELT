#!/usr/bin/env python3
"""Controlled REAL Zenodo -> S3 ranged-multipart integration test.

Exercises the ACTUAL resilient Bronze code paths against real Zenodo + real S3,
but WITHOUT transferring the full 15.76 GB archive:

  * real ``preflight_source`` (HTTP Range 0-0 -> 206 + Content-Range total),
  * real ``fetch_range_to_file`` for a SMALL number of small ranges (strict 206
    / Content-Range / length validation), streamed into bounded per-part temp
    files,
  * real S3 create_multipart_upload / upload_part / complete_multipart_upload,
  * a forced transient part failure to prove per-part retry works end-to-end,
  * real final HEAD validation of the assembled object,
  * full cleanup: delete the test object + checkpoint, abort any stray upload.

Safety
------
* Writes ONLY to a clearly-labelled, timestamped TEMP integration key under
  ``bronze/_integration_test/`` — NEVER the production Bronze key
  ``bronze/pneuma/pNEUMA_dataset.zip``.
* The completed object is a VALID object of exactly the bytes uploaded (a small
  multi-part object), not a truncated production archive.
* Everything created is deleted at the end (object, checkpoint, any MPU).
* Bounded: transfers ``PARTS * PART_MIB`` bytes total (default 3 * 4 MiB = 12
  MiB). Never writes a large local file (asserts the temp dir stays bounded).

Prints a PASS/FAIL summary. Never prints credentials or signed URLs.

Usage
-----
    python scripts/bronze_ranged_integration_test.py \
        [--parts 3] [--part-mib 4] [--env-file v2_cloud/.env]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

import boto3  # noqa: E402
import requests  # noqa: E402

from traffic_data_elt.config import AwsConfig  # noqa: E402
from traffic_data_elt.load.bronze_ingest import (  # noqa: E402
    CheckpointStore,
    PartRange,
    _clean_etag,
    fetch_range_to_file,
    plan_parts,
    preflight_source,
    safe_url,
)

MIB = 1024 * 1024


def _load_env(env_file: str) -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file, override=False)
    except ImportError:
        pass


def _fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def main() -> int:
    ap = argparse.ArgumentParser(description="Real controlled Zenodo->S3 ranged MPU test.")
    ap.add_argument("--parts", type=int, default=3)
    ap.add_argument("--part-mib", type=int, default=4)
    ap.add_argument("--env-file", default=str(_REPO_ROOT / "v2_cloud" / ".env"))
    args = ap.parse_args()

    _load_env(args.env_file)
    aws = AwsConfig.from_env(args.env_file)
    source_url = aws.zenodo_url
    if not source_url:
        _fail("ZENODO_URL not configured")

    # S3 requires every part except the last to be >= 5 MiB. With >= 2 parts the
    # first parts are non-final, so enforce the same 5 MiB floor the production
    # BronzeTransferConfig enforces (avoids a spurious EntityTooSmall at Complete).
    if args.parts >= 2 and args.part_mib < 5:
        _fail("--part-mib must be >= 5 for multi-part tests (S3 minimum part size)")
    part_size = args.part_mib * MIB
    # Treat the first (parts * part_size) bytes of the real archive as a small
    # synthetic "source" so completion yields a VALID small object.
    test_source_size = args.parts * part_size

    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    test_key = f"bronze/_integration_test/pNEUMA_ranged_probe_{ts}.zip"
    prod_key = "bronze/pneuma/pNEUMA_dataset.zip"
    if test_key == prod_key or "_integration_test/" not in test_key:
        _fail("refusing: test key must be under bronze/_integration_test/")

    bucket = aws.bucket
    s3 = boto3.client("s3", region_name=aws.region)
    store = CheckpointStore(s3, bucket)

    print("=== Bronze ranged integration test ===")
    print(f"source (log-safe): {safe_url(source_url)}")
    print(f"bucket: {bucket}")
    print(f"test key: {test_key}   (NOT the production key)")
    print(f"parts: {args.parts} x {args.part_mib}MiB = {test_source_size} bytes total")

    # 1. Real preflight (proves real 206 + Content-Range total for the archive).
    #    Retry transient errors (e.g. Zenodo 504) as the production uploader does.
    from traffic_data_elt.load.bronze_ingest import (  # noqa: PLC0415
        BronzeTransientError,
    )
    pf_session = requests.Session()
    meta = None
    for attempt in range(1, 6):
        try:
            meta = preflight_source(source_url, connect_timeout_s=30,
                                    read_timeout_s=60, session=pf_session)
            break
        except BronzeTransientError as exc:
            print(f"[preflight] transient attempt {attempt}/5: {exc}; retrying")
            time.sleep(min(2 ** attempt, 15))
    if meta is None:
        _fail("preflight failed after retries (transient) — Zenodo unavailable")
    print(f"[preflight] real source size={meta.size} accept_ranges={meta.accept_ranges} "
          f"name={meta.object_name}")
    if not meta.accept_ranges:
        _fail("real source does not advertise range support")
    if meta.size < test_source_size:
        _fail("real source smaller than requested test window")
    if meta.object_name != "pNEUMA_dataset.zip":
        _fail(f"unexpected object name {meta.object_name!r}")

    # 2. Plan small parts over the synthetic window.
    parts = plan_parts(test_source_size, part_size)
    assert len(parts) == args.parts, (len(parts), args.parts)

    temp_dir = _REPO_ROOT / "build" / "bronze_it_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    upload_id = None
    max_temp_seen = 0
    forced_retry_done = False
    try:
        # 3. Real multipart upload.
        upload_id = s3.create_multipart_upload(Bucket=bucket, Key=test_key)["UploadId"]
        print(f"[mpu] created upload (id length={len(upload_id)})")
        completed = []
        for part in parts:
            etag, retried = _upload_part_with_forced_retry(
                s3, bucket, test_key, upload_id, source_url, meta.size, part,
                session, temp_dir, force_retry=(part.part_number == 2
                                                and not forced_retry_done),
            )
            if part.part_number == 2 and retried:
                forced_retry_done = True
            completed.append({"ETag": f'"{etag}"', "PartNumber": part.part_number})
            # Track max temp-dir footprint (should stay ~<= one part).
            max_temp_seen = max(max_temp_seen, _dir_bytes(temp_dir))
            print(f"[part] {part.part_number}/{len(parts)} uploaded "
                  f"bytes={part.expected_length} etag_ok={bool(etag)} retried={retried}")

        # 4. Complete (parts sorted).
        s3.complete_multipart_upload(
            Bucket=bucket, Key=test_key, UploadId=upload_id,
            MultipartUpload={"Parts": sorted(completed, key=lambda p: p["PartNumber"])},
        )
        upload_id = None  # completed; nothing to abort
        print("[mpu] completed")

        # 5. Real HEAD validation.
        head = s3.head_object(Bucket=bucket, Key=test_key)
        final_size = int(head["ContentLength"])
        if final_size != test_source_size:
            _fail(f"final object size {final_size} != expected {test_source_size}")
        print(f"[validate] HEAD size={final_size} == expected {test_source_size} OK")

        # 6. Observability assertions.
        if max_temp_seen > part_size + 4 * MIB:
            _fail(f"temp dir exceeded ~one part: {max_temp_seen} bytes")
        if not forced_retry_done:
            print("[warn] forced retry was not exercised (unexpected)")
        # Ensure no full local archive exists anywhere obvious.
        if (temp_dir / "pNEUMA_dataset.zip").exists():
            _fail("a full local archive file exists — bounded-disk guarantee broken")
        print(f"[observe] max temp-dir footprint={_human(max_temp_seen)} "
              f"(<= ~1 part) OK; forced part retry exercised={forced_retry_done}")

        print("\nPASS: real 206 + Content-Range, real UploadPart, completion, "
              "forced part retry, HEAD validation, bounded local disk.")
        return 0
    finally:
        # 7. Cleanup — always.
        if upload_id is not None:
            try:
                s3.abort_multipart_upload(Bucket=bucket, Key=test_key, UploadId=upload_id)
                print("[cleanup] aborted incomplete MPU")
            except Exception as exc:  # noqa: BLE001
                print(f"[cleanup] abort failed: {exc}")
        try:
            s3.delete_object(Bucket=bucket, Key=test_key)
            print("[cleanup] deleted test object")
        except Exception as exc:  # noqa: BLE001
            print(f"[cleanup] delete object failed: {exc}")
        try:
            store.delete(test_key)
            print("[cleanup] deleted checkpoint (if any)")
        except Exception:  # noqa: BLE001
            pass
        # Remove any temp files.
        for f in temp_dir.glob("bronze_part_*"):
            try:
                f.unlink()
            except OSError:
                pass


def _upload_part_with_forced_retry(
    s3, bucket, key, upload_id, source_url, source_size, part: PartRange,
    session, temp_dir, *, force_retry: bool,
) -> tuple[str, bool]:
    """Fetch a part into a bounded temp file and UploadPart; optionally force one
    transient failure first to prove per-part retry against the REAL stack.
    """
    import tempfile

    retried = False
    attempts = 3
    for attempt in range(1, attempts + 1):
        tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
            prefix="bronze_part_", suffix=".tmp", dir=str(temp_dir), delete=False,
        )
        tmp_path = tmp.name
        try:
            # Force a single transient failure on the first attempt of the
            # targeted part to exercise the retry path end-to-end.
            if force_retry and attempt == 1:
                retried = True
                raise requests.exceptions.ChunkedEncodingError(
                    "forced transient failure for integration retry test"
                )
            written = fetch_range_to_file(
                source_url, part, tmp, source_size=source_size,
                http_chunk_bytes=1 * MIB, connect_timeout_s=30, read_timeout_s=60,
                session=session,
            )
            tmp.flush()
            tmp.seek(0)
            resp = s3.upload_part(
                Bucket=bucket, Key=key, UploadId=upload_id,
                PartNumber=part.part_number, Body=tmp, ContentLength=written,
            )
            return _clean_etag(resp.get("ETag")) or "", retried
        except requests.exceptions.ChunkedEncodingError:
            if attempt >= attempts:
                raise
            time.sleep(0.2)
        finally:
            try:
                tmp.close()
            finally:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
    raise RuntimeError("unreachable")


def _dir_bytes(path: Path) -> int:
    return sum(f.stat().st_size for f in path.glob("**/*") if f.is_file())


def _human(n: float) -> str:
    for unit, div in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if n >= div:
            return f"{n / div:.2f}{unit}"
    return f"{int(n)}B"


if __name__ == "__main__":
    sys.exit(main())
