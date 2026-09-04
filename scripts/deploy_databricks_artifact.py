#!/usr/bin/env python3
"""Build and deploy the project wheel to a Databricks/UC artifact path.

Deterministic artifact deployment for the Databricks runtime:

1. Build the project wheel (contains the shared parser + the
   ``traffic_data_elt.databricks`` runtime modules).
2. Stamp the version + git SHA for traceability.
3. Create the target UC artifact volume if it does not already exist.
4. Upload the wheel to a configurable Databricks/UC artifact path using a
   Databricks CLI profile.
5. Verify the upload.
6. Print the deployed wheel path for job submission.

The wheel is a **deployment artifact**, not temporary ETL data — it is NOT
deleted after a pipeline run. The ``v2_temp`` volume remains for temporary
ZIP/CSV processing only; the artifact volume/path is separate and configurable.

Never uploads ``.env``, secrets, or data — only the built wheel.

Usage
-----
    python scripts/deploy_databricks_artifact.py \\
        --databricks-profile DEFAULT \\
        --artifact-path /Volumes/workspace/default/v2_artifacts/wheels \\
        [--prune-old]

Exit codes: 0 success; non-zero on failure.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    env = dict(os.environ)
    env.pop("DATABRICKS_HOST", None)  # never override the chosen profile
    return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=600, **kw)


def _git_sha() -> str:
    p = _run(["git", "-C", str(_REPO_ROOT), "rev-parse", "--short", "HEAD"])
    return p.stdout.strip() if p.returncode == 0 else "nogit"


def _build_wheel() -> Path:
    """Build the wheel into dist/ and return its path."""
    dist = _REPO_ROOT / "dist"
    # Build with the standard PEP 517 frontend.
    p = _run([sys.executable, "-m", "pip", "wheel", ".", "-w", str(dist), "--no-deps"],
             cwd=str(_REPO_ROOT))
    if p.returncode != 0:
        raise RuntimeError(f"wheel build failed: {p.stderr.strip() or p.stdout.strip()}")
    wheels = sorted(dist.glob("traffic_data_elt-*.whl"), key=lambda w: w.stat().st_mtime)
    if not wheels:
        raise RuntimeError("no wheel produced in dist/")
    return wheels[-1]


def _db(profile: str, *args: str) -> subprocess.CompletedProcess:
    # The implicit [DEFAULT] profile is selected by omitting --profile; passing
    # "--profile DEFAULT" fails to resolve on the Databricks CLI. Treat the
    # literal "DEFAULT" (or empty) as "use the implicit default profile".
    prefix = [] if profile.upper() in ("", "DEFAULT") else ["--profile", profile]
    return _run(["databricks", *prefix, *args])


def _parse_volume(artifact_path: str) -> tuple[str, str, str] | None:
    """Return ``(catalog, schema, volume)`` from a UC volume artifact path.

    A UC volume path looks like ``/Volumes/<catalog>/<schema>/<volume>/...``.
    Returns ``None`` for non-``/Volumes/`` paths (e.g. plain DBFS), where there
    is no UC volume to create.
    """
    parts = artifact_path.strip("/").split("/")
    if len(parts) >= 4 and parts[0] == "Volumes":
        return parts[1], parts[2], parts[3]
    return None


def _ensure_volume(profile: str, artifact_path: str) -> None:
    """Create the UC volume for *artifact_path* if it does not already exist.

    UC volumes cannot be created with ``fs mkdirs`` (that only makes a
    sub-directory inside an existing volume), so this issues an idempotent
    ``volumes create``. No-op for non-UC-volume paths.
    """
    parsed = _parse_volume(artifact_path)
    if parsed is None:
        return
    catalog, schema, volume = parsed
    # Idempotent: check existence first, then create only if absent.
    existing = _db(profile, "volumes", "read", f"{catalog}.{schema}.{volume}")
    if existing.returncode == 0:
        print(f"artifact volume exists: {catalog}.{schema}.{volume}")
        return
    created = _db(profile, "volumes", "create", catalog, schema, volume, "MANAGED")
    if created.returncode != 0:
        raise RuntimeError(
            f"could not create UC volume {catalog}.{schema}.{volume}: "
            f"{created.stderr.strip()}"
        )
    print(f"created artifact volume: {catalog}.{schema}.{volume} (MANAGED)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Build + deploy the project wheel to Databricks.")
    ap.add_argument("--databricks-profile", required=True)
    ap.add_argument("--artifact-path", required=True,
                    help="Databricks/UC directory for wheels, e.g. "
                         "/Volumes/<cat>/<schema>/v2_artifacts/wheels")
    ap.add_argument("--prune-old", action="store_true",
                    help="Remove previously-deployed wheels for this package "
                         "(explicit opt-in only).")
    args = ap.parse_args()

    sha = _git_sha()
    print(f"git sha: {sha}")

    # 1-2. Build wheel.
    try:
        wheel = _build_wheel()
    except RuntimeError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print(f"built wheel: {wheel.name}")

    dest_dir = args.artifact_path.rstrip("/")
    # Databricks fs cp addresses UC volume paths under dbfs:.
    dest = f"dbfs:{dest_dir}/{wheel.name}"

    # Ensure the UC artifact volume exists (create if missing), then the
    # wheels sub-directory inside it (idempotent).
    try:
        _ensure_volume(args.databricks_profile, dest_dir)
    except RuntimeError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    _db(args.databricks_profile, "fs", "mkdirs", f"dbfs:{dest_dir}")

    # Optional prune of stale wheels for this package (explicit only).
    if args.prune_old:
        listed = _db(args.databricks_profile, "fs", "ls", f"dbfs:{dest_dir}")
        for line in listed.stdout.splitlines():
            name = line.strip().split()[-1] if line.strip() else ""
            if (name.startswith("traffic_data_elt-") and name.endswith(".whl")
                    and name != wheel.name):
                _db(args.databricks_profile, "fs", "rm", f"dbfs:{dest_dir}/{name}")
                print(f"pruned old wheel: {name}")

    # 3. Upload.
    up = _db(args.databricks_profile, "fs", "cp", str(wheel), dest, "--overwrite")
    if up.returncode != 0:
        print(f"FAIL: upload: {up.stderr.strip()}", file=sys.stderr)
        return 1

    # 4. Verify.
    check = _db(args.databricks_profile, "fs", "ls", f"dbfs:{dest_dir}")
    if wheel.name not in check.stdout:
        print("FAIL: wheel not present after upload", file=sys.stderr)
        return 1

    # 5. Report path for job submission (WHEEL_PATH parameter).
    print("OK: wheel deployed.")
    print(f"WHEEL_PATH={dest_dir}/{wheel.name}")
    print(f"  (git sha {sha}; pass WHEEL_PATH to the serving/gold/silver jobs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
