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

The heavy lifting (volume-ensure, versioned build+upload, content validation,
reuse-if-exists) lives in the packaged ``traffic_data_elt.databricks.bootstrap``
module so the same logic is reused by the Airflow production DAG. This script is
a thin CLI wrapper around it.

Deployed artifact is SHA-stamped and immutable per commit:

    traffic_data_elt-<version>-<git_sha>-py3-none-any.whl

If that exact wheel already exists it is reused (no rebuild/overwrite).

Never uploads ``.env``, secrets, or data — only the built wheel.

Usage
-----
    python scripts/deploy_databricks_artifact.py \\
        --databricks-profile DEFAULT \\
        --artifact-path /Volumes/workspace/default/v2_artifacts/wheels

Exit codes: 0 success; non-zero on failure.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make the packaged bootstrap module importable when run from a source checkout.
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from traffic_data_elt.databricks import bootstrap  # noqa: E402


def _project_version() -> str:
    """Read the project version from pyproject.toml (stdlib tomllib/tomli)."""
    try:
        import tomllib as toml  # py3.11+
    except ModuleNotFoundError:  # pragma: no cover - py3.10 fallback
        import tomli as toml  # type: ignore
    with open(_REPO_ROOT / "pyproject.toml", "rb") as fh:
        data = toml.load(fh)
    return data["project"]["version"]


def main() -> int:
    ap = argparse.ArgumentParser(description="Build + deploy the project wheel to Databricks.")
    ap.add_argument("--databricks-profile", required=True)
    ap.add_argument("--artifact-path", required=True,
                    help="Databricks/UC directory for wheels, e.g. "
                         "/Volumes/<cat>/<schema>/v2_artifacts/wheels")
    args = ap.parse_args()

    profile = args.databricks_profile
    artifact_dir = args.artifact_path

    # 1. Ensure the persistent artifact volume exists (idempotent, reuse-if-present).
    try:
        fqn = bootstrap.ensure_artifact_volume(profile, artifact_dir)
    except RuntimeError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print(f"artifact volume: {fqn}")

    # 2. Build + upload the versioned wheel (reuse if the exact SHA wheel exists).
    version = _project_version()
    try:
        result = bootstrap.ensure_versioned_wheel(
            profile, artifact_dir, repo_root=_REPO_ROOT, version=version,
        )
    except RuntimeError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    print(f"git sha: {result.git_sha}")
    print(f"wheel:   {result.wheel_name} ({'reused' if result.reused else 'built+uploaded'})")
    print("OK: wheel deployed.")
    print(f"WHEEL_PATH={result.wheel_path}")
    print(f"  (version {result.version}, git sha {result.git_sha}; "
          f"pass WHEEL_PATH to the serving/gold/silver jobs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
