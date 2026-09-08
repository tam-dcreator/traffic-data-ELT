#!/usr/bin/env python3
"""Idempotently deploy the V2 pipeline notebooks into the Databricks workspace.

Thin CLI wrapper around ``traffic_data_elt.databricks.bootstrap
.ensure_databricks_notebooks`` so the same logic is reused by the Airflow
production DAG (``ensure_databricks_notebooks`` task).

Behaviour (per notebook):
  * create the workspace directory (e.g. ``/traffic_data``) if missing,
  * import the notebook if missing,
  * overwrite it if the repository version changed,
  * leave it untouched if identical,
  * verify each notebook exists afterwards.

Does not assume the workspace directory was created manually. Uses the selected
Databricks CLI profile; never embeds credentials.

Usage
-----
    python scripts/deploy_databricks_notebooks.py \\
        --databricks-profile DEFAULT \\
        [--workspace-dir /traffic_data] \\
        [--notebooks-dir v2_cloud/databricks/notebooks]

Exit codes: 0 success; non-zero on failure.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from traffic_data_elt.databricks import bootstrap  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Deploy V2 notebooks to Databricks workspace.")
    ap.add_argument("--databricks-profile", required=True)
    ap.add_argument("--workspace-dir", default="/traffic_data",
                    help="Workspace directory for the notebooks (default: /traffic_data)")
    ap.add_argument("--notebooks-dir",
                    default=str(_REPO_ROOT / "v2_cloud" / "databricks" / "notebooks"),
                    help="Local directory containing the notebook .py files.")
    args = ap.parse_args()

    try:
        results = bootstrap.ensure_databricks_notebooks(
            args.databricks_profile, args.notebooks_dir,
            workspace_dir=args.workspace_dir,
        )
    except (RuntimeError, FileNotFoundError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    for r in results:
        print(f"{r.action:>9}  {r.workspace_path}")
    print(f"OK: {len(results)} notebook(s) deployed to {args.workspace_dir}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
