#!/usr/bin/env python3
"""Neon target preflight — control-plane validation before deployment.

Confirms that the configured Neon **data** endpoint (``NEON_DB_HOST``) actually
belongs to the configured Neon **branch** (``NEON_BRANCH``) within the
configured project/org, using the Neon control plane via ``NEON_API_KEY``.

This is a deployment/CI check run from the developer machine — NOT something the
Databricks job needs. The Neon API key is used only here and is never sent to
Databricks.

It exits non-zero on any mismatch so a misconfigured target is caught before a
job is submitted (e.g. a dev deploy accidentally pointing at production).

Usage
-----
    python scripts/validate_neon_target.py [--env-file v2_cloud/.env]

Required env (from the env file or the ambient environment):
    NEON_API_KEY        control-plane key (never printed)
    NEON_PROJECT_ID     Neon project id
    NEON_BRANCH         expected branch name (e.g. dev)
    NEON_DB_HOST        configured data endpoint host
Optional:
    NEON_ORG_ID         organization id (used to disambiguate the CLI)

Secrets (API key, password) are never printed. Only branch/endpoint metadata
and a pass/fail verdict are reported.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys


def _load_env_file(path: str) -> None:
    """Load KEY=VALUE lines from *path* into os.environ (quote-stripped).

    Uses python-dotenv when available; otherwise a minimal parser. Never prints
    values.
    """
    if not path or not os.path.exists(path):
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(path, override=False)
        return
    except ImportError:
        pass
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                v = v[1:-1]
            os.environ.setdefault(k.strip(), v)


def _neon_api(path: str, api_key: str) -> dict:
    """Call the Neon control plane via the `neon` CLI passthrough.

    Returns parsed JSON. Raises on non-zero exit. The API key is passed via the
    environment to the child process, never logged.
    """
    env = dict(os.environ)
    env["NEON_API_KEY"] = api_key
    proc = subprocess.run(
        ["neon", "api", path],
        capture_output=True, text=True, env=env, timeout=60,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"neon api {path} failed (rc={proc.returncode}): {proc.stderr.strip()}")
    return json.loads(proc.stdout)


def _host_matches(configured: str, endpoint_host: str) -> bool:
    """True when the configured host resolves to the given endpoint host.

    Neon exposes both a direct host (``ep-x.<region>...``) and a pooled host
    (``ep-x-pooler.<region>...``). The configured host matches when its endpoint
    id (the ``ep-...`` label, minus any ``-pooler`` suffix) equals the endpoint's
    id label.
    """
    def _ep_id(host: str) -> str:
        label = host.split(".", 1)[0]
        return label[:-len("-pooler")] if label.endswith("-pooler") else label
    return _ep_id(configured) == _ep_id(endpoint_host)


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate the Neon target before deployment.")
    ap.add_argument("--env-file", default="v2_cloud/.env",
                    help="Path to the (gitignored) env file. Default: v2_cloud/.env")
    args = ap.parse_args()

    _load_env_file(args.env_file)

    api_key = os.environ.get("NEON_API_KEY", "")
    project_id = os.environ.get("NEON_PROJECT_ID", "")
    expected_branch = os.environ.get("NEON_BRANCH", "")
    db_host = os.environ.get("NEON_DB_HOST", "")

    missing = [n for n, v in [
        ("NEON_API_KEY", api_key), ("NEON_PROJECT_ID", project_id),
        ("NEON_BRANCH", expected_branch), ("NEON_DB_HOST", db_host),
    ] if not v]
    if missing:
        print(f"FAIL: missing required config: {missing}", file=sys.stderr)
        return 2

    print(f"project:          {project_id}")
    print(f"expected branch:  {expected_branch}")
    print(f"configured host:  {db_host.split('.')[0]} ...")

    try:
        branches = _neon_api(f"/projects/{project_id}/branches", api_key).get("branches", [])
        endpoints = _neon_api(f"/projects/{project_id}/endpoints", api_key).get("endpoints", [])
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: control-plane query error: {exc}", file=sys.stderr)
        return 1

    branch_by_id = {b["id"]: b["name"] for b in branches}
    if expected_branch not in branch_by_id.values():
        print(f"FAIL: branch '{expected_branch}' not found in project "
              f"(have: {sorted(branch_by_id.values())})", file=sys.stderr)
        return 1

    # Find the endpoint the configured host maps to and its owning branch.
    matched = [e for e in endpoints if _host_matches(db_host, e.get("host", ""))]
    if not matched:
        print("FAIL: NEON_DB_HOST does not match any endpoint in this project.",
              file=sys.stderr)
        return 1
    endpoint_branch = branch_by_id.get(matched[0].get("branch_id"), "<unknown>")

    print(f"resolved endpoint branch: {endpoint_branch}")

    if endpoint_branch != expected_branch:
        print(f"FAIL: NEON_DB_HOST belongs to branch '{endpoint_branch}', "
              f"but NEON_BRANCH is '{expected_branch}'. Aborting.", file=sys.stderr)
        return 1

    print(f"OK: NEON_DB_HOST correctly targets branch '{expected_branch}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
