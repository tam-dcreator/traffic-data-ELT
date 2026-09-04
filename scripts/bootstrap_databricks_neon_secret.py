#!/usr/bin/env python3
"""Bootstrap the Neon DB password into a Databricks secret scope.

Reads ``NEON_DB_PASSWORD`` from a (gitignored) env file and stores it in a
Databricks secret scope/key using the requested Databricks CLI profile. The
password is never printed and is passed to the CLI via stdin (never argv, never
logged). Only scope/key *metadata* is verified afterwards.

Works against any Databricks account/workspace simply by choosing a different
``--databricks-profile``.

Usage
-----
    python scripts/bootstrap_databricks_neon_secret.py \\
        --databricks-profile DEFAULT \\
        --env-file v2_cloud/.env \\
        --scope v2-neon \\
        --key db-password

Exit codes: 0 success; non-zero on any failure.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


def _read_password(env_file: str, var: str = "NEON_DB_PASSWORD") -> str:
    """Return the password value from *env_file* without printing it."""
    if not os.path.exists(env_file):
        raise FileNotFoundError(f"env file not found: {env_file}")
    # Minimal parser (avoid importing dotenv into os.environ for a secret).
    for line in open(env_file):
        line = line.rstrip("\n")
        if line.startswith(f"{var}="):
            v = line.split("=", 1)[1].strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
                v = v[1:-1]
            return v
    raise KeyError(f"{var} not found in {env_file}")


def _db(profile: str, *args: str, stdin: str | None = None) -> subprocess.CompletedProcess:
    """Run the databricks CLI for *profile*.

    ``DATABRICKS_HOST`` is unset for the child so a stray env value cannot
    override the chosen profile.
    """
    env = dict(os.environ)
    env.pop("DATABRICKS_HOST", None)
    # The implicit [DEFAULT] profile is selected by omitting --profile; passing
    # "--profile DEFAULT" fails to resolve on the Databricks CLI.
    prefix = [] if profile.upper() in ("", "DEFAULT") else ["--profile", profile]
    return subprocess.run(
        ["databricks", *prefix, *args],
        input=stdin, capture_output=True, text=True, env=env, timeout=120,
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bootstrap the Neon password into a Databricks secret.")
    ap.add_argument("--databricks-profile", required=True)
    ap.add_argument("--env-file", default="v2_cloud/.env")
    ap.add_argument("--scope", default="v2-neon")
    ap.add_argument("--key", default="db-password")
    args = ap.parse_args()

    try:
        password = _read_password(args.env_file)
    except (FileNotFoundError, KeyError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 2
    if not password:
        print("FAIL: NEON_DB_PASSWORD is empty", file=sys.stderr)
        return 2

    # 1. Create the scope if missing (idempotent).
    scopes = _db(args.databricks_profile, "secrets", "list-scopes", "--output", "json")
    if scopes.returncode != 0:
        print(f"FAIL: could not list scopes: {scopes.stderr.strip()}", file=sys.stderr)
        return 1
    existing = {s.get("name") for s in _safe_json(scopes.stdout)}
    if args.scope not in existing:
        created = _db(args.databricks_profile, "secrets", "create-scope", args.scope)
        if created.returncode != 0:
            print(f"FAIL: create-scope: {created.stderr.strip()}", file=sys.stderr)
            return 1
        print(f"created scope: {args.scope}")
    else:
        print(f"scope already exists: {args.scope}")

    # 2. Put the secret via stdin (never argv/logs).
    put = _db(
        args.databricks_profile,
        "secrets", "put-secret", args.scope, args.key,
        stdin=password,
    )
    if put.returncode != 0:
        print(f"FAIL: put-secret: {put.stderr.strip()}", file=sys.stderr)
        return 1

    # 3. Verify metadata only (never the value).
    listed = _db(args.databricks_profile, "secrets", "list-secrets", args.scope,
                 "--output", "json")
    if listed.returncode != 0:
        print(f"FAIL: list-secrets: {listed.stderr.strip()}", file=sys.stderr)
        return 1
    keys = {s.get("key") for s in _safe_json(listed.stdout)}
    if args.key not in keys:
        print(f"FAIL: key '{args.key}' not present after put", file=sys.stderr)
        return 1

    print(f"OK: secret {args.scope}/{args.key} is set (value not shown).")
    return 0


def _safe_json(text: str) -> list:
    import json
    text = text.strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return []
    return data if isinstance(data, list) else data.get("secrets", data.get("scopes", []))


if __name__ == "__main__":
    sys.exit(main())
