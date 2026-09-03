"""Opt-in integration test: V1 dbt vs V2 Spark Gold semantic parity.

Proves that the V2 Spark ``trajectory_summary`` Gold dataset reproduces the V1
dbt ``intermediate.int_vehicle_trajectory_summary`` semantics field-for-field on
the same pNEUMA sample.

This test is intentionally excluded from the normal unit run.  It requires:
  - A running V1 PostgreSQL warehouse (``v1_local`` compose) with
    ``intermediate.int_vehicle_trajectory_summary`` already built via dbt from
    the same ``data/sample/pnemas.csv`` sample.
  - A V2 Gold export JSON produced by the Databricks ``gold_pipeline`` notebook
    (``/Volumes/.../v2_temp/exports/gold_<run_id>.json`` downloaded locally).

Environment
-----------
    TRAFFIC_DB_HOST      (default: localhost)
    TRAFFIC_DB_PORT      (default: 5432)
    TRAFFIC_DB_NAME / TRAFFIC_DB_USER / TRAFFIC_DB_PASSWORD  (V1 warehouse)
    GOLD_EXPORT_JSON     path to the downloaded Gold export JSON

To run explicitly:
    TRAFFIC_DB_NAME=... TRAFFIC_DB_USER=... TRAFFIC_DB_PASSWORD=... \\
    GOLD_EXPORT_JSON=/tmp/gold_export.json \\
    pytest tests/integration/test_gold_v1_parity.py -v -s

It is skipped automatically when the warehouse env vars or the export file are
absent.  See ``v2_cloud/databricks/GOLD_CONTRACT.md`` for the field contract and
the 1e-6 float tolerance rationale.
"""

from __future__ import annotations

import json
import os

import pytest

_DB_ENV = ("TRAFFIC_DB_NAME", "TRAFFIC_DB_USER", "TRAFFIC_DB_PASSWORD")
_GOLD_EXPORT = os.environ.get("GOLD_EXPORT_JSON", "")

_missing = [v for v in _DB_ENV if not os.environ.get(v)]
_skip_reasons = []
if _missing:
    _skip_reasons.append(f"missing warehouse env: {_missing}")
if not (_GOLD_EXPORT and os.path.exists(_GOLD_EXPORT)):
    _skip_reasons.append("GOLD_EXPORT_JSON not set or file missing")

try:
    import psycopg  # noqa: F401
except ModuleNotFoundError:
    _skip_reasons.append("psycopg not installed")

pytestmark = pytest.mark.skipif(
    bool(_skip_reasons),
    reason="V1/V2 parity test skipped: " + "; ".join(_skip_reasons)
    if _skip_reasons else "",
)

EXPECTED_TRAJECTORIES = 922


def test_v1_v2_trajectory_summary_parity():
    import psycopg

    from v2_cloud.databricks.gold_parity import compare_trajectory_summaries
    from v2_cloud.databricks.schemas.gold_schema import GOLD_FIELD_NAMES

    # ── V1 reference from Postgres ──────────────────────────────────────────
    cols = ", ".join(GOLD_FIELD_NAMES)
    with psycopg.connect(
        host=os.environ.get("TRAFFIC_DB_HOST", "localhost"),
        port=int(os.environ.get("TRAFFIC_DB_PORT", "5432")),
        dbname=os.environ["TRAFFIC_DB_NAME"],
        user=os.environ["TRAFFIC_DB_USER"],
        password=os.environ["TRAFFIC_DB_PASSWORD"],
    ) as conn, conn.cursor() as cur:
        cur.execute(f"select {cols} from intermediate.int_vehicle_trajectory_summary")
        names = [d.name for d in cur.description]
        v1_rows = [dict(zip(names, r)) for r in cur.fetchall()]

    # psycopg returns Decimal for numeric-typed SQL expressions; coerce the
    # metric columns to float so the comparison treats them numerically.
    for r in v1_rows:
        for k, v in list(r.items()):
            if k in ("source_file", "vehicle_type") or v is None:
                continue
            if not isinstance(v, int):
                r[k] = float(v)

    # ── V2 Gold export ──────────────────────────────────────────────────────
    with open(_GOLD_EXPORT) as fh:
        v2_rows = json.load(fh)

    assert len(v1_rows) == EXPECTED_TRAJECTORIES, (
        f"V1 rows {len(v1_rows)} != {EXPECTED_TRAJECTORIES}"
    )
    assert len(v2_rows) == EXPECTED_TRAJECTORIES, (
        f"V2 rows {len(v2_rows)} != {EXPECTED_TRAJECTORIES}"
    )

    # ── Field-level parity ──────────────────────────────────────────────────
    result = compare_trajectory_summaries(v1_rows, v2_rows)
    assert result.passed, "V1/V2 parity FAILED:\n" + result.summary()
