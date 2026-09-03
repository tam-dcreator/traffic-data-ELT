"""V1/V2 semantic parity comparison for the Gold trajectory summary.

This module compares the V2 Spark Gold ``trajectory_summary`` against the V1
dbt ``intermediate.int_vehicle_trajectory_summary`` generated from the same
pNEUMA sample.  It proves that moving the heavy computation from PostgreSQL/dbt
into Spark preserves the V1 semantics field-for-field.

Design
------
The comparison logic is **pure Python** and operates on two mappings:

    {(source_file, track_id): {column: value, ...}, ...}

- Integer/count and categorical fields are compared **exactly**.
- Floating-point fields are compared with an absolute tolerance
  (``FLOAT_TOLERANCE``, default ``1e-6`` — see ``GOLD_CONTRACT.md`` §7).

Keeping the comparison pure means it is unit-testable without Spark, PostgreSQL,
or any live resource.  The Spark/psycopg row extraction is done by thin adapters
in the notebook (or the integration harness), which convert rows to the mapping
form and call :func:`compare_trajectory_summaries`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from v2_cloud.databricks.schemas.gold_schema import (
    FLOAT_TOLERANCE,
    GOLD_EXACT_FIELDS,
    GOLD_FIELD_NAMES,
    GOLD_FLOAT_FIELDS,
    GOLD_GRAIN_KEYS,
)

# Key type: (source_file, track_id)
Key = tuple[str, int]
Row = Mapping[str, object]


@dataclass
class ParityResult:
    """Outcome of a V1/V2 field-level parity comparison."""

    v1_row_count: int
    v2_row_count: int
    compared_keys: int
    float_tolerance: float
    missing_in_v2: list[Key] = field(default_factory=list)
    missing_in_v1: list[Key] = field(default_factory=list)
    field_mismatches: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return (
            not self.missing_in_v2
            and not self.missing_in_v1
            and not self.field_mismatches
            and self.v1_row_count == self.v2_row_count
        )

    def summary(self) -> str:
        lines = [
            f"V1/V2 parity: {'PASSED' if self.passed else 'FAILED'}",
            f"  v1_rows:          {self.v1_row_count:,}",
            f"  v2_rows:          {self.v2_row_count:,}",
            f"  keys compared:    {self.compared_keys:,}",
            f"  float_tolerance:  {self.float_tolerance:g}",
            f"  missing in V2:    {len(self.missing_in_v2)}",
            f"  missing in V1:    {len(self.missing_in_v1)}",
            f"  field mismatches: {len(self.field_mismatches)}",
        ]
        # Show at most the first 20 mismatches to keep the report readable.
        for msg in self.field_mismatches[:20]:
            lines.append(f"  DIFF  {msg}")
        if len(self.field_mismatches) > 20:
            lines.append(f"  ...   ({len(self.field_mismatches) - 20} more)")
        for key in self.missing_in_v2[:10]:
            lines.append(f"  MISSING-V2  {key}")
        for key in self.missing_in_v1[:10]:
            lines.append(f"  MISSING-V1  {key}")
        return "\n".join(lines)


def _key_of(row: Row) -> Key:
    """Extract the (source_file, track_id) key from a row mapping."""
    return (str(row[GOLD_GRAIN_KEYS[0]]), int(row[GOLD_GRAIN_KEYS[1]]))


def index_rows(rows: list[Row]) -> dict[Key, Row]:
    """Index a list of row mappings by the trajectory grain key.

    Raises ``ValueError`` on a duplicate key — the grain must be unique on both
    sides before a meaningful comparison can be made.
    """
    indexed: dict[Key, Row] = {}
    for row in rows:
        key = _key_of(row)
        if key in indexed:
            raise ValueError(f"duplicate grain key in input: {key}")
        indexed[key] = row
    return indexed


def compare_trajectory_summaries(
    v1_rows: list[Row],
    v2_rows: list[Row],
    *,
    float_tolerance: float = FLOAT_TOLERANCE,
    fields: list[str] | None = None,
) -> ParityResult:
    """Compare V1 and V2 trajectory-summary rows field-by-field.

    Parameters
    ----------
    v1_rows, v2_rows:
        Lists of row mappings.  Each must contain the grain keys plus the
        compared fields.
    float_tolerance:
        Absolute tolerance for floating-point field comparison.
    fields:
        Fields to compare.  Defaults to all Gold contract columns except the
        grain keys.  Exactness is decided per field: categorical/count fields
        compare exactly, float fields use the tolerance.

    Returns
    -------
    ParityResult
        Row-count comparison, missing keys on either side, and per-field
        mismatches.  ``.passed`` is the acceptance gate.
    """
    compare_fields = fields if fields is not None else [
        f for f in GOLD_FIELD_NAMES if f not in GOLD_GRAIN_KEYS
    ]
    exact_set = set(GOLD_EXACT_FIELDS)
    float_set = set(GOLD_FLOAT_FIELDS)

    v1_idx = index_rows(v1_rows)
    v2_idx = index_rows(v2_rows)

    result = ParityResult(
        v1_row_count=len(v1_idx),
        v2_row_count=len(v2_idx),
        compared_keys=0,
        float_tolerance=float_tolerance,
    )

    v1_keys = set(v1_idx)
    v2_keys = set(v2_idx)
    result.missing_in_v2 = sorted(v1_keys - v2_keys)
    result.missing_in_v1 = sorted(v2_keys - v1_keys)

    for key in sorted(v1_keys & v2_keys):
        r1 = v1_idx[key]
        r2 = v2_idx[key]
        result.compared_keys += 1
        for f_name in compare_fields:
            v1_val = r1.get(f_name)
            v2_val = r2.get(f_name)
            if f_name in float_set:
                if not _float_equal(v1_val, v2_val, float_tolerance):
                    result.field_mismatches.append(
                        f"{key} {f_name}: v1={v1_val!r} v2={v2_val!r} "
                        f"(|Δ|={_abs_delta(v1_val, v2_val)})"
                    )
            elif f_name in exact_set:
                if not _exact_equal(v1_val, v2_val):
                    result.field_mismatches.append(
                        f"{key} {f_name}: v1={v1_val!r} v2={v2_val!r}"
                    )
            else:
                # Unknown field classification — compare exactly and flag.
                if v1_val != v2_val:
                    result.field_mismatches.append(
                        f"{key} {f_name} (unclassified): v1={v1_val!r} v2={v2_val!r}"
                    )

    return result


# ---------------------------------------------------------------------------
# Value comparison helpers
# ---------------------------------------------------------------------------


def _float_equal(a: object, b: object, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    try:
        return abs(float(a) - float(b)) <= tol
    except (TypeError, ValueError):
        return False


def _abs_delta(a: object, b: object) -> str:
    try:
        return f"{abs(float(a) - float(b)):.3e}"
    except (TypeError, ValueError):
        return "n/a"


def _exact_equal(a: object, b: object) -> bool:
    # Normalise numeric-vs-string counts (e.g. Decimal/int) by comparing as
    # int where both look integral; otherwise compare directly.
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if _looks_integral(a) and _looks_integral(b):
        return int(a) == int(b)  # type: ignore[arg-type]
    return a == b


def _looks_integral(v: object) -> bool:
    if isinstance(v, int):
        return True
    if isinstance(v, float):
        return v.is_integer()
    return False
