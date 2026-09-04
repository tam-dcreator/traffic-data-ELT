"""Test-only fixture manifest loader.

This package is imported ONLY by tests. Runtime/production code must never
import it. It exposes the pNEUMA integration-test expectations from
``pneuma_sample_expectations.toml`` as a plain dict.

TOML parsing uses the stdlib ``tomllib`` on Python >= 3.11 and falls back to the
test-only ``tomli`` package on Python 3.10 (the project's supported floor).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

try:  # Python 3.11+
    import tomllib as _toml
    _READ_MODE = "rb"
except ModuleNotFoundError:  # Python 3.10 — test-only fallback
    import tomli as _toml  # type: ignore[no-redef]
    _READ_MODE = "rb"

_MANIFEST = Path(__file__).with_name("pneuma_sample_expectations.toml")


@lru_cache(maxsize=1)
def load_expectations() -> dict[str, Any]:
    """Load and cache the pNEUMA fixture expectations manifest."""
    with _MANIFEST.open(_READ_MODE) as fh:
        return _toml.load(fh)


# Convenience accessors used by the integration tests.
def source_file() -> str:
    return load_expectations()["source"]["file_name"]


def silver_frame_rows() -> int:
    return int(load_expectations()["silver"]["frame_rows"])


def gold_trajectories() -> int:
    return int(load_expectations()["gold"]["trajectories"])


def gold_sum_frame_count() -> int:
    return int(load_expectations()["gold"]["sum_frame_count"])


def serving_row_count() -> int:
    return int(load_expectations()["serving"]["row_count"])


def float_tolerance() -> float:
    return float(load_expectations()["tolerance"]["float_abs"])
