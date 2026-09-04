"""Contract tests for the dbt V2 target-detection convention.

The dbt macro ``is_v2_target()`` (macros/v2_target.sql) classifies a target as
V2 when its name starts with the literal prefix ``v2_``.  dbt Jinja macros can't
be imported into pytest directly, so these tests lock the *convention* the macro
implements and assert the macro source uses the prefix rule (not a loose
substring test).  The macro is additionally exercised end-to-end by the dbt
build/docs runs.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_MACRO = Path("dbt/traffic_dwh/macros/v2_target.sql")


def is_v2_target(name: str) -> bool:
    """Reference implementation of the macro's classification rule."""
    return name.startswith("v2_")


def v2_environment(name: str) -> str | None:
    return name[3:] if is_v2_target(name) else None


@pytest.mark.parametrize("name", ["v2_dev", "v2_staging", "v2_production", "v2_feature_x"])
def test_v2_targets_qualify(name):
    assert is_v2_target(name) is True


@pytest.mark.parametrize("name", [
    "v1_local",     # V1
    "v2",           # bare 'v2' without underscore does NOT qualify
    "myv2_local",   # substring 'v2' mid-name must NOT qualify
    "prod_v2_x",    # 'v2_' not at the start must NOT qualify
    "v2dev",        # missing underscore
])
def test_non_v2_targets_do_not_qualify(name):
    assert is_v2_target(name) is False


@pytest.mark.parametrize("name,expected", [
    ("v2_dev", "dev"),
    ("v2_staging", "staging"),
    ("v2_production", "production"),
    ("v2_feature_x", "feature_x"),
    ("v1_local", None),
])
def test_environment_suffix(name, expected):
    assert v2_environment(name) == expected


class TestMacroSource:
    def test_macro_file_exists(self):
        assert _MACRO.exists()

    def test_uses_startswith_prefix_not_loose_substring(self):
        src = _MACRO.read_text()
        assert "startswith('v2_')" in src
        # Guard against regressing to a loose substring classification. The
        # is_v2_target macro body must not `return` a substring membership test.
        assert "return(('v2' in target.name)" not in src
        assert "return('v2' in target.name" not in src

    def test_defines_both_macros(self):
        src = _MACRO.read_text()
        assert "macro is_v2_target()" in src
        assert "macro v2_environment()" in src
