"""Structural unit tests for the V2 production Airflow DAG.

Airflow is not installed locally, so we inject lightweight stub modules for
``airflow.decorators`` / ``airflow.exceptions`` / ``airflow.utils.trigger_rule``
that RECORD the DAG's structure as the module builds it. We then import the DAG
file and assert on the captured task IDs, dependency edges, trigger rules, and
DAG-level settings — without a real scheduler.

This validates the wiring contract required by the milestone:
- DAG parses / builds
- expected task IDs exist
- dependency order (bootstrap order, bootstrap→bronze, cleanup after dbt)
- max_active_runs == 1, catchup False, schedule None
- cleanup + verify + success use ALL_SUCCESS
- no /test/ production paths and no fixture counts hardcoded in the DAG
- no password key passed to Databricks params
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_DAG_FILE = (
    Path(__file__).resolve().parents[2]
    / "v2_cloud" / "airflow" / "dags" / "traffic_data_v2_production.py"
)


# ---------------------------------------------------------------------------
# Airflow stub that captures structure
# ---------------------------------------------------------------------------


class _Capture:
    """Shared recorder for the stubbed airflow decorators."""

    def __init__(self) -> None:
        self.dag_kwargs: dict = {}
        self.tasks: dict[str, dict] = {}     # task_id -> {trigger_rule, upstreams}
        self.edges: list[tuple[str, str]] = []  # (upstream, downstream)


class _TaskRef:
    """Return value of a called stub task; records >> wiring."""

    def __init__(self, task_id: str, capture: _Capture):
        self.task_id = task_id
        self._cap = capture

    def __rshift__(self, other):  # self >> other
        self._cap.edges.append((self.task_id, other.task_id))
        return other

    def __lshift__(self, other):  # self << other
        self._cap.edges.append((other.task_id, self.task_id))
        return other


def _install_airflow_stubs(capture: _Capture) -> None:
    decorators = types.ModuleType("airflow.decorators")

    def task(*d_args, **d_kwargs):
        # Supports @task and @task(task_id=..., trigger_rule=...)
        def make(fn):
            task_id = d_kwargs.get("task_id", fn.__name__)
            trigger_rule = d_kwargs.get("trigger_rule", "all_success")

            def caller(*args, **kwargs):
                # Record the task and any TaskRef args as upstream dependencies.
                capture.tasks.setdefault(
                    task_id, {"trigger_rule": trigger_rule, "upstreams": []}
                )
                for a in list(args) + list(kwargs.values()):
                    if isinstance(a, _TaskRef):
                        capture.edges.append((a.task_id, task_id))
                        capture.tasks[task_id]["upstreams"].append(a.task_id)
                return _TaskRef(task_id, capture)

            caller._task_id = task_id
            return caller
        # bare @task usage
        if d_args and callable(d_args[0]) and not d_kwargs:
            return make(d_args[0])
        return make

    def task_group(*g_args, **g_kwargs):
        group_id = g_kwargs.get("group_id")

        def make(fn):
            def caller(*args, **kwargs):
                # Execute the group body so inner tasks + edges register; the
                # body returns the terminal inner TaskRef.
                return fn(*args, **kwargs)
            caller._group_id = group_id
            return caller
        return make

    def dag(*a, **kwargs):
        def make(fn):
            def caller(*args, **kw):
                capture.dag_kwargs.update(kwargs)
                return fn(*args, **kw)
            return caller
        return make

    decorators.task = task
    decorators.task_group = task_group
    decorators.dag = dag

    exceptions = types.ModuleType("airflow.exceptions")

    class AirflowException(Exception):
        pass

    exceptions.AirflowException = AirflowException

    utils = types.ModuleType("airflow.utils")
    trigger_rule_mod = types.ModuleType("airflow.utils.trigger_rule")

    class TriggerRule:
        ALL_SUCCESS = "all_success"
        ALL_DONE = "all_done"

    trigger_rule_mod.TriggerRule = TriggerRule

    airflow = types.ModuleType("airflow")
    sys.modules["airflow"] = airflow
    sys.modules["airflow.decorators"] = decorators
    sys.modules["airflow.exceptions"] = exceptions
    sys.modules["airflow.utils"] = utils
    sys.modules["airflow.utils.trigger_rule"] = trigger_rule_mod


@pytest.fixture()
def dag_capture(monkeypatch):
    # Ensure src is importable.
    src = str(Path(__file__).resolve().parents[2] / "src")
    if src not in sys.path:
        sys.path.insert(0, src)

    capture = _Capture()
    _install_airflow_stubs(capture)

    # Import the DAG module fresh under the stubs.
    sys.modules.pop("traffic_data_v2_production", None)
    spec = importlib.util.spec_from_file_location(
        "traffic_data_v2_production", _DAG_FILE
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # building the dag records structure
    yield capture, module

    for m in ("airflow", "airflow.decorators", "airflow.exceptions",
              "airflow.utils", "airflow.utils.trigger_rule"):
        sys.modules.pop(m, None)
    sys.modules.pop("traffic_data_v2_production", None)


# ---------------------------------------------------------------------------
# Structure assertions
# ---------------------------------------------------------------------------

_EXPECTED_TASKS = {
    "preflight_config", "preflight_databricks", "preflight_neon_production",
    "ensure_artifact_volume", "ensure_versioned_wheel",
    "ensure_databricks_notebooks", "ensure_v2_temp",
    "ingest_full_bronze", "validate_bronze",
    "run_full_silver", "validate_silver",
    "run_full_gold", "validate_gold",
    "load_neon_production", "validate_neon_production",
    "run_dbt_v2_production", "validate_dbt_production",
    "drop_v2_temp_volume", "verify_v2_temp_removed", "pipeline_success",
}


class TestDagStructure:
    def test_dag_builds(self, dag_capture):
        capture, _ = dag_capture
        assert capture.dag_kwargs, "dag(...) was never invoked"

    def test_dag_id(self, dag_capture):
        capture, _ = dag_capture
        assert capture.dag_kwargs["dag_id"] == "traffic_data_v2_production"

    def test_max_active_runs_one(self, dag_capture):
        capture, _ = dag_capture
        assert capture.dag_kwargs["max_active_runs"] == 1

    def test_catchup_false_and_manual_schedule(self, dag_capture):
        capture, _ = dag_capture
        assert capture.dag_kwargs["catchup"] is False
        assert capture.dag_kwargs["schedule"] is None

    def test_all_expected_tasks_present(self, dag_capture):
        capture, _ = dag_capture
        missing = _EXPECTED_TASKS - set(capture.tasks)
        assert not missing, f"missing tasks: {missing}"

    def _edge(self, capture, up, down):
        return (up, down) in capture.edges

    def test_bootstrap_order(self, dag_capture):
        capture, _ = dag_capture
        assert self._edge(capture, "ensure_artifact_volume", "ensure_versioned_wheel")
        assert self._edge(capture, "ensure_versioned_wheel", "ensure_databricks_notebooks")
        assert self._edge(capture, "ensure_databricks_notebooks", "ensure_v2_temp")

    def test_bootstrap_precedes_bronze(self, dag_capture):
        capture, _ = dag_capture
        assert self._edge(capture, "ensure_v2_temp", "ingest_full_bronze")

    def test_layer_order(self, dag_capture):
        capture, _ = dag_capture
        for up, down in [
            ("ingest_full_bronze", "validate_bronze"),
            ("validate_bronze", "run_full_silver"),
            ("run_full_silver", "validate_silver"),
            ("validate_silver", "run_full_gold"),
            ("run_full_gold", "validate_gold"),
            ("validate_gold", "load_neon_production"),
            ("load_neon_production", "validate_neon_production"),
            ("validate_neon_production", "run_dbt_v2_production"),
            ("run_dbt_v2_production", "validate_dbt_production"),
        ]:
            assert self._edge(capture, up, down), f"missing edge {up}->{down}"

    def test_cleanup_downstream_of_dbt_validation(self, dag_capture):
        capture, _ = dag_capture
        assert self._edge(capture, "validate_dbt_production", "drop_v2_temp_volume")
        assert self._edge(capture, "drop_v2_temp_volume", "verify_v2_temp_removed")
        assert self._edge(capture, "verify_v2_temp_removed", "pipeline_success")

    def test_cleanup_uses_all_success(self, dag_capture):
        capture, _ = dag_capture
        for t in ("drop_v2_temp_volume", "verify_v2_temp_removed", "pipeline_success"):
            assert capture.tasks[t]["trigger_rule"] == "all_success", t


# ---------------------------------------------------------------------------
# Source-level guards (no /test/, no fixture counts, no password param)
# ---------------------------------------------------------------------------

class TestDagSourceGuards:
    def _source(self) -> str:
        return _DAG_FILE.read_text()

    def test_no_test_path_segment(self):
        # The DAG legitimately contains a guard that REJECTS "/test/" paths;
        # what must be absent is any production path/URI literal using /test/.
        src = self._source()
        for bad in (
            "bronze/pneuma/test", "silver/pneuma/trajectories/test",
            "gold/pneuma/trajectory_summary/test", "s3://",
        ):
            # No hardcoded s3:// literals or /test/ dataset paths at all.
            assert bad not in src, f"unexpected hardcoded path literal: {bad!r}"

    def test_no_fixture_counts(self):
        src = self._source()
        assert "922" not in src
        assert "1446887" not in src and "1_446_887" not in src

    def test_no_password_param_key(self):
        # The DAG must never put NEON_DB_PASSWORD into job parameters.
        src = self._source()
        assert "NEON_DB_PASSWORD" not in src

    def test_helpers_are_importable(self, dag_capture):
        _, module = dag_capture
        # Sanity: derived dbt target uses v2_ prefix.
        assert module._dbt_target().startswith("v2_")
