"""Unit tests for traffic_data_elt.airflow_callbacks.

Verifies that callbacks:
- do not raise regardless of input
- log expected structured fields on failure
- handle missing/None context values gracefully
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from traffic_data_elt.airflow_callbacks import on_pipeline_success, on_task_failure


class TestOnTaskFailure:
    """Tests for the on_task_failure callback."""

    def test_logs_structured_context(self, caplog: pytest.LogCaptureFixture) -> None:
        """Callback logs dag_id, task_id, run_id, try_number, and error."""
        ti = SimpleNamespace(
            dag_id="test_dag",
            task_id="test_task",
            run_id="manual__2025-01-01T00:00:00+00:00",
            try_number=2,
            log_url="http://localhost:8080/log/test",
        )
        exc = ValueError("something broke")
        context = {
            "task_instance": ti,
            "exception": exc,
            "logical_date": "2025-01-01T00:00:00+00:00",
        }

        with caplog.at_level(logging.ERROR, logger="traffic_data_elt.airflow_callbacks"):
            on_task_failure(context)

        assert len(caplog.records) == 1
        msg = caplog.records[0].message
        assert "test_dag" in msg
        assert "test_task" in msg
        assert "manual__2025-01-01T00:00:00+00:00" in msg
        assert "something broke" in msg
        assert "try_number=2" in msg

    def test_does_not_raise_with_empty_context(self) -> None:
        """Callback must never raise, even with an empty context dict."""
        # Should not raise
        on_task_failure({})

    def test_does_not_raise_with_none_task_instance(self) -> None:
        """Callback handles None task_instance gracefully."""
        context = {"task_instance": None, "exception": RuntimeError("fail")}
        # Should not raise
        on_task_failure(context)

    def test_does_not_raise_when_context_is_missing_keys(self) -> None:
        """Callback handles missing optional keys."""
        context = {"task_instance": SimpleNamespace(dag_id="x", task_id="y", run_id="z", try_number=1)}
        # No 'exception', no 'logical_date', no 'log_url' on ti
        on_task_failure(context)

    def test_does_not_raise_on_internal_error(self) -> None:
        """If something unexpected happens inside the callback, it still does not raise."""
        # Patch log.error to raise, simulating an internal error
        with patch(
            "traffic_data_elt.airflow_callbacks.log.error",
            side_effect=RuntimeError("logging broken"),
        ):
            # Must not propagate
            on_task_failure({"task_instance": SimpleNamespace(
                dag_id="d", task_id="t", run_id="r", try_number=1, log_url=None,
            ), "exception": ValueError("x"), "logical_date": None})


class TestOnPipelineSuccess:
    """Tests for the on_pipeline_success callback."""

    def test_logs_success_message(self, caplog: pytest.LogCaptureFixture) -> None:
        """Callback logs dag_id and run_id on success."""
        ti = SimpleNamespace(dag_id="test_dag", run_id="run_123")
        context = {"task_instance": ti}

        with caplog.at_level(logging.INFO, logger="traffic_data_elt.airflow_callbacks"):
            on_pipeline_success(context)

        assert len(caplog.records) == 1
        msg = caplog.records[0].message
        assert "test_dag" in msg
        assert "run_123" in msg
        assert "successfully" in msg

    def test_does_not_raise_with_empty_context(self) -> None:
        """Callback must never raise, even with an empty context dict."""
        on_pipeline_success({})

    def test_does_not_raise_with_none_task_instance(self) -> None:
        """Callback handles None task_instance."""
        on_pipeline_success({"task_instance": None})
