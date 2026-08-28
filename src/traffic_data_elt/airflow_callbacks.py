"""Airflow task lifecycle callbacks.

Provides reusable callback functions for DAG-level and task-level
failure/success hooks.  Designed to be referenced from DAG default_args
or individual task definitions.

These callbacks must never raise — a callback failure must not obscure
the original Airflow task failure.
"""

from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger("traffic_data_elt.airflow_callbacks")


def on_task_failure(context: dict[str, Any]) -> None:
    """Log structured context when an Airflow task fails.

    Intended for use as ``on_failure_callback`` in default_args or
    individual task definitions.

    Parameters
    ----------
    context : dict
        Airflow callback context dictionary containing task instance,
        DAG run, and exception information.
    """
    try:
        ti = context.get("task_instance")
        exception = context.get("exception")

        dag_id = getattr(ti, "dag_id", None) if ti else None
        task_id = getattr(ti, "task_id", None) if ti else None
        run_id = getattr(ti, "run_id", None) if ti else None
        try_number = getattr(ti, "try_number", None) if ti else None
        logical_date = context.get("logical_date")
        log_url = getattr(ti, "log_url", None) if ti else None

        log.error(
            "Pipeline task failed"
            " dag_id=%s task_id=%s run_id=%s try_number=%s"
            " logical_date=%s error=%s log_url=%s",
            dag_id,
            task_id,
            run_id,
            try_number,
            logical_date,
            exception,
            log_url,
        )
    except Exception:
        # Callback must never raise and obscure the original failure.
        try:
            log.exception("on_task_failure callback encountered an unexpected error")
        except Exception:
            pass


def on_pipeline_success(context: dict[str, Any]) -> None:
    """Log a summary when the full pipeline completes successfully.

    Intended for use as ``on_success_callback`` on a terminal success task,
    or as a DAG-level success callback.

    Parameters
    ----------
    context : dict
        Airflow callback context dictionary.
    """
    try:
        ti = context.get("task_instance")
        dag_id = getattr(ti, "dag_id", None) if ti else None
        run_id = getattr(ti, "run_id", None) if ti else None

        log.info(
            "Pipeline completed successfully dag_id=%s run_id=%s",
            dag_id,
            run_id,
        )
    except Exception:
        try:
            log.exception("on_pipeline_success callback encountered an unexpected error")
        except Exception:
            pass
