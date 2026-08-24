"""Logging helper.

Provides a pre-configured logger so all modules emit consistent log lines.
Secrets must never be passed to any log call — callers are responsible for
redacting connection strings before logging.
"""

from __future__ import annotations

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a module-level logger.

    Usage::

        from traffic_data_elt.utils import get_logger
        log = get_logger(__name__)
        log.info("processing file %s", path)
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s")
        )
        logger.addHandler(handler)
    return logger
