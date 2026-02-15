#!/usr/bin/env python3
"""Shared logging helpers for EVClaw."""

from __future__ import annotations

import logging
import os
from typing import Optional

from env_utils import env_str as _compat_env_str

_DEFAULT_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
_DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"


def _env_level(default: int) -> int:
    raw = _compat_env_str("EVCLAW_LOG_LEVEL")
    if not raw:
        return default
    val = str(raw).strip().upper()
    if val.isdigit():
        return int(val)
    return getattr(logging, val, default)


def get_logger(name: str, level: Optional[int] = None) -> logging.Logger:
    """Get or create a logger with standard formatting."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATEFMT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(_env_level(logging.INFO) if level is None else level)
    return logger


def setup_logging(
    name: str,
    log_file: Optional[str] = None,
    verbose: bool = False,
    level: Optional[int] = None,
) -> logging.Logger:
    """Setup logging for a component (console + optional file)."""
    logger = logging.getLogger(name)
    logger.handlers = []
    logger.setLevel(_env_level(logging.DEBUG if verbose else logging.INFO) if level is None else level)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATEFMT))
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DEFAULT_DATEFMT))
    logger.addHandler(console_handler)

    return logger
