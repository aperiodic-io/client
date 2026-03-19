"""Backward-compatible re-exports.

Everything lives in _compat now; this module keeps old
``from aperiodic.client import …`` imports working.
"""

from ._compat import APIError, DownloadError, run_async


class AperiodicDataError(Exception):
    """Base exception for Aperiodic Data Client errors."""


__all__ = ["APIError", "AperiodicDataError", "DownloadError", "run_async"]
