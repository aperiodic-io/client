from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, TypeVar

from ._compat import APIError, DownloadError

if TYPE_CHECKING:
    from collections.abc import Coroutine

T = TypeVar("T")

# Re-export errors so existing imports from aperiodic.client still work
__all__ = ["APIError", "AperiodicDataError", "DownloadError", "run_async"]


class AperiodicDataError(Exception):
    """Base exception for Aperiodic Data Client errors."""


def run_async(coro: Coroutine[None, None, T]) -> T:
    """
    Run an async coroutine, handling both regular Python and Jupyter environments.

    This function detects whether there's already a running event loop (e.g., in Jupyter)
    and uses nest_asyncio to allow nested event loops if needed.

    Args:
        coro: The coroutine to execute

    Returns:
        The result of the coroutine
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No running loop, use asyncio.run()
        return asyncio.run(coro)

    # Running in an existing event loop (e.g., Jupyter)
    import nest_asyncio

    nest_asyncio.apply()
    return loop.run_until_complete(coro)
