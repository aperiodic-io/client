"""Backoff policy shared by the httpx and pyfetch transports."""

from __future__ import annotations

import random

from ..config import MAX_RETRY_DELAY, RETRY_BACKOFF_BASE


def retry_delay(
    attempt: int,
    backoff_base: float = RETRY_BACKOFF_BASE,
    retry_after: str | None = None,
) -> float:
    """Seconds to wait before re-issuing a request, for a 0-based attempt.

    A server-sent ``Retry-After`` wins over the exponential schedule, capped at
    ``MAX_RETRY_DELAY``. Only the delta-seconds form is honoured; the HTTP-date
    form falls back to the schedule, which is what our API sends anyway.
    """
    if retry_after and retry_after.strip().isdigit():
        return float(min(int(retry_after.strip()), MAX_RETRY_DELAY))

    jittered = backoff_base * (2**attempt) + random.uniform(0, 1)
    return min(jittered, MAX_RETRY_DELAY)
