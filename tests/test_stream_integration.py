"""Live streaming against the real stream service, end to end.

Runs against production, or APERIODIC_STREAM_URL (CI sets staging before a
release). Skipped without APERIODIC_API_KEY, which must be on a plan with live
ohlcv.binance-futures.1m, and when APERIODIC_STREAM_LIVE_TESTS is "0": CI runs
them in one matrix job only.

One connection serves every check: the CI matrix shares one key, and each job
holding several sockets for a minute would run into ``maxConnections``.
"""

from __future__ import annotations

import os
import threading
import time
import warnings
from dataclasses import dataclass

import pytest

import aperiodic
from aperiodic import APIError, StreamMessage, StreamSubscription
from aperiodic.config import DEFAULT_STREAM_URL

pytest.importorskip("websockets")

API_KEY = os.environ.get("APERIODIC_API_KEY")

pytestmark = [
    pytest.mark.skipif(not API_KEY, reason="APERIODIC_API_KEY is not set"),
    pytest.mark.skipif(
        os.environ.get("APERIODIC_STREAM_LIVE_TESTS") == "0",
        reason="live stream tests run in one CI matrix job only (APERIODIC_STREAM_LIVE_TESTS=0)",
    ),
]

SYMBOL = "perpetual-BTC-USDT:USDT"
OHLCV = "ohlcv.binance-futures.1m"
UNKNOWN = "no_such_dataset.binance-futures.1m"

# Rows are published on minute boundaries. Wait for two: these tests gate every
# unravel-router data release against staging, and a single missed minute on
# staging's live pipeline must not block a release.
ROW_TIMEOUT = 150.0

# Other jobs of the matrix may hold every connection the plan allows; wait them out.
BUSY_TIMEOUT = 240.0


@dataclass
class LiveRun:
    subscription: StreamSubscription | None
    row: StreamMessage | None


def _first_live_row() -> LiveRun:
    stream = aperiodic.stream(
        API_KEY,
        dataset="ohlcv",
        exchange="binance-futures",
        interval="1m",
        symbols=[SYMBOL],
        channels=[UNKNOWN],
        reconnect=False,
        stream_url=DEFAULT_STREAM_URL,
    )
    timer = threading.Timer(ROW_TIMEOUT, stream.close)
    timer.start()
    try:
        with stream, warnings.catch_warnings():
            warnings.simplefilter("ignore", aperiodic.StreamWarning)
            row = next((m for m in stream if not m.snapshot), None)
    finally:
        timer.cancel()
    return LiveRun(subscription=stream.subscription, row=row)


@pytest.fixture(scope="module")
def live() -> LiveRun:
    deadline = time.monotonic() + BUSY_TIMEOUT
    while True:
        try:
            return _first_live_row()
        except APIError as exc:
            if exc.status_code != 429 or time.monotonic() > deadline:
                raise
            time.sleep(15)


def test_bad_key_is_refused():
    stream = aperiodic.stream(
        "not-a-real-key", channels=[OHLCV], stream_url=DEFAULT_STREAM_URL
    )

    with pytest.raises(APIError) as info, stream:
        next(iter(stream))

    assert info.value.status_code == 401


def test_ohlcv_channel_is_granted(live):
    assert live.subscription is not None
    assert OHLCV in live.subscription.channels


def test_unknown_dataset_is_rejected(live):
    assert live.subscription is not None
    [rejection] = [r for r in live.subscription.rejected if r.channel == UNKNOWN]
    assert rejection.code in {"unknown_channel", "not_entitled"}


def test_a_live_row_arrives(live):
    assert live.row is not None, f"no live row within {ROW_TIMEOUT:.0f} s"
    assert live.row.channel == OHLCV
    assert not live.row.snapshot
    assert live.row.data["symbol"] == SYMBOL
