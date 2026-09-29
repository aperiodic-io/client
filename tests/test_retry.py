"""Retry behaviour of the httpx transport on transient failures."""

from __future__ import annotations

import asyncio

import httpx
import pytest
import respx

from aperiodic._backends._httpx_transport import APIError, fetch_json
from aperiodic._backends._retry import retry_delay
from aperiodic.config import MAX_RETRY_DELAY

URL = "https://api.example.com/metadata/symbols"

SYMBOLS = {"symbols": ["perpetual-BTC-USDT:USDT"], "exchange": "binance-futures"}


@pytest.fixture
def slept(monkeypatch):
    """Record backoff waits instead of serving them, keeping the suite fast."""
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    return delays


async def _fetch(**kwargs):
    return await fetch_json(URL, params={}, headers={}, **kwargs)


class TestFetchJsonRetries:
    @respx.mock
    @pytest.mark.usefixtures("slept")
    async def test_retries_transient_failure_then_succeeds(self):
        route = respx.get(URL).mock(
            side_effect=[
                httpx.Response(503, json={"error": "Failed to list symbols"}),
                httpx.Response(200, json=SYMBOLS),
            ]
        )

        assert await _fetch() == SYMBOLS
        assert route.call_count == 2

    @respx.mock
    @pytest.mark.usefixtures("slept")
    async def test_retries_connection_errors(self):
        route = respx.get(URL).mock(
            side_effect=[
                httpx.ConnectError("connection reset"),
                httpx.Response(200, json=SYMBOLS),
            ]
        )

        assert await _fetch() == SYMBOLS
        assert route.call_count == 2

    @respx.mock
    @pytest.mark.usefixtures("slept")
    async def test_surfaces_the_error_once_retries_run_out(self):
        route = respx.get(URL).mock(
            return_value=httpx.Response(503, json={"error": "Failed to list symbols"})
        )

        with pytest.raises(APIError) as exc_info:
            await _fetch(max_retries=2)

        assert exc_info.value.status_code == 503
        assert route.call_count == 3

    @respx.mock
    @pytest.mark.usefixtures("slept")
    async def test_does_not_retry_client_errors(self):
        route = respx.get(URL).mock(
            return_value=httpx.Response(401, json={"error": "Authorization Required"})
        )

        with pytest.raises(APIError) as exc_info:
            await _fetch()

        assert exc_info.value.status_code == 401
        assert route.call_count == 1

    @respx.mock
    async def test_honours_retry_after_up_to_the_cap(self, slept):
        respx.get(URL).mock(
            side_effect=[
                httpx.Response(503, headers={"Retry-After": "60"}),
                httpx.Response(200, json=SYMBOLS),
            ]
        )

        await _fetch()

        assert slept == [MAX_RETRY_DELAY]


class TestRetryDelay:
    def test_grows_exponentially_with_jitter(self):
        assert 1 <= retry_delay(0, backoff_base=1.0) <= 2
        assert 4 <= retry_delay(2, backoff_base=1.0) <= 5

    def test_caps_long_waits(self):
        assert retry_delay(10, backoff_base=1.0) == MAX_RETRY_DELAY
        assert retry_delay(0, retry_after="600") == MAX_RETRY_DELAY

    def test_prefers_retry_after_when_shorter(self):
        assert retry_delay(3, backoff_base=1.0, retry_after="2") == 2

    @pytest.mark.parametrize("retry_after", ["Wed, 21 Oct 2026 07:28:00 GMT", "-5", ""])
    def test_falls_back_to_backoff_for_unusable_hints(self, retry_after):
        assert 1 <= retry_delay(0, backoff_base=1.0, retry_after=retry_after) <= 2
