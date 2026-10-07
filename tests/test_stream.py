"""Live streaming client against a local fake of the stream service.

The fake runs ``websockets.sync.server`` in a thread. It refuses handshakes
without the right ``X-API-KEY``, acknowledges the subscribe frame (rejecting
the channels a test lists), then plays a per-connection script: data,
heartbeat or error frames, and a close or a dropped socket to exercise
reconnects.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import threading
from collections.abc import Callable
from itertools import islice
from pathlib import Path

import pytest

import aperiodic
from aperiodic import (
    AperiodicDataError,
    APIError,
    StreamClosedError,
    StreamMessage,
    StreamSubscriptionError,
    StreamWarning,
)
from aperiodic.endpoints import stream as stream_module

websockets_server = pytest.importorskip("websockets.sync.server")
from websockets.exceptions import ConnectionClosed  # noqa: E402

API_KEY = "test-key"
SYMBOL = "perpetual-BTC-USDT:USDT"
OHLCV = "ohlcv.binance-futures.1m"
OPEN_INTEREST = "open_interest.okx-perps.1m"

Script = Callable[..., None]


def _name(channel: str | dict) -> str:
    if isinstance(channel, str):
        return channel
    return f"{channel['dataset']}.{channel['exchange']}.{channel['interval']}"


def _row(channel: str, time: int, *, snapshot: bool = False) -> str:
    frame: dict = {"channel": channel, "data": {"symbol": SYMBOL, "time": time}}
    if snapshot:
        frame["snapshot"] = True
    return json.dumps(frame)


def _hold(ws, granted: list[str]) -> None:
    """Default script: acknowledge, then send nothing."""


class FakeStreamServer:
    def __init__(self) -> None:
        self.url = ""
        self.refuse: tuple[int, str] | None = None
        self.reject: dict[str, str] = {}
        self.scripts: list[Script] = []
        self.requests: list = []
        self.subscribes: list[dict] = []
        self.client_close_codes: list[int | None] = []
        self.client_closed = threading.Event()

    def process_request(self, connection, request):
        self.requests.append(request)
        if request.headers.get("X-API-KEY") != API_KEY:
            return connection.respond(401, json.dumps({"error": "Invalid API key"}))
        if self.refuse is not None:
            status, message = self.refuse
            return connection.respond(status, json.dumps({"error": message}))
        return None

    def handler(self, ws) -> None:
        index = len(self.subscribes)
        frame = json.loads(ws.recv())
        self.subscribes.append(frame)
        names = [_name(channel) for channel in frame["channels"]]
        ws.send(
            json.dumps(
                {
                    "op": "subscribed",
                    "id": frame["id"],
                    "channels": [n for n in names if n not in self.reject],
                    "rejected": [
                        {"channel": n, "code": self.reject[n], "message": "nope"}
                        for n in names
                        if n in self.reject
                    ],
                }
            )
        )
        script = (
            self.scripts[min(index, len(self.scripts) - 1)] if self.scripts else _hold
        )
        script(ws, names)
        try:
            while True:
                ws.recv()
        except ConnectionClosed as exc:
            self.client_close_codes.append(exc.rcvd.code if exc.rcvd else None)
            self.client_closed.set()


@pytest.fixture
def server():
    fake = FakeStreamServer()
    with websockets_server.serve(
        fake.handler, "127.0.0.1", 0, process_request=fake.process_request
    ) as srv:
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        fake.url = f"ws://127.0.0.1:{srv.socket.getsockname()[1]}/v1/stream"
        yield fake


@pytest.fixture(autouse=True)
def no_backoff(monkeypatch):
    monkeypatch.setattr(stream_module, "_reconnect_delay", lambda attempt: 0.0)


def _stream(server: FakeStreamServer, **kwargs):
    params = {
        "dataset": "ohlcv",
        "exchange": "binance-futures",
        "interval": "1m",
        "symbols": [SYMBOL],
        "stream_url": server.url,
    }
    return aperiodic.stream(kwargs.pop("api_key", API_KEY), **{**params, **kwargs})


def _take(stream, count: int) -> list[StreamMessage]:
    with stream:
        return list(islice(stream, count))


# ---------------------------------------------------------------------------
# Handshake and subscribe
# ---------------------------------------------------------------------------


def test_key_goes_in_the_header_not_the_url(server):
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]

    _take(_stream(server), 1)

    [request] = server.requests
    assert request.headers["X-API-KEY"] == API_KEY
    assert API_KEY not in request.path


def test_cloudflare_access_headers_are_sent_when_set(server, monkeypatch):
    monkeypatch.setenv("CF_ACCESS_CLIENT_ID", "client-id")
    monkeypatch.setenv("CF_ACCESS_CLIENT_SECRET", "client-secret")
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]

    _take(_stream(server), 1)

    headers = server.requests[0].headers
    assert headers["CF-Access-Client-Id"] == "client-id"
    assert headers["CF-Access-Client-Secret"] == "client-secret"


def test_sends_one_subscribe_with_the_channel_object_and_extra_channels(server):
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]

    _take(_stream(server, channels=[OPEN_INTEREST]), 1)

    [frame] = server.subscribes
    assert frame["op"] == "subscribe"
    assert isinstance(frame["id"], str)
    assert frame["channels"] == [
        {
            "dataset": "ohlcv",
            "exchange": "binance-futures",
            "interval": "1m",
            "symbols": [SYMBOL],
        },
        OPEN_INTEREST,
    ]


def test_symbols_are_omitted_when_not_given(server):
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]

    _take(_stream(server, symbols=None), 1)

    assert "symbols" not in server.subscribes[0]["channels"][0]


def test_channels_alone_are_enough(server):
    server.scripts = [lambda ws, names: ws.send(_row(OPEN_INTEREST, 1))]

    stream = aperiodic.stream(API_KEY, channels=[OPEN_INTEREST], stream_url=server.url)
    [message] = _take(stream, 1)

    assert server.subscribes[0]["channels"] == [OPEN_INTEREST]
    assert message.channel == OPEN_INTEREST


def test_no_channel_is_refused_before_connecting(server):
    with pytest.raises(AperiodicDataError):
        aperiodic.stream(API_KEY, stream_url=server.url)
    with pytest.raises(AperiodicDataError):
        aperiodic.stream(API_KEY, dataset="ohlcv", stream_url=server.url)
    assert server.requests == []


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


def test_yields_rows_and_skips_heartbeats_and_pongs(server):
    def script(ws, names):
        ws.send(json.dumps({"op": "heartbeat", "time": 1791210600000}))
        ws.send(_row(OHLCV, 1))
        ws.send(json.dumps({"op": "pong", "id": "p1"}))
        ws.send(_row(OHLCV, 2))

    server.scripts = [script]

    messages = _take(_stream(server), 2)

    assert messages == [
        StreamMessage(
            channel=OHLCV, data={"symbol": SYMBOL, "time": 1}, snapshot=False
        ),
        StreamMessage(
            channel=OHLCV, data={"symbol": SYMBOL, "time": 2}, snapshot=False
        ),
    ]


def test_snapshots_are_flagged(server):
    def script(ws, names):
        ws.send(_row(OHLCV, 1, snapshot=True))
        ws.send(_row(OHLCV, 2))

    server.scripts = [script]

    messages = _take(_stream(server), 2)

    assert [m.snapshot for m in messages] == [True, False]


def test_snapshots_are_dropped_when_not_wanted(server):
    def script(ws, names):
        ws.send(_row(OHLCV, 1, snapshot=True))
        ws.send(_row(OHLCV, 2))

    server.scripts = [script]

    [message] = _take(_stream(server, snapshot=False), 1)

    assert message.data["time"] == 2


def test_error_frames_warn_and_the_stream_goes_on(server):
    def script(ws, names):
        ws.send(
            json.dumps(
                {
                    "op": "error",
                    "code": "limit_exceeded",
                    "message": "At most 10 control frames per second",
                }
            )
        )
        ws.send(_row(OHLCV, 1))

    server.scripts = [script]

    with pytest.warns(StreamWarning, match="limit_exceeded"):
        [message] = _take(_stream(server), 1)

    assert message.data["time"] == 1


def test_the_ack_is_exposed(server):
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]
    stream = _stream(server)

    assert stream.subscription is None
    _take(stream, 1)

    assert stream.subscription is not None
    assert stream.subscription.channels == (OHLCV,)
    assert stream.subscription.rejected == ()


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------


def test_partial_rejection_warns_and_streams_the_rest(server):
    server.reject = {OPEN_INTEREST: "not_entitled"}
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]
    stream = _stream(server, channels=[OPEN_INTEREST])

    with pytest.warns(
        StreamWarning, match=r"open_interest\.okx-perps\.1m.*not_entitled"
    ):
        [message] = _take(stream, 1)

    assert message.channel == OHLCV
    assert stream.subscription.channels == (OHLCV,)
    [rejection] = stream.subscription.rejected
    assert rejection.channel == OPEN_INTEREST
    assert rejection.code == "not_entitled"
    assert rejection.message == "nope"


def test_total_rejection_raises_with_the_rejections(server):
    server.reject = {OHLCV: "not_live", OPEN_INTEREST: "unknown_channel"}

    with pytest.raises(StreamSubscriptionError) as info:
        _take(_stream(server, channels=[OPEN_INTEREST]), 1)

    assert [(r.channel, r.code) for r in info.value.rejected] == [
        (OHLCV, "not_live"),
        (OPEN_INTEREST, "unknown_channel"),
    ]
    assert "not_live" in str(info.value)


def test_server_unsubscribing_every_channel_ends_the_stream(server):
    def script(ws, names):
        ws.send(_row(OHLCV, 1))
        ws.send(
            json.dumps(
                {
                    "op": "unsubscribed",
                    "channels": [OHLCV],
                    "rejected": [
                        {
                            "channel": OHLCV,
                            "code": "not_entitled",
                            "message": "Plan changed",
                        }
                    ],
                }
            )
        )

    server.scripts = [script]

    with _stream(server) as stream:
        iterator = iter(stream)
        assert next(iterator).data["time"] == 1
        with pytest.raises(StreamSubscriptionError, match="not_entitled"):
            next(iterator)


# ---------------------------------------------------------------------------
# Refused handshakes
# ---------------------------------------------------------------------------


def test_bad_key_raises_api_error_401(server):
    with pytest.raises(APIError) as info:
        _take(_stream(server, api_key="wrong-key"), 1)

    assert info.value.status_code == 401
    assert info.value.message == "Invalid API key"
    assert len(server.requests) == 1


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (403, "Your plan does not include live data"),
        (429, "maxConnections reached"),
        (426, "Upgrade required"),
    ],
)
def test_refused_handshake_is_never_retried(server, status, message):
    server.refuse = (status, message)

    with pytest.raises(APIError) as info:
        _take(_stream(server, reconnect=True), 1)

    assert info.value.status_code == status
    assert info.value.message == message
    assert len(server.requests) == 1


# ---------------------------------------------------------------------------
# Reconnects and closes
# ---------------------------------------------------------------------------


def _drop_socket(ws, names):
    ws.send(_row(OHLCV, 1))
    ws.socket.shutdown(socket.SHUT_RDWR)


def _restart(ws, names):
    ws.send(_row(OHLCV, 1))
    ws.close(1012, "service restart")


@pytest.mark.parametrize("script", [_drop_socket, _restart], ids=["dropped", "1012"])
def test_reconnects_and_resubscribes(server, script):
    server.scripts = [script, lambda ws, names: ws.send(_row(OHLCV, 2))]

    with pytest.warns(StreamWarning, match="reconnecting"):
        messages = _take(_stream(server, channels=[OPEN_INTEREST]), 2)

    assert [m.data["time"] for m in messages] == [1, 2]
    assert len(server.subscribes) == 2
    assert server.subscribes[1]["channels"] == server.subscribes[0]["channels"]


def test_rejected_resubscribe_after_reconnect_raises(server):
    def downgrade(ws, names):
        ws.send(_row(OHLCV, 1))
        server.reject = {OHLCV: "not_entitled"}
        ws.close(1012)

    server.scripts = [downgrade, _hold]

    with pytest.raises(StreamSubscriptionError), pytest.warns(StreamWarning):
        _take(_stream(server), 2)


def test_no_reconnect_when_disabled(server):
    server.scripts = [_restart]

    with pytest.raises(StreamClosedError) as info:
        _take(_stream(server, reconnect=False), 2)

    assert info.value.code == 1012
    assert len(server.subscribes) == 1


@pytest.mark.parametrize("code", [4001, 1008])
def test_terminal_close_codes_are_not_reconnected(server, code):
    def close(ws, names):
        ws.send(_row(OHLCV, 1))
        ws.close(code, "Plan lapsed")

    server.scripts = [close]

    with pytest.raises(StreamClosedError) as info:
        _take(_stream(server, reconnect=True), 2)

    assert info.value.code == code
    assert info.value.reason == "Plan lapsed"
    assert len(server.subscribes) == 1


def test_breaking_out_closes_the_socket(server):
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]

    for _message in _stream(server):
        break

    assert server.client_closed.wait(5)
    assert server.client_close_codes == [1000]


def test_closing_the_stream_closes_the_socket(server):
    server.scripts = [lambda ws, names: ws.send(_row(OHLCV, 1))]

    with _stream(server) as stream:
        next(iter(stream))

    assert server.client_closed.wait(5)
    assert server.client_close_codes == [1000]


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


def test_pyodide_is_not_supported(monkeypatch):
    monkeypatch.setattr(sys, "platform", "emscripten")

    with pytest.raises(AperiodicDataError, match="Pyodide"):
        aperiodic.stream(API_KEY, channels=[OHLCV])


def test_missing_extra_points_at_the_install(monkeypatch):
    monkeypatch.setattr(stream_module, "_HAS_WEBSOCKETS", False)

    with pytest.raises(ImportError, match=r"pip install aperiodic\[stream\]"):
        aperiodic.stream(API_KEY, channels=[OHLCV])


def test_stream_url_can_be_overridden_from_the_environment():
    def default_url(env: dict[str, str]) -> str:
        return subprocess.run(
            [
                sys.executable,
                "-c",
                "from aperiodic.config import DEFAULT_STREAM_URL; print(DEFAULT_STREAM_URL)",
            ],
            env=env,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

    env = {k: v for k, v in os.environ.items() if k != "APERIODIC_STREAM_URL"}
    env["PYTHONPATH"] = str(Path(stream_module.__file__).parents[2])

    assert default_url(env) == "wss://stream.aperiodic.io/v1/stream"
    assert default_url(
        {**env, "APERIODIC_STREAM_URL": "wss://staging.example/v1/stream"}
    ) == ("wss://staging.example/v1/stream")


def test_closing_from_another_thread_ends_the_loop(server):
    stream = _stream(server)
    timer = threading.Timer(0.2, stream.close)
    timer.start()

    assert list(stream) == []
    assert server.client_closed.wait(5)
    assert server.client_close_codes == [1000]
