"""Live data over WebSocket: rows as they are published, at most once.

``stream`` subscribes to one or more channels on the stream service and yields
every row as it arrives. A channel is a ``dataset.exchange.interval`` triple,
e.g. ``ohlcv.binance-futures.1m``, optionally narrowed to some symbols.

Delivery is at-most-once with no replay: rows published while the connection
is down are not sent again after a reconnect. Fill such gaps from the REST
endpoints (``get_ohlcv``, ``get_metrics``, ...).

Needs the ``stream`` extra (``pip install aperiodic[stream]``) and CPython; the
browser WebSocket API used by Pyodide cannot send the ``X-API-KEY`` header.
"""

from __future__ import annotations

import json
import random
import sys
import threading
import time
import warnings
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.util import find_spec
from itertools import chain, count
from typing import TYPE_CHECKING, Any

from .. import __version__
from ..client import AperiodicDataError, APIError
from ..config import DEFAULT_STREAM_URL, RETRY_BACKOFF_BASE, get_headers
from ..types import Exchange, Interval, StreamChannel

if TYPE_CHECKING:
    from websockets.exceptions import ConnectionClosed
    from websockets.http11 import Response
    from websockets.sync.client import ClientConnection

_HAS_WEBSOCKETS = find_spec("websockets") is not None

# The service accepts at most this many channels in one subscribe frame.
MAX_CHANNELS = 200

# Seconds to wait for the handshake, and then for the subscribe acknowledgement.
OPEN_TIMEOUT = 10.0

# The server sends a heartbeat about every 30 s on an idle socket; silence for
# this long means the connection is dead (half-open TCP, laptop sleep, NAT).
IDLE_TIMEOUT = 75.0

MAX_RECONNECT_DELAY = 30.0

# A session that stayed up this long, or delivered a row, resets the backoff.
HEALTHY_SESSION = 60.0

# Close codes that end the stream: 1008 (rate limit exhausted) and every
# application code 4000-4999 (4001: plan lapsed or key rotated). Any other
# close, including a server-initiated 1000 (a graceful drain), is reconnected.
FINAL_CLOSE_CODES = frozenset({1008, *range(4000, 5000)})

# Handshake statuses worth retrying on a reconnect. Every other status (any
# 4xx but 429: 400, 401, 403, 404, 426, ...) is an answer, and always raised. 429 is raised on the first
# connect, but retried on a reconnect: the server may still count the socket
# that just dropped toward maxConnections.
RETRYABLE_HANDSHAKE_STATUSES = frozenset({502, 503, 504})
RECONNECT_HANDSHAKE_STATUSES = RETRYABLE_HANDSHAKE_STATUSES | {429}

_CLOSE_HINTS = {
    4001: "the plan no longer includes live data, or the API key was rotated",
    1008: "the control-frame rate limit stayed exhausted",
}


class StreamWarning(UserWarning):
    """Something went wrong but the stream carries on: a rejected channel, an
    error frame from the server, or a reconnect (which may leave a gap)."""


@dataclass(frozen=True)
class StreamMessage:
    """One row from a channel.

    ``data`` is the row exactly as published, e.g. for OHLCV ``symbol``,
    ``interval``, ``time`` (microseconds since the epoch), ``open``, ...
    ``snapshot`` marks the latest row per symbol sent right after subscribing
    (Pro plans and above, best-effort), as opposed to a newly published one.
    """

    channel: str
    data: dict[str, Any]
    snapshot: bool = False


@dataclass(frozen=True)
class ChannelRejection:
    """A channel the server refused. ``code`` is one of ``not_entitled``,
    ``not_live``, ``unknown_channel``, ``limit_exceeded``, ``invalid_message``."""

    channel: str
    code: str
    message: str


@dataclass(frozen=True)
class StreamSubscription:
    """The server's answer to the subscription: what streams, what was refused."""

    channels: tuple[str, ...]
    rejected: tuple[ChannelRejection, ...] = ()


class StreamSubscriptionError(AperiodicDataError):
    """No channel is left to stream: all were rejected, or the server removed them."""

    def __init__(self, message: str, rejected: Sequence[ChannelRejection] = ()):
        self.rejected = tuple(rejected)
        super().__init__(message)


class StreamClosedError(AperiodicDataError):
    """The server closed the connection and it is not reconnected.

    ``code`` is the WebSocket close code (1006 when the connection dropped
    without one): 4001 when the plan lapsed or the key was rotated, 1008 when
    the control-frame rate limit stayed exhausted.
    """

    def __init__(self, code: int, reason: str = ""):
        self.code = code
        self.reason = reason
        detail = reason or _CLOSE_HINTS.get(code, "")
        super().__init__(
            f"Stream closed by the server ({code})" + (f": {detail}" if detail else "")
        )


def _reconnect_delay(attempt: int) -> float:
    """Seconds before reconnect ``attempt`` (0-based): exponential, jittered, capped."""
    return min(
        RETRY_BACKOFF_BASE * 2 ** min(attempt, 10) + random.uniform(0, 1),
        MAX_RECONNECT_DELAY,
    )


def _is_transient(error: Exception) -> bool:
    """Whether a failure of an already-subscribed session is worth a reconnect."""
    if isinstance(error, StreamClosedError):
        return error.code not in FINAL_CLOSE_CODES
    if isinstance(error, APIError):
        return error.status_code in RECONNECT_HANDSHAKE_STATUSES
    return isinstance(error, OSError)


def _now() -> datetime:
    return datetime.now(UTC)


def _parse(raw: str | bytes) -> dict[str, Any] | None:
    """A frame as a dict, or ``None`` (with a warning) when it is not one."""
    try:
        frame = json.loads(raw)
    except ValueError:
        frame = None
    if not isinstance(frame, dict):
        warnings.warn(
            f"Skipped a malformed stream frame: {str(raw)[:200]!r}",
            StreamWarning,
            stacklevel=2,
        )
        return None
    return frame


def _closed_error(exc: ConnectionClosed) -> StreamClosedError:
    if exc.rcvd is None:
        return StreamClosedError(1006)
    return StreamClosedError(exc.rcvd.code, exc.rcvd.reason)


def _handshake_error(response: Response) -> APIError:
    text = (response.body or b"").decode("utf-8", errors="replace")
    try:
        body = json.loads(text)
    except ValueError:
        body = None
    if isinstance(body, dict):
        message = body.get("error") or body.get("message") or text
        return APIError(message, response.status_code, code=body.get("code"))
    return APIError(text or response.reason_phrase, response.status_code)


def _rejections(items: list[dict[str, Any]]) -> tuple[ChannelRejection, ...]:
    return tuple(
        ChannelRejection(
            channel=str(item.get("channel", "")),
            code=str(item.get("code", "")),
            message=str(item.get("message", "")),
        )
        for item in items
    )


def _describe(rejected: Sequence[ChannelRejection]) -> str:
    return "; ".join(f"{r.channel} ({r.code}): {r.message}" for r in rejected)


class Stream:
    """An open-ended iterator of ``StreamMessage``, returned by ``stream``.

    Iterating connects, subscribes and yields rows until the loop is left;
    breaking out (or ``KeyboardInterrupt``) closes the connection. Iterate it
    once: each iteration opens its own connection, which counts against the
    plan's ``maxConnections``. Use it as a context manager, or call ``close``,
    to close it from outside the loop.

    ``subscription`` holds the server's latest acknowledgement (granted and
    rejected channels), ``None`` until the first one arrives.

    ``gaps`` lists every outage bridged by a reconnect as
    ``(disconnected_at, reconnected_at)`` UTC datetimes. Delivery is
    at-most-once, so rows published in a gap are lost; fetch them from the
    REST endpoints if you need them.
    """

    def __init__(
        self,
        api_key: str,
        channels: list[str | StreamChannel],
        *,
        snapshot: bool,
        reconnect: bool,
        stream_url: str,
    ):
        self.subscription: StreamSubscription | None = None
        self._api_key = api_key
        self._channels = channels
        self._snapshot = snapshot
        self._reconnect = reconnect
        self._stream_url = stream_url
        self.gaps: list[tuple[datetime, datetime]] = []
        self._ws: ClientConnection | None = None
        self._closed = threading.Event()
        self._request_ids = count(1)
        self._drops = 0

    def __iter__(self) -> Iterator[StreamMessage]:
        return self._messages()

    def __enter__(self) -> Stream:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the connection; a loop over the stream then ends."""
        self._closed.set()
        if self._ws is not None:
            self._ws.close()

    def _messages(self) -> Iterator[StreamMessage]:
        subscribed = False
        attempt = 0
        disconnected_at: datetime | None = None
        while not self._closed.is_set():
            session_started: float | None = None
            delivered = False
            try:
                with self._connect() as ws:
                    self._ws = ws
                    try:
                        if self._closed.is_set():
                            return
                        pending = self._subscribe(ws)
                        if self._closed.is_set():
                            return
                        subscribed = True
                        session_started = time.monotonic()
                        if disconnected_at is not None:
                            self.gaps.append((disconnected_at, _now()))
                            disconnected_at = None
                        for message in chain(pending, self._receive(ws)):
                            delivered = True
                            yield message
                    finally:
                        # Leaving the loop is a normal close (1000), where the
                        # context manager alone would report 1011 on GeneratorExit.
                        ws.close()
                return
            except Exception as exc:
                if self._closed.is_set():
                    return
                if not (self._reconnect and subscribed and _is_transient(exc)):
                    raise
                if delivered or (
                    session_started is not None
                    and time.monotonic() - session_started >= HEALTHY_SESSION
                ):
                    attempt = 0
                if disconnected_at is None:
                    disconnected_at = _now()
                    self._drops += 1
                    warnings.warn(
                        f"Stream connection lost at {disconnected_at:%Y-%m-%d %H:%M:%S} "
                        f"UTC (drop {self._drops}: {exc}); reconnecting. Rows "
                        "published meanwhile are not replayed: see Stream.gaps "
                        "and fill them from the REST endpoints if you need them.",
                        StreamWarning,
                        stacklevel=2,
                    )
                self._closed.wait(_reconnect_delay(attempt))
                attempt += 1
            finally:
                self._ws = None

    def _connect(self) -> ClientConnection:
        from websockets.exceptions import InvalidHandshake, InvalidStatus
        from websockets.sync.client import connect

        try:
            return connect(
                self._stream_url,
                additional_headers=get_headers(self._api_key),
                user_agent_header=f"aperiodic-python/{__version__}",
                open_timeout=OPEN_TIMEOUT,
            )
        except InvalidStatus as exc:
            raise _handshake_error(exc.response) from None
        except InvalidHandshake as exc:
            raise ConnectionError(f"WebSocket handshake failed: {exc}") from exc

    def _subscribe(self, ws: ClientConnection) -> list[StreamMessage]:
        """Send the subscription and wait for its ack. Returns rows that came first."""
        from websockets.exceptions import ConnectionClosed

        request_id = f"s{next(self._request_ids)}"
        pending: list[StreamMessage] = []
        deadline = time.monotonic() + OPEN_TIMEOUT
        try:
            ws.send(
                json.dumps(
                    {"op": "subscribe", "id": request_id, "channels": self._channels}
                )
            )
            while True:
                try:
                    raw = ws.recv(timeout=max(deadline - time.monotonic(), 0))
                except TimeoutError:
                    raise TimeoutError(
                        f"The stream did not acknowledge the subscription within {OPEN_TIMEOUT:.0f} s"
                    ) from None
                frame = _parse(raw)
                if frame is None:
                    continue
                if frame.get("id") == request_id:
                    if frame.get("op") == "subscribed":
                        self._acknowledge(frame, request_id)
                        return pending
                    if frame.get("op") == "error":
                        raise StreamSubscriptionError(
                            f"Subscription refused ({frame.get('code')}): {frame.get('message')}"
                        )
                message = self._handle(frame)
                if message is not None:
                    pending.append(message)
        except ConnectionClosed as exc:
            raise _closed_error(exc) from None

    def _acknowledge(self, frame: dict[str, Any], request_id: str) -> None:
        rejected = _rejections(frame.get("rejected") or [])
        self.subscription = StreamSubscription(
            channels=tuple(frame.get("channels") or []), rejected=rejected
        )
        if not self.subscription.channels:
            raise StreamSubscriptionError(
                f"Every channel was rejected: {_describe(rejected)}", rejected
            )
        if rejected:
            warnings.warn(
                f"Some channels were rejected and will not stream (subscription "
                f"{request_id} at {_now():%Y-%m-%d %H:%M:%S} UTC): {_describe(rejected)}",
                StreamWarning,
                stacklevel=2,
            )

    def _receive(self, ws: ClientConnection) -> Iterator[StreamMessage]:
        from websockets.exceptions import ConnectionClosed

        while True:
            try:
                raw = ws.recv(timeout=IDLE_TIMEOUT)
            except TimeoutError:
                raise TimeoutError(
                    f"No frame for {IDLE_TIMEOUT:g} s, not even a heartbeat; "
                    "the connection is presumed dead"
                ) from None
            except ConnectionClosed as exc:
                raise _closed_error(exc) from None
            frame = _parse(raw)
            message = self._handle(frame) if frame is not None else None
            if message is not None:
                yield message

    def _handle(self, frame: dict[str, Any]) -> StreamMessage | None:
        """A data frame becomes a message; control frames are acted on."""
        op = frame.get("op")
        if op is None and "channel" in frame:
            if not isinstance(frame.get("data"), dict):
                warnings.warn(
                    f"Skipped a data frame without a row on {frame['channel']!r}",
                    StreamWarning,
                    stacklevel=2,
                )
                return None
            snapshot = bool(frame.get("snapshot", False))
            if snapshot and not self._snapshot:
                return None
            return StreamMessage(frame["channel"], frame["data"], snapshot)
        if op == "error":
            warnings.warn(
                f"Stream error ({frame.get('code')}): {frame.get('message')}",
                StreamWarning,
                stacklevel=2,
            )
        elif op == "unsubscribed":
            self._unsubscribed(frame)
        # heartbeat, pong and any op this client does not know need nothing.
        return None

    def _unsubscribed(self, frame: dict[str, Any]) -> None:
        rejected = _rejections(frame.get("rejected") or [])
        # A channel listed only under "rejected" is gone too; keeping it would
        # leave the loop waiting for rows that never come.
        removed = set(frame.get("channels") or []) | {r.channel for r in rejected}
        previous = self.subscription or StreamSubscription(channels=())
        self.subscription = StreamSubscription(
            channels=tuple(c for c in previous.channels if c not in removed),
            rejected=previous.rejected + rejected,
        )
        reasons = _describe(rejected) or ", ".join(sorted(removed))
        if not self.subscription.channels:
            raise StreamSubscriptionError(
                f"The server removed every channel: {reasons}", rejected
            )
        warnings.warn(
            f"The server removed channels: {reasons}", StreamWarning, stacklevel=2
        )


def _requested_channels(
    dataset: str | None,
    exchange: Exchange | None,
    interval: Interval | None,
    symbols: Sequence[str] | None,
    channels: Sequence[str | StreamChannel] | None,
) -> list[str | StreamChannel]:
    requested: list[str | StreamChannel] = []
    if dataset is not None or exchange is not None or interval is not None:
        if dataset is None or exchange is None or interval is None:
            raise AperiodicDataError(
                "dataset, exchange and interval go together: pass all three, "
                "or name channels with channels=[...]."
            )
        channel: StreamChannel = {
            "dataset": dataset,
            "exchange": exchange,
            "interval": interval,
        }
        if symbols is not None:
            channel["symbols"] = (
                [symbols] if isinstance(symbols, str) else list(symbols)
            )
        requested.append(channel)
    elif symbols is not None:
        raise AperiodicDataError("symbols needs dataset, exchange and interval.")

    requested.extend(channels or [])
    if not requested:
        raise AperiodicDataError(
            "Name at least one channel: dataset, exchange and interval, "
            'or channels=["ohlcv.binance-futures.1m", ...].'
        )
    if len(requested) > MAX_CHANNELS:
        raise AperiodicDataError(
            f"At most {MAX_CHANNELS} channels per stream, got {len(requested)}."
        )
    return requested


def stream(
    api_key: str,
    dataset: str | None = None,
    exchange: Exchange | None = None,
    interval: Interval | None = None,
    symbols: Sequence[str] | None = None,
    *,
    channels: Sequence[str | StreamChannel] | None = None,
    snapshot: bool = True,
    reconnect: bool = True,
    stream_url: str = DEFAULT_STREAM_URL,
) -> Stream:
    """
    Stream live rows over WebSocket as they are published.

    Subscribes to ``dataset.exchange.interval`` (narrowed to ``symbols``, or
    every symbol the plan allows when omitted) plus any ``channels``, and
    yields a ``StreamMessage`` per row. The connection opens when iteration
    starts and closes when the loop is left.

    Delivery is at-most-once with no replay. With ``reconnect=True`` a dropped
    or silent connection (no frame for 75 s), a server restart or a graceful
    server close (1000) is re-opened with exponential backoff and the same
    subscription, emitting a ``StreamWarning`` and adding the outage to
    ``Stream.gaps``; rows published meanwhile are lost, so fill the gap from
    the REST endpoints if you need it. Close codes 4000-4999 (4001: plan
    lapsed or key rotated) and 1008 (rate limit exhausted) raise
    ``StreamClosedError`` and are never reconnected.

    Needs ``pip install aperiodic[stream]`` and CPython (not Pyodide).

    Args:
        api_key: Your Aperiodic API key, sent in the ``X-API-KEY`` header.
        dataset: Dataset of the channel, e.g. ``"ohlcv"`` or ``"open_interest"``.
        exchange: Exchange of the channel, e.g. ``"binance-futures"``.
        interval: Interval of the channel, e.g. ``"1m"``.
        symbols: Atlas symbols (https://github.com/aperiodic-io/atlas), e.g.
            ``["perpetual-BTC-USDT:USDT"]``. Omit for every symbol.
        channels: More channels, each ``"dataset.exchange.interval"`` or a
            ``StreamChannel`` dict with optional ``symbols``. Up to 200 in all.
        snapshot: Yield the latest row per symbol sent right after subscribing
            (Pro plans and above), flagged ``snapshot=True``. Default ``True``.
        reconnect: Re-open dropped connections. Default ``True``.
        stream_url: Stream endpoint (default: ``wss://stream.aperiodic.io/v1/stream``,
            or ``APERIODIC_STREAM_URL``).

    Returns:
        Stream: iterate it for ``StreamMessage`` objects; ``.subscription``
        holds the granted and rejected channels.

    Raises:
        APIError: The handshake was refused: 401 (bad key), 403 (no live data
            on the plan), 426, or 429 (``maxConnections`` reached) on the first
            connect. 401/403/426 are never retried; a 429 on a reconnect is.
        StreamSubscriptionError: Every channel was rejected; ``.rejected``
            says why. Partial rejections emit a ``StreamWarning`` instead.
        StreamClosedError: The server closed the connection for good.
        ImportError: The ``stream`` extra is not installed.

    Example:
        >>> from aperiodic import stream
        >>>
        >>> for message in stream(
        ...     api_key="your-api-key",
        ...     dataset="ohlcv",
        ...     exchange="binance-futures",
        ...     interval="1m",
        ...     symbols=["perpetual-BTC-USDT:USDT"],
        ... ):
        ...     print(message.channel, message.data["close"])
    """
    if sys.platform == "emscripten":
        raise AperiodicDataError(
            "Live streaming is not supported in Pyodide/WASM (marimo, "
            "JupyterLite): a browser WebSocket cannot send the X-API-KEY "
            "header. Stream from CPython instead."
        )
    if not _HAS_WEBSOCKETS:
        raise ImportError(
            "Live streaming needs the websockets package. Install with: "
            "pip install aperiodic[stream]"
        )
    if not api_key:
        raise AperiodicDataError("api_key is required to stream live data.")

    return Stream(
        api_key,
        _requested_channels(dataset, exchange, interval, symbols, channels),
        snapshot=snapshot,
        reconnect=reconnect,
        stream_url=stream_url,
    )
