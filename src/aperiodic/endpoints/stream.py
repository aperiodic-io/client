"""Live data over WebSocket: rows as they are published.

``stream`` subscribes to one or more channels on the stream service and yields
every row as it arrives. A channel is a ``dataset.exchange.interval`` triple,
e.g. ``ohlcv.binance-futures.1m``, optionally narrowed to some symbols.

On 1m and longer intervals the server keeps recent bars and replays them to a
subscribe that asks (``since``): a reconnect asks for the bars since the newest
one received, so rows published while the connection was down arrive first,
flagged ``replay=True``. Below 1m, and past what the server keeps, delivery is
at most once: fill such gaps from the REST endpoints (``get_ohlcv``,
``get_metrics``, ...).

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
from datetime import UTC, datetime, timedelta
from importlib.util import find_spec
from itertools import chain, count
from typing import TYPE_CHECKING, Any

from .. import __version__
from ..auth import default_api_key, explain_rejected_key
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

# The server splits a replayed bar into small frames, but one row can be
# larger, and earlier servers sent a whole bar (over a megabyte on an
# all-symbols channel) in one frame: the websockets default of 1 MiB would
# close the socket on it, reconnect and ask for the same replay forever.
MAX_MESSAGE_BYTES = 16 * 2**20

# The intervals the server keeps bars for, with their length in milliseconds.
# A since on any other interval is refused.
REPLAY_STEPS_MS = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

# A subscribe refused while the server is busy replaying (after it restarts)
# is sent again after this pause, plus as much again at random, this often;
# then without the replay. A replay cut short at the server's size cap is
# followed up from the next bar at most this often per catch-up, and an
# unavailable one is asked for once more after the same pause.
BUSY_PAUSE = 1.0
BUSY_RETRIES = 5
MAX_CONTINUATIONS = 50

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
    (best-effort), as opposed to a newly published one. ``replay`` marks a row
    from the history the server replays before the live rows. A row can come
    both replayed and live: it has the same key, so overwrite it.
    """

    channel: str
    data: dict[str, Any]
    snapshot: bool = False
    replay: bool = False


@dataclass(frozen=True)
class StreamReplay:
    """The server's account of one channel's replay.

    ``start`` and ``end`` are the first and last bar sent (open times, UTC);
    ``missing`` lists bars the server does not hold, to fetch from the REST
    endpoints. ``truncated`` means the server stopped at its size cap and the
    stream asks for the rest. ``unavailable`` means the history could not be
    read, or the server was too busy to read it in time; the stream asks once
    more, then warns: fill the gap from the REST endpoints.
    """

    channel: str
    start: datetime | None
    end: datetime | None
    bars: int
    rows: int
    missing: tuple[datetime, ...] = ()
    truncated: bool = False
    unavailable: bool = False


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


def _from_ms(ms: Any) -> datetime | None:
    return (
        datetime.fromtimestamp(ms / 1000, UTC) if isinstance(ms, int | float) else None
    )


def _bar_ms(value: Any) -> int | None:
    """A row's ``time`` (microseconds on the topics) in epoch milliseconds."""
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        return None
    if value >= 1e17:
        return int(value // 1_000_000)
    if value >= 1e14:
        return int(value // 1_000)
    if value >= 1e11:
        return int(value)
    return int(value * 1_000)


def _channel_name(channel: str | StreamChannel) -> str:
    if isinstance(channel, str):
        return channel
    return f"{channel['dataset']}.{channel['exchange']}.{channel['interval']}"


def _with_since(channel: str | StreamChannel, since: int | None) -> str | StreamChannel:
    """The channel in the form a subscribe sends, asking for bars from ``since``."""
    if isinstance(channel, str):
        if since is None:
            return channel
        dataset, exchange, interval = channel.split(".")
        return {
            "dataset": dataset,
            "exchange": exchange,
            "interval": interval,
            "since": since,
        }
    copy: StreamChannel = {
        key: value for key, value in channel.items() if key != "since"
    }  # type: ignore[assignment]
    if since is not None:
        copy["since"] = since
    return copy


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
    ``(disconnected_at, reconnected_at)`` UTC datetimes. On 1m and longer
    intervals a reconnect replays the bars missed in it (``replays`` says what
    came back and what is missing); below 1m the rows published in a gap are
    lost, so fetch them from the REST endpoints if you need them.

    ``replays`` holds a ``StreamReplay`` for every replay the server ran.
    """

    def __init__(
        self,
        api_key: str,
        channels: list[str | StreamChannel],
        *,
        snapshot: bool,
        reconnect: bool,
        stream_url: str,
        since: int | None = None,
        replay: bool = True,
    ):
        self.subscription: StreamSubscription | None = None
        self._api_key = api_key
        self._channels = channels
        self._snapshot = snapshot
        self._reconnect = reconnect
        self._stream_url = stream_url
        self._since = since
        self._replay = replay
        self.gaps: list[tuple[datetime, datetime]] = []
        self.replays: list[StreamReplay] = []
        # Each channel's newest bar received (epoch ms), and when the last
        # session subscribed: where a reconnect's replay starts.
        self._last_bar: dict[str, int] = {}
        self._subscribed_at: int | None = None
        # Each channel whose replay is not finished, and the bar to ask from
        # if it must be asked for again: after a drop, the size cap or a
        # refusal. A drop resumes from here, not from the newest live row,
        # which comes before the replay has caught up.
        self._replay_from: dict[str, int] = {}
        # Channels whose `replayed` has not come yet. Follow-ups wait for all
        # of them, so one replay runs at a time and none is refused for
        # exceeding the server's replays per connection.
        self._awaiting: set[str] = set()
        self._awaiting_until = 0.0
        self._follow_ups: dict[str, int] = {}
        self._follow_ups_due = 0.0
        self._follow_up_ids: dict[str, list[str]] = {}
        self._follow_up_refusals = 0
        self._continuations: dict[str, int] = {}
        # The bar a replay returned nothing from, cut off by the size cap that
        # a subscribe's channels share; asked for again alone and first.
        self._stalled: dict[str, int] = {}
        self._retried: set[str] = set()
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
                        f"UTC (drop {self._drops}: {exc}); reconnecting. "
                        + (
                            "Bars missed meanwhile on 1m and longer intervals are "
                            "replayed (see Stream.replays); fill anything else "
                            "from the REST endpoints if you need it."
                            if self._replay
                            else "Rows published meanwhile are not replayed: see "
                            "Stream.gaps and fill them from the REST endpoints if "
                            "you need them."
                        ),
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
                max_size=MAX_MESSAGE_BYTES,
            )
        except InvalidStatus as exc:
            raise explain_rejected_key(
                _handshake_error(exc.response), self._api_key
            ) from None
        except InvalidHandshake as exc:
            raise ConnectionError(f"WebSocket handshake failed: {exc}") from exc

    def _subscribe_channels(self, resume: bool) -> list[str | StreamChannel]:
        """The channels to subscribe with, each asking for bars from where it
        should replay: ``since`` on the first session; on a reconnect the newest
        bar received, or one bar before the last subscribe when none came."""
        channels: list[str | StreamChannel] = []
        for channel in self._channels:
            name = _channel_name(channel)
            step = REPLAY_STEPS_MS.get(name.rsplit(".", 1)[-1])
            since: int | None = None
            if step is not None and not resume:
                since = (
                    self._since
                    if self._since is not None
                    else (channel.get("since") if isinstance(channel, dict) else None)
                )
            elif step is not None and self._replay:
                if name in self._replay_from:
                    since = self._replay_from[name]
                elif name in self._last_bar:
                    since = self._last_bar[name]
                elif self._subscribed_at is not None:
                    since = self._subscribed_at - step
            channels.append(_with_since(channel, since))
        return channels

    def _subscribe(self, ws: ClientConnection) -> list[StreamMessage]:
        """Send the subscription and wait for its ack. Returns rows that came first."""
        from websockets.exceptions import ConnectionClosed

        request_id = f"s{next(self._request_ids)}"
        pending: list[StreamMessage] = []
        # A session has subscribed before: this one resumes after a drop.
        channels = self._subscribe_channels(resume=self._subscribed_at is not None)
        self._reset_replays()
        self._expect_replays(channels)
        busy = 0
        deadline = time.monotonic() + OPEN_TIMEOUT
        try:
            ws.send(
                json.dumps({"op": "subscribe", "id": request_id, "channels": channels})
            )
            self._subscribed_at = int(time.time() * 1000)
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
                        if frame.get("code") == "limit_exceeded" and any(
                            isinstance(c, dict) and "since" in c for c in channels
                        ):
                            # Too many replays at once on the server: try again
                            # after a jittered pause, then without the replay.
                            busy += 1
                            if busy > BUSY_RETRIES:
                                channels = [_with_since(c, None) for c in channels]
                                self._replay_from.clear()
                                self._reset_replays()
                                warnings.warn(
                                    "The stream stayed too busy to replay; subscribing "
                                    "without it. Fill the gap from the REST endpoints.",
                                    StreamWarning,
                                    stacklevel=2,
                                )
                            elif self._closed.wait(
                                BUSY_PAUSE + random.uniform(0, BUSY_PAUSE)
                            ):
                                return pending
                            ws.send(
                                json.dumps(
                                    {
                                        "op": "subscribe",
                                        "id": request_id,
                                        "channels": channels,
                                    }
                                )
                            )
                            deadline = time.monotonic() + OPEN_TIMEOUT
                            continue
                        raise StreamSubscriptionError(
                            f"Subscription refused ({frame.get('code')}): {frame.get('message')}"
                        )
                pending.extend(self._handle(frame))
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
            self._send_follow_ups(ws)
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
            if frame is not None:
                yield from self._handle(frame)

    def _reset_replays(self) -> None:
        """Forget the follow-ups of a session; ``_replay_from`` is kept to resume from."""
        self._awaiting.clear()
        self._follow_ups.clear()
        self._follow_up_ids.clear()
        self._follow_up_refusals = 0
        self._continuations.clear()
        self._stalled.clear()
        self._retried.clear()

    def _expect_replays(self, channels: list[str | StreamChannel]) -> None:
        for channel in channels:
            if isinstance(channel, dict) and "since" in channel:
                name = _channel_name(channel)
                self._replay_from[name] = channel["since"]
                self._awaiting.add(name)
        # A `replayed` can fail to come (the server drops the channel): never
        # hold the follow-ups longer than this.
        self._awaiting_until = time.monotonic() + IDLE_TIMEOUT

    def _send_follow_ups(self, ws: ClientConnection) -> None:
        """Ask for the rest of the replays in one subscribe, once none is running.

        A channel that came back empty goes first and alone among the stalled,
        so it has the whole size cap to itself.
        """
        now = time.monotonic()
        if (
            not self._follow_ups
            or now < self._follow_ups_due
            or (self._awaiting and now < self._awaiting_until)
        ):
            return
        stalled = [name for name in self._follow_ups if name in self._stalled]
        names = stalled[:1] + [n for n in self._follow_ups if n not in self._stalled]
        channels: list[str | StreamChannel] = []
        for name in names:
            source = next((c for c in self._channels if _channel_name(c) == name), None)
            since = self._follow_ups.pop(name)
            if source is not None:
                channels.append(_with_since(source, since))
        if not channels:
            return
        request_id = f"r{next(self._request_ids)}"
        self._follow_up_ids[request_id] = [_channel_name(c) for c in channels]
        self._expect_replays(channels)
        ws.send(json.dumps({"op": "subscribe", "id": request_id, "channels": channels}))

    def _queue_follow_up(self, channel: str, since: int, *, pause: bool) -> None:
        self._replay_from[channel] = since
        self._follow_ups[channel] = since
        if pause:
            self._follow_ups_due = max(
                self._follow_ups_due,
                time.monotonic() + BUSY_PAUSE + random.uniform(0, BUSY_PAUSE),
            )

    def _finish_replay(self, channel: str) -> None:
        self._replay_from.pop(channel, None)
        self._continuations.pop(channel, None)
        self._stalled.pop(channel, None)
        self._retried.discard(channel)

    def _follow_up_refused(self, channels: list[str]) -> None:
        """A follow-up refused while the server is busy: try again after a pause."""
        self._follow_up_refusals += 1
        for name in channels:
            self._awaiting.discard(name)
            since = self._replay_from.get(name)
            if since is None:
                continue
            if self._follow_up_refusals > BUSY_RETRIES:
                self._finish_replay(name)
                warnings.warn(
                    f"The stream stayed too busy to finish replaying {name}; fill it "
                    f"from {_from_ms(since):%Y-%m-%d %H:%M} UTC from the REST endpoints.",
                    StreamWarning,
                    stacklevel=2,
                )
            else:
                self._queue_follow_up(name, since, pause=True)

    def _handle(self, frame: dict[str, Any]) -> list[StreamMessage]:
        """Data and replay frames become messages; control frames are acted on."""
        op = frame.get("op")
        if op is None and "channel" in frame and frame.get("replay"):
            rows = frame.get("data")
            if not isinstance(rows, list):
                warnings.warn(
                    f"Skipped a replay frame without rows on {frame['channel']!r}",
                    StreamWarning,
                    stacklevel=2,
                )
                return []
            messages = [
                StreamMessage(frame["channel"], row, replay=True)
                for row in rows
                if isinstance(row, dict)
            ]
            for message in messages:
                self._note_bar(message)
            bar = frame.get("time")
            name = frame["channel"]
            if isinstance(bar, int) and name in self._replay_from:
                self._replay_from[name] = max(self._replay_from[name], bar)
            return messages
        if op is None and "channel" in frame:
            if not isinstance(frame.get("data"), dict):
                warnings.warn(
                    f"Skipped a data frame without a row on {frame['channel']!r}",
                    StreamWarning,
                    stacklevel=2,
                )
                return []
            snapshot = bool(frame.get("snapshot", False))
            if snapshot and not self._snapshot:
                return []
            message = StreamMessage(frame["channel"], frame["data"], snapshot)
            if not snapshot:
                self._note_bar(message)
            return [message]
        if op == "replayed":
            self._replayed(frame)
        elif op == "error" and (
            frame.get("code") == "limit_exceeded"
            and str(frame.get("id")) in self._follow_up_ids
        ):
            self._follow_up_refused(self._follow_up_ids.pop(str(frame.get("id"))))
        elif op == "error":
            warnings.warn(
                f"Stream error ({frame.get('code')}): {frame.get('message')}",
                StreamWarning,
                stacklevel=2,
            )
        elif op == "unsubscribed":
            self._unsubscribed(frame)
        # heartbeat, pong, a follow-up's ack and any op this client does not
        # know need nothing.
        return []

    def _note_bar(self, message: StreamMessage) -> None:
        ms = _bar_ms(message.data.get("time"))
        if ms is not None and ms > self._last_bar.get(message.channel, 0):
            self._last_bar[message.channel] = ms

    def _replayed(self, frame: dict[str, Any]) -> None:
        channel = str(frame.get("channel", ""))
        missing = tuple(
            at for at in (_from_ms(ms) for ms in frame.get("missing") or []) if at
        )
        replay = StreamReplay(
            channel=channel,
            start=_from_ms(frame.get("from")) if frame.get("bars") else None,
            end=_from_ms(frame.get("to")),
            bars=int(frame.get("bars") or 0),
            rows=int(frame.get("rows") or 0),
            missing=missing,
            truncated=bool(frame.get("truncated")),
            unavailable=bool(frame.get("unavailable")),
        )
        self.replays.append(replay)
        self._awaiting.discard(channel)
        if replay.unavailable:
            if self._retry_unavailable(channel):
                return
            warnings.warn(
                f"The replay of {channel} is unavailable; fill the gap from the REST endpoints.",
                StreamWarning,
                stacklevel=2,
            )
        elif missing:
            warnings.warn(
                f"The replay of {channel} misses {len(missing)} bar(s) the server "
                f"does not hold, from {missing[0]:%Y-%m-%d %H:%M} UTC; fetch them "
                "from the REST endpoints.",
                StreamWarning,
                stacklevel=2,
            )
        if replay.truncated:
            self._continue_replay(channel, frame)
        else:
            self._finish_replay(channel)

    def _retry_unavailable(self, channel: str) -> bool:
        """Ask once more for a replay the server could not read in time."""
        since = self._replay_from.get(channel)
        if since is None or channel in self._retried:
            self._finish_replay(channel)
            return False
        self._retried.add(channel)
        self._queue_follow_up(channel, since, pause=True)
        return True

    def _continue_replay(self, channel: str, frame: dict[str, Any]) -> None:
        """Ask for the rest of a replay cut short at the server's size cap.

        The cap is shared by a subscribe's channels, so a later one can come
        back with no bars at all (``to`` null): it is asked for again from
        ``from``. Empty a second time from the same bar, alone, that bar is
        larger than the cap itself.
        """
        step = REPLAY_STEPS_MS.get(channel.rsplit(".", 1)[-1])
        if step is None or channel not in self._replay_from:
            return
        to, start = frame.get("to"), frame.get("from")
        if isinstance(to, int):
            since = to + step
        elif isinstance(start, int) and self._stalled.get(channel) != start:
            self._stalled[channel] = start
            since = start
        else:
            self._finish_replay(channel)
            warnings.warn(
                f"A bar of {channel} is larger than the server's replay cap; "
                "fetch it and what follows from the REST endpoints.",
                StreamWarning,
                stacklevel=2,
            )
            return
        continuations = self._continuations.get(channel, 0) + 1
        if continuations > MAX_CONTINUATIONS:
            self._finish_replay(channel)
            warnings.warn(
                f"The replay of {channel} is still cut short after "
                f"{MAX_CONTINUATIONS} follow-ups; fill it from "
                f"{_from_ms(since):%Y-%m-%d %H:%M} UTC from the REST endpoints.",
                StreamWarning,
                stacklevel=2,
            )
            return
        self._continuations[channel] = continuations
        if isinstance(to, int):
            self._stalled.pop(channel, None)
        self._queue_follow_up(channel, since, pause=False)

    def _unsubscribed(self, frame: dict[str, Any]) -> None:
        rejected = _rejections(frame.get("rejected") or [])
        # A channel listed only under "rejected" is gone too; keeping it would
        # leave the loop waiting for rows that never come.
        removed = set(frame.get("channels") or []) | {r.channel for r in rejected}
        for name in removed:
            self._awaiting.discard(name)
            self._follow_ups.pop(name, None)
            self._finish_replay(name)
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
    api_key: str | None = None,
    dataset: str | None = None,
    exchange: Exchange | None = None,
    interval: Interval | None = None,
    symbols: Sequence[str] | None = None,
    *,
    channels: Sequence[str | StreamChannel] | None = None,
    snapshot: bool = True,
    reconnect: bool = True,
    stream_url: str = DEFAULT_STREAM_URL,
    since: datetime | timedelta | None = None,
    replay: bool = True,
) -> Stream:
    """
    Stream live rows over WebSocket as they are published.

    Subscribes to ``dataset.exchange.interval`` (narrowed to ``symbols``, or
    every symbol the plan allows when omitted) plus any ``channels``, and
    yields a ``StreamMessage`` per row. The connection opens when iteration
    starts and closes when the loop is left.

    With ``reconnect=True`` a dropped or silent connection (no frame for 75 s),
    a server restart or a graceful server close (1000) is re-opened with
    exponential backoff and the same subscription, emitting a
    ``StreamWarning`` and adding the outage to ``Stream.gaps``. With
    ``replay=True`` the reconnect also asks for each 1m-or-longer channel's
    bars since the newest one received, so rows published meanwhile arrive
    first, flagged ``replay=True`` (the server keeps 1 h of 1m to 30m bars,
    3 h of 1h, the newest 4h bar and 48 h of 1d). Below 1m, or past that,
    rows published in a gap are lost: fill it from the REST endpoints if you
    need it. Close codes 4000-4999 (4001: plan lapsed or key rotated) and 1008
    (rate limit exhausted) raise ``StreamClosedError`` and are never
    reconnected.

    Needs ``pip install aperiodic[stream]`` and CPython (not Pyodide).

    Args:
        api_key: Your Aperiodic API key, sent in the ``X-API-KEY`` header.
            Defaults to ``APERIODIC_API_KEY``, else the key ``aperiodic.login()``
            saved.
        dataset: Dataset of the channel, e.g. ``"ohlcv"`` or ``"open_interest"``.
        exchange: Exchange of the channel, e.g. ``"binance-futures"``.
        interval: Interval of the channel, e.g. ``"1m"``.
        symbols: Atlas symbols (https://github.com/aperiodic-io/atlas), e.g.
            ``["perpetual-BTC-USDT:USDT"]``. Omit for every symbol.
        channels: More channels, each ``"dataset.exchange.interval"`` or a
            ``StreamChannel`` dict with optional ``symbols``. Up to 200 in all.
        snapshot: Yield the latest row per symbol sent right after subscribing,
            flagged ``snapshot=True``. Default ``True``.
        reconnect: Re-open dropped connections. Default ``True``.
        since: Replay the bars from this time on (a UTC ``datetime``, or a
            ``timedelta`` back from now) before the live rows, on 1m and longer
            channels. A channel dict may carry its own ``since`` (epoch ms)
            instead. Default: no replay on the first connect.
        replay: After a reconnect, replay the bars missed while disconnected.
            Default ``True``.
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
    api_key = api_key or default_api_key()
    if not api_key:
        raise AperiodicDataError(
            "No API key. Run aperiodic.login(), set APERIODIC_API_KEY, or pass api_key=."
        )

    requested = _requested_channels(dataset, exchange, interval, symbols, channels)
    since_ms: int | None = None
    if since is not None:
        at = _now() - since if isinstance(since, timedelta) else since
        if at.tzinfo is None:
            raise AperiodicDataError(
                "since must be timezone-aware, e.g. datetime(..., tzinfo=UTC)."
            )
        since_ms = int(at.timestamp() * 1000)
        if not any(
            _channel_name(c).rsplit(".", 1)[-1] in REPLAY_STEPS_MS for c in requested
        ):
            raise AperiodicDataError(
                "since needs a channel at 1m or longer: the server keeps no bars below a minute."
            )

    return Stream(
        api_key,
        requested,
        snapshot=snapshot,
        reconnect=reconnect,
        stream_url=stream_url,
        since=since_ms,
        replay=replay,
    )
