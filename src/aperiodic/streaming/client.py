"""Thin wrapper over ``confluent-kafka`` for Aperiodic live metric streams.

Deliberately small: it builds the connection config, enforces the consumer
group naming rule, keeps infrastructure identifiers out of everything it
raises or logs, and bounds every network call. Anything beyond that is left to
``confluent-kafka`` itself.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Iterator
from dataclasses import dataclass
from importlib.util import find_spec
from types import TracebackType
from typing import TYPE_CHECKING, Any

from ._config import StreamCredentials
from ._errors import (
    StreamAuthError,
    StreamAuthorizationError,
    StreamConfigError,
    StreamConnectionError,
    StreamGroupIdError,
    StreamingError,
)
from ._redaction import (
    ACCOUNT_ID_PLACEHOLDER,
    BOOTSTRAP_PLACEHOLDER,
    Redactor,
    install_log_filter,
)

if TYPE_CHECKING:
    from confluent_kafka import KafkaError

# Use find_spec so importing this module never pulls in the C extension; see
# AGENTS.md. confluent-kafka is an optional extra (aperiodic[streaming]).
_HAS_CONFLUENT_KAFKA = find_spec("confluent_kafka") is not None

DEFAULT_STREAM_TIMEOUT = 10.0

# Poll slice. Bounds how long a single blocking poll can sit before the loop
# re-checks its deadline, so a caller's timeout is honoured to within a second.
_POLL_INTERVAL = 1.0

SECURITY_PROTOCOL = "SASL_SSL"
SASL_MECHANISM = "SCRAM-SHA-256"

TOPIC_SEPARATOR = "."
TOPIC_FIELD_COUNT = 3

_FOREIGN_ACCOUNT_ID = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}-"
)

logger = install_log_filter(logging.getLogger("aperiodic.streaming"))

# librdkafka writes here instead of stderr, so its output passes through the
# redacting filter. Without an explicit logger it prints from C, out of reach.
_RDKAFKA_LOGGER = install_log_filter(logging.getLogger("aperiodic.streaming.rdkafka"))

# librdkafka's syslog-style verbosity. 3 (error) keeps routine broker and
# connection metadata out of the record stream entirely.
_RDKAFKA_LOG_LEVEL = 3

_AUTH_CODES = frozenset({"_AUTHENTICATION", "SASL_AUTHENTICATION_FAILED"})

_AUTHORIZATION_CODES = frozenset(
    {
        "TOPIC_AUTHORIZATION_FAILED",
        "GROUP_AUTHORIZATION_FAILED",
        "CLUSTER_AUTHORIZATION_FAILED",
        "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
    }
)

_CONNECTION_CODES = frozenset(
    {
        "_ALL_BROKERS_DOWN",
        "_RESOLVE",
        "_SSL",
        "_TIMED_OUT",
        "_TIMED_OUT_QUEUE",
        "_TRANSPORT",
    }
)


def _error_class(code: str) -> type[StreamingError]:
    if code in _AUTH_CODES:
        return StreamAuthError
    if code in _AUTHORIZATION_CODES:
        return StreamAuthorizationError
    if code in _CONNECTION_CODES:
        return StreamConnectionError
    return StreamingError


def _require_confluent_kafka() -> Any:
    if not _HAS_CONFLUENT_KAFKA:
        raise ImportError(
            "confluent-kafka is required for live metric streams. "
            "Install with: pip install aperiodic[streaming]"
        )

    import confluent_kafka
    import confluent_kafka.admin  # not bound as an attribute by the parent import

    return confluent_kafka


@dataclass(frozen=True)
class ClusterInfo:
    """A health-check view of the cluster.

    Carries no broker hostnames or addresses by design — the count is enough to
    prove the connection worked, and the names are infrastructure detail.
    """

    broker_count: int
    topics: tuple[str, ...]


@dataclass(frozen=True)
class TopicName:
    """A stream topic, named ``<dataset>.<exchange>.<interval>``."""

    dataset: str
    exchange: str
    interval: str

    @classmethod
    def parse(cls, topic: str) -> TopicName:
        """Parse ``topic``, raising :class:`ValueError` if it is malformed."""
        fields = topic.split(TOPIC_SEPARATOR)

        if len(fields) != TOPIC_FIELD_COUNT or not all(fields):
            raise ValueError(
                f"Malformed topic name {topic!r}: expected <dataset>"
                f"{TOPIC_SEPARATOR}<exchange>{TOPIC_SEPARATOR}<interval>, "
                "for example 'ohlcv.binance-futures.m1'"
            )

        return cls(*fields)

    def __str__(self) -> str:
        return TOPIC_SEPARATOR.join((self.dataset, self.exchange, self.interval))


class KafkaStreamClient:
    """Reads Aperiodic live metric streams over SASL_SSL / SCRAM-SHA-256.

    Authenticate with your account id as the username and your Aperiodic data
    API key — the same key the REST API takes — as the password. Your
    subscription tier decides which topics you can read; listing returns only
    the datasets you are entitled to.

    ```python
    from aperiodic.streaming import KafkaStreamClient

    with KafkaStreamClient.from_env() as stream:
        print(stream.list_topics())

        for message in stream.consume(
            "ohlcv.binance-futures.m1",
            group_suffix="research",
        ):
            print(message)
    ```

    Consumer groups must be named ``<account-id>-<suffix>``; the broker rejects
    anything else. Pass ``group_suffix`` and the prefix is applied for you.

    Errors and log records emitted by this client are scrubbed of the endpoint,
    your account id and your API key, so they are safe to print in shared CI
    output.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        account_id: str,
        api_key: str,
        timeout: float = DEFAULT_STREAM_TIMEOUT,
    ) -> None:
        if timeout <= 0:
            raise StreamConfigError(f"timeout must be positive, got {timeout}")

        self._credentials = StreamCredentials(
            bootstrap_servers=bootstrap_servers,
            account_id=account_id,
            api_key=api_key,
        )
        self._timeout = timeout
        self._redactor = Redactor.for_credentials(
            bootstrap_servers=bootstrap_servers,
            account_id=account_id,
            api_key=api_key,
        )
        self._last_error: KafkaError | None = None
        self._admin_client: Any = None

    @classmethod
    def from_env(cls, *, timeout: float = DEFAULT_STREAM_TIMEOUT) -> KafkaStreamClient:
        """Build a client from ``APERIODIC_KAFKA_*`` environment variables.

        Raises :class:`StreamConfigError` naming any variable that is missing.
        """
        credentials = StreamCredentials.from_env()

        return cls(
            bootstrap_servers=credentials.bootstrap_servers,
            account_id=credentials.account_id,
            api_key=credentials.api_key,
            timeout=timeout,
        )

    @property
    def account_id(self) -> str:
        return self._credentials.account_id

    @property
    def timeout(self) -> float:
        return self._timeout

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(bootstrap_servers='{BOOTSTRAP_PLACEHOLDER}', "
            f"account_id='{ACCOUNT_ID_PLACEHOLDER}', timeout={self._timeout})"
        )

    def __enter__(self) -> KafkaStreamClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    # --- consumer groups ---------------------------------------------------

    @property
    def group_prefix(self) -> str:
        """The prefix every consumer group of this account must carry."""
        return f"{self.account_id}-"

    def build_group_id(self, suffix: str) -> str:
        """Return the qualified group id for ``suffix``.

        Accepts a bare suffix (``"research"``) or an already-qualified id
        (``"<account-id>-research"``), and rejects anything the broker would
        answer with ``GROUP_AUTHORIZATION_FAILED``.
        """
        if not suffix or not suffix.strip():
            raise StreamGroupIdError(
                "A consumer group suffix is required — groups must be named "
                "'<account-id>-<suffix>'."
            )

        suffix = suffix.strip()

        if suffix.startswith(self.group_prefix):
            return suffix

        if any(character.isspace() or character == "," for character in suffix):
            raise StreamGroupIdError(
                "A consumer group suffix may not contain whitespace or commas."
            )

        # A suffix carrying some *other* account's id would build a group the
        # broker rejects; naming the account ids in the message would leak them.
        if _starts_with_foreign_account_id(suffix):
            raise StreamGroupIdError(
                "The consumer group suffix starts with an account id that is not "
                "yours. Groups must be named '<your-account-id>-<suffix>'; pass "
                "only the suffix and the prefix is applied for you."
            )

        return f"{self.group_prefix}{suffix}"

    def validate_group_id(self, group_id: str) -> str:
        """Return ``group_id`` unchanged, or raise if it lacks the account prefix."""
        if not group_id.startswith(self.group_prefix) or group_id == self.group_prefix:
            raise StreamGroupIdError(
                "Consumer group ids must be named '<account-id>-<suffix>'. The "
                "broker rejects any other group with GROUP_AUTHORIZATION_FAILED."
            )

        return group_id

    # --- cluster access ----------------------------------------------------

    def cluster_metadata(self, timeout: float | None = None) -> ClusterInfo:
        """Fetch cluster metadata — a full DNS, TLS and SASL round trip."""
        metadata = self._fetch_metadata(
            timeout=timeout, action="Cluster metadata request"
        )

        return ClusterInfo(
            broker_count=len(metadata.brokers),
            topics=tuple(sorted(metadata.topics)),
        )

    def list_topics(self, timeout: float | None = None) -> list[str]:
        """List the topics this account is entitled to read."""
        return list(self.cluster_metadata(timeout=timeout).topics)

    def check_topic_access(self, topic: str, timeout: float | None = None) -> None:
        """Raise unless ``topic`` is readable by this account.

        Raises :class:`StreamAuthorizationError` with code
        ``TOPIC_AUTHORIZATION_FAILED`` when the account's tier does not cover
        the topic's dataset.
        """
        metadata = self._fetch_metadata(
            timeout=timeout,
            action=f"Metadata request for topic '{topic}'",
            topic=topic,
        )

        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None:
            raise StreamingError(
                f"The broker returned no metadata for topic '{topic}'."
            )

        if topic_metadata.error is not None:
            raise self._stream_error(
                topic_metadata.error,
                action=f"Access to topic '{topic}'",
            ) from None

    def stream(
        self,
        topic: str,
        *,
        group_suffix: str,
        idle_timeout: float | None = None,
    ) -> Iterator[bytes]:
        """Yield message payloads from ``topic`` until the caller stops.

        One consumer is held open for the life of the iterator, so group
        membership survives between messages — repeatedly calling
        :meth:`consume` instead would rejoin the group, and trigger a rebalance,
        every time.

        ``idle_timeout`` ends the iteration after that many seconds without a
        message; ``None`` runs until the caller breaks out. Either way the
        consumer is closed on the way out.
        """
        # Validate before requiring the extra, so a bad group id is reported as
        # a bad group id whether or not confluent-kafka happens to be installed.
        group_id = self.build_group_id(group_suffix)
        action = f"Consuming from '{topic}'"

        confluent_kafka = _require_confluent_kafka()

        self._last_error = None

        consumer = confluent_kafka.Consumer(self._consumer_config(group_id))
        received = False

        try:
            consumer.subscribe([topic])
            deadline = None if idle_timeout is None else time.monotonic() + idle_timeout

            while True:
                poll_timeout = _POLL_INTERVAL

                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    poll_timeout = min(_POLL_INTERVAL, remaining)

                message = consumer.poll(poll_timeout)

                if message is None:
                    continue

                error = message.error()

                if error is not None:
                    if error.code() == confluent_kafka.KafkaError._PARTITION_EOF:
                        continue
                    raise self._stream_error(error, action=action) from None

                received = True

                if idle_timeout is not None:
                    deadline = time.monotonic() + idle_timeout

                yield message.value()
        except confluent_kafka.KafkaException as exc:
            raise self._stream_error(exc.args[0], action=action) from None
        finally:
            self._close_quietly(consumer)

        # Reached only when the idle timeout expires. A quiet topic, a rejected
        # subscription and an unreachable cluster all look identical from here —
        # the error callback is what tells them apart.
        self._raise_if_failed(action=action, received=received)

    def consume(
        self,
        topic: str,
        *,
        group_suffix: str,
        timeout: float | None = None,
        max_messages: int = 1,
    ) -> list[bytes]:
        """Read up to ``max_messages`` from ``topic``, returning early on timeout.

        An empty result means the topic stayed quiet for ``timeout`` seconds,
        which is not an error. Being refused the topic, or never reaching the
        cluster at all, is — both raise rather than passing as a quiet topic.

        Use :meth:`stream` to follow a topic continuously.
        """
        messages: list[bytes] = []

        if max_messages < 1:
            return messages

        stream = self.stream(
            topic,
            group_suffix=group_suffix,
            idle_timeout=self._resolve_timeout(timeout),
        )

        try:
            for message in stream:
                messages.append(message)

                if len(messages) >= max_messages:
                    break
        finally:
            stream.close()

        return messages

    def close(self) -> None:
        """Release the underlying admin client, if one was created."""
        if self._admin_client is not None:
            self._close_quietly(self._admin_client)
            self._admin_client = None

    # --- internals ---------------------------------------------------------

    def _resolve_timeout(self, timeout: float | None) -> float:
        return self._timeout if timeout is None else timeout

    def _client_config(self) -> dict[str, Any]:
        """Build the librdkafka config.

        Never log or print the result — it holds the endpoint and the API key.
        """
        return {
            "bootstrap.servers": self._credentials.bootstrap_servers,
            "security.protocol": SECURITY_PROTOCOL,
            "sasl.mechanisms": SASL_MECHANISM,
            "sasl.username": self._credentials.account_id,
            "sasl.password": self._credentials.api_key,
            # The brokers present a publicly trusted certificate, so the system
            # CA store verifies them. Never disable this, and never point it at
            # a bundled CA.
            "enable.ssl.certificate.verification": True,
            "client.id": "aperiodic-python-client",
            "socket.timeout.ms": int(self._timeout * 1000),
            "log_level": _RDKAFKA_LOG_LEVEL,
            "log.connection.close": False,
            "logger": _RDKAFKA_LOGGER,
            "error_cb": self._on_error,
        }

    def _consumer_config(self, group_id: str) -> dict[str, Any]:
        return {
            **self._client_config(),
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
            # Brokers reject session timeouts below their configured floor
            # (6s on a default cluster), so never derive one shorter than that.
            "session.timeout.ms": max(6000, int(self._timeout * 1000)),
        }

    def _admin(self) -> Any:
        if self._admin_client is None:
            confluent_kafka = _require_confluent_kafka()
            self._admin_client = confluent_kafka.admin.AdminClient(
                self._client_config()
            )

        return self._admin_client

    def _fetch_metadata(
        self,
        *,
        timeout: float | None,
        action: str,
        topic: str | None = None,
    ) -> Any:
        confluent_kafka = _require_confluent_kafka()

        self._last_error = None

        try:
            return self._admin().list_topics(
                topic=topic, timeout=self._resolve_timeout(timeout)
            )
        except confluent_kafka.KafkaException as exc:
            raise self._stream_error(exc.args[0], action=action) from None

    def _on_error(self, error: KafkaError) -> None:
        """librdkafka error callback — the only place auth failures surface.

        A rejected SASL handshake does not fail the blocking call directly; the
        call just times out. Capturing the callback's diagnosis is what lets a
        timeout be reported as the authentication error it really was.
        """
        self._last_error = error
        logger.debug(
            "Stream error [%s]: %s", error.name(), self._redactor.scrub(error.str())
        )

    def _resolve_error(self, primary: KafkaError) -> KafkaError:
        captured = self._last_error

        if captured is None:
            return primary

        if _error_class(captured.name()) in (StreamAuthError, StreamAuthorizationError):
            return captured

        return primary

    def _stream_error(self, error: KafkaError, *, action: str) -> StreamingError:
        """Translate a librdkafka error into a redacted exception.

        Always raise the result with ``from None``. Chaining the original would
        reprint librdkafka's message — broker address and all — in the traceback,
        undoing the redaction.
        """
        resolved = self._resolve_error(error)
        code = resolved.name()

        return _error_class(code)(
            f"{action} failed [{code}]: {self._redactor.scrub(resolved.str())}",
            code=code,
        )

    def _raise_if_failed(self, *, action: str, received: bool) -> None:
        """Surface a failure the polling loop swallowed by simply ending empty.

        Authentication and authorization failures always raise. A connection
        failure only raises when nothing at all came through — once messages
        have arrived, a dropped broker connection is a blip librdkafka retries,
        not a reason to fail the caller.
        """
        captured = self._last_error

        if captured is None:
            return

        error_class = _error_class(captured.name())

        if error_class in (StreamAuthError, StreamAuthorizationError):
            raise self._stream_error(captured, action=action) from None

        if error_class is StreamConnectionError and not received:
            raise self._stream_error(captured, action=action) from None

    def _close_quietly(self, resource: Any) -> None:
        try:
            resource.close()
        except Exception as exc:
            # Teardown must never mask the error that brought us here.
            logger.debug(
                "Ignoring error while closing stream resource: %s", type(exc).__name__
            )


def _starts_with_foreign_account_id(suffix: str) -> bool:
    """Whether ``suffix`` opens with a full account id followed by more text.

    Necessarily *another* account's — a suffix carrying this client's own id was
    returned as already-qualified before reaching here. The whole UUID shape is
    required rather than a hex-looking head, so ordinary suffixes built from a
    short commit sha (``a1b2c3d4-worker``) are not caught by it.
    """
    return _FOREIGN_ACCOUNT_ID.match(suffix) is not None


__all__ = [
    "DEFAULT_STREAM_TIMEOUT",
    "SASL_MECHANISM",
    "SECURITY_PROTOCOL",
    "ClusterInfo",
    "KafkaStreamClient",
    "TopicName",
]
