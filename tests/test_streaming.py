"""Unit tests for the streaming client. No network, no credentials.

Covers the three things that must hold before anything talks to a broker:
config building, consumer group prefix enforcement, and — most importantly —
redaction of the endpoint, account id and API key from everything the client
raises or logs.
"""

from __future__ import annotations

import logging
import traceback

import pytest

from aperiodic.streaming import (
    ENV_ACCOUNT_ID,
    ENV_API_KEY,
    ENV_BOOTSTRAP,
    SASL_MECHANISM,
    SECURITY_PROTOCOL,
    KafkaStreamClient,
    StreamConfigError,
    StreamConnectionError,
    StreamCredentials,
    StreamGroupIdError,
    StreamingError,
    TopicName,
    is_configured,
)
from aperiodic.streaming._redaction import (
    RedactingLogFilter,
    Redactor,
    scrub_structural,
)
from aperiodic.streaming.client import _HAS_CONFLUENT_KAFKA

# Stand-ins with the same shape as the real values. Nothing here is real, and
# nothing real may ever be written into this file.
FAKE_BOOTSTRAP = "seed-7f2a.streams.example.net:9093"
FAKE_HOST = "seed-7f2a.streams.example.net"
FAKE_ACCOUNT_ID = "3f9b1c04-8a2e-4d61-9f70-2b5c8ad41e77"
FAKE_API_KEY = "test-api-key-not-a-real-credential"

FAKE_ENV = {
    ENV_BOOTSTRAP: FAKE_BOOTSTRAP,
    ENV_ACCOUNT_ID: FAKE_ACCOUNT_ID,
    ENV_API_KEY: FAKE_API_KEY,
}

# Verbatim shapes librdkafka produces. These are exactly the strings that would
# otherwise land in a public CI log.
RDKAFKA_RESOLVE_FAILURE = (
    f"Failed to resolve '{FAKE_BOOTSTRAP}': Name or service not known "
    f"(after 12ms in state CONNECT)"
)
RDKAFKA_CONNECT_FAILURE = (
    f"sasl_ssl://{FAKE_BOOTSTRAP}/bootstrap: Connect to ipv4#10.12.4.31:9093 "
    f"failed: Connection refused (after 3ms in state CONNECT)"
)
RDKAFKA_SASL_FAILURE = (
    f"sasl_ssl://{FAKE_BOOTSTRAP}/1: SASL authentication error: "
    f"SASL_AUTHENTICATION_FAILED: Invalid credentials for user {FAKE_ACCOUNT_ID}"
)
# The advertised broker the client never configured — literal masking (and
# GitHub's secret masking) cannot know this name exists.
RDKAFKA_ADVERTISED_BROKER = (
    "broker-3.internal.example.net:9093/3: Disconnected (after 1002ms in state UP)"
)

SECRETS = (FAKE_BOOTSTRAP, FAKE_HOST, FAKE_ACCOUNT_ID, FAKE_API_KEY)


@pytest.fixture
def redactor():
    return Redactor.for_credentials(
        bootstrap_servers=FAKE_BOOTSTRAP,
        account_id=FAKE_ACCOUNT_ID,
        api_key=FAKE_API_KEY,
    )


@pytest.fixture
def client():
    return KafkaStreamClient(
        bootstrap_servers=FAKE_BOOTSTRAP,
        account_id=FAKE_ACCOUNT_ID,
        api_key=FAKE_API_KEY,
    )


def assert_scrubbed(text: str) -> None:
    """Assert no secret survives in ``text``, without echoing the secret."""
    for name, secret in zip(
        ("bootstrap", "host", "account id", "api key"), SECRETS, strict=True
    ):
        assert secret not in text, f"{name} leaked into: {text!r}"


class TestRedaction:
    """The reason this wrapper exists. Failures here are security failures."""

    def test_resolve_failure_is_scrubbed(self, redactor):
        scrubbed = redactor.scrub(RDKAFKA_RESOLVE_FAILURE)

        assert_scrubbed(scrubbed)
        assert "Name or service not known" in scrubbed

    def test_connect_failure_scrubs_endpoint_and_ip(self, redactor):
        scrubbed = redactor.scrub(RDKAFKA_CONNECT_FAILURE)

        assert_scrubbed(scrubbed)
        assert "10.12.4.31" not in scrubbed
        assert "Connection refused" in scrubbed

    def test_sasl_failure_scrubs_account_id_but_keeps_error_code(self, redactor):
        scrubbed = redactor.scrub(RDKAFKA_SASL_FAILURE)

        assert_scrubbed(scrubbed)
        assert "SASL_AUTHENTICATION_FAILED" in scrubbed

    def test_advertised_broker_hostname_is_scrubbed(self, redactor):
        """The hostname was never configured, so only structural rules catch it."""
        scrubbed = redactor.scrub(RDKAFKA_ADVERTISED_BROKER)

        assert "broker-3.internal.example.net" not in scrubbed
        assert "example.net" not in scrubbed
        assert "Disconnected" in scrubbed

    def test_structural_scrub_needs_no_credentials(self):
        """Redaction still applies before any client is constructed."""
        scrubbed = scrub_structural(RDKAFKA_RESOLVE_FAILURE)

        assert FAKE_HOST not in scrubbed
        assert FAKE_BOOTSTRAP not in scrubbed

    @pytest.mark.parametrize(
        "preserved",
        [
            "TOPIC_AUTHORIZATION_FAILED",
            "GROUP_AUTHORIZATION_FAILED",
            "Broker: Topic authorization failed",
            "ohlcv.binance-futures.m1",
            "l2_imbalance.okx-perps.s1",
            "Local: Timed out",
        ],
    )
    def test_useful_diagnostics_survive(self, redactor, preserved):
        """Over-redacting to the point of uselessness is its own failure."""
        assert redactor.scrub(preserved) == preserved

    def test_api_key_containing_dots_is_fully_redacted(self):
        """A key with dots is hostname-shaped. It must be replaced whole, not
        chewed on by the structural rules and left partly readable."""
        dotted_key = "live.k3y.va1ue"

        scrubbed = Redactor.for_credentials(
            bootstrap_servers=FAKE_BOOTSTRAP,
            account_id=FAKE_ACCOUNT_ID,
            api_key=dotted_key,
        ).scrub(f"SASL authentication error: bad credentials for {dotted_key}")

        assert dotted_key not in scrubbed
        assert "va1ue" not in scrubbed
        assert "k3y" not in scrubbed

    def test_degenerate_endpoint_does_not_redact_everything(self):
        """A malformed endpoint must not register punctuation as a literal —
        redactors are global, so one bad client would corrupt every message in
        the process."""
        scrubbed = Redactor.for_credentials(
            bootstrap_servers="...",
            account_id=FAKE_ACCOUNT_ID,
            api_key=FAKE_API_KEY,
        ).scrub("Broker: Topic authorization failed")

        assert scrubbed == "Broker: Topic authorization failed"

    def test_scrub_is_idempotent(self, redactor):
        once = redactor.scrub(RDKAFKA_CONNECT_FAILURE)

        assert redactor.scrub(once) == once

    def test_client_errors_are_scrubbed(self, client):
        """A forced connection failure must not name the endpoint."""
        error = client._stream_error(
            _fake_kafka_error(), action="Cluster metadata request"
        )

        assert_scrubbed(str(error))
        assert error.code == "_TRANSPORT"

    def test_repr_redacts(self, client):
        assert_scrubbed(repr(client))

    def test_credentials_repr_redacts(self):
        assert_scrubbed(repr(StreamCredentials.from_env(FAKE_ENV)))

    def test_log_records_are_scrubbed(self, redactor, caplog):
        """librdkafka log records route through the filter, not stderr."""
        record_logger = logging.getLogger("aperiodic.streaming.rdkafka")
        record_logger.addFilter(RedactingLogFilter())

        with caplog.at_level(logging.ERROR, logger=record_logger.name):
            record_logger.error("%s", RDKAFKA_CONNECT_FAILURE)

        assert caplog.records
        for record in caplog.records:
            assert_scrubbed(record.getMessage())

    @pytest.mark.skipif(
        not _HAS_CONFLUENT_KAFKA,
        reason="requires the streaming extra (confluent-kafka)",
    )
    def test_forced_connection_failure_leaks_nothing(self, capfd, caplog):
        """The stubs above assume the shape of librdkafka's output; this makes
        librdkafka actually produce it, against a closed local port so no
        external network is involved.

        Every surface that could reach a public CI log is checked at once.
        ``capfd`` rather than ``capsys`` because librdkafka writes its FAIL
        lines from C, straight to the process's stderr — an unconfigured client
        prints ``Connect to ipv4#127.0.0.1:9 failed`` there, below the reach of
        Python-level capture.
        """
        unreachable = "127.0.0.1:9"

        client = KafkaStreamClient(
            bootstrap_servers=unreachable,
            account_id=FAKE_ACCOUNT_ID,
            api_key=FAKE_API_KEY,
            timeout=3.0,
        )

        with (
            caplog.at_level(logging.DEBUG, logger="aperiodic.streaming.rdkafka"),
            pytest.raises(StreamingError) as exc_info,
        ):
            client.consume("ohlcv.binance-futures.m1", group_suffix="unit", timeout=3.0)

        captured = capfd.readouterr()
        surfaces = {
            "exception": str(exc_info.value),
            "traceback": "".join(
                traceback.format_exception(
                    type(exc_info.value), exc_info.value, exc_info.value.__traceback__
                )
            ),
            "stdout": captured.out,
            "stderr": captured.err,
            "log records": "\n".join(record.getMessage() for record in caplog.records),
        }

        for surface, text in surfaces.items():
            assert unreachable not in text, f"endpoint leaked into {surface}"
            assert "127.0.0.1" not in text, f"address leaked into {surface}"
            assert FAKE_API_KEY not in text, f"api key leaked into {surface}"

        # librdkafka's FAIL records do reach us on the consumer path, so the
        # filter is doing real work above rather than passing an empty list.
        assert caplog.records, "expected librdkafka to log the connection failure"
        assert exc_info.value.code

    @pytest.mark.skipif(
        not _HAS_CONFLUENT_KAFKA,
        reason="requires the streaming extra (confluent-kafka)",
    )
    def test_unreachable_cluster_is_not_reported_as_a_quiet_topic(self):
        """An empty return must mean "no messages", never "never connected"."""
        client = KafkaStreamClient(
            bootstrap_servers="127.0.0.1:9",
            account_id=FAKE_ACCOUNT_ID,
            api_key=FAKE_API_KEY,
            timeout=3.0,
        )

        with pytest.raises(StreamConnectionError):
            client.consume("ohlcv.binance-futures.m1", group_suffix="unit", timeout=3.0)

    def test_log_record_tracebacks_are_scrubbed(self, redactor, caplog):
        record_logger = logging.getLogger("aperiodic.streaming.rdkafka")
        record_logger.addFilter(RedactingLogFilter())

        with caplog.at_level(logging.ERROR, logger=record_logger.name):
            try:
                raise RuntimeError(RDKAFKA_RESOLVE_FAILURE)
            except RuntimeError:
                record_logger.exception("stream failure")

        assert caplog.records
        for record in caplog.records:
            assert_scrubbed(record.getMessage())


class TestConfig:
    def test_from_env_builds_credentials(self):
        credentials = StreamCredentials.from_env(FAKE_ENV)

        assert credentials.bootstrap_servers == FAKE_BOOTSTRAP
        assert credentials.account_id == FAKE_ACCOUNT_ID
        assert credentials.api_key == FAKE_API_KEY

    @pytest.mark.parametrize("missing", [ENV_BOOTSTRAP, ENV_ACCOUNT_ID, ENV_API_KEY])
    def test_missing_variable_is_named_without_its_value(self, missing):
        env = {name: value for name, value in FAKE_ENV.items() if name != missing}

        with pytest.raises(StreamConfigError) as exc_info:
            StreamCredentials.from_env(env)

        message = str(exc_info.value)
        assert missing in message
        assert_scrubbed(message)

    def test_blank_variable_counts_as_missing(self):
        with pytest.raises(StreamConfigError, match=ENV_API_KEY):
            StreamCredentials.from_env({**FAKE_ENV, ENV_API_KEY: "   "})

    def test_is_configured(self):
        assert is_configured(FAKE_ENV)
        assert not is_configured({**FAKE_ENV, ENV_BOOTSTRAP: ""})
        assert not is_configured({})

    def test_client_config_matches_the_connection_contract(self, client):
        config = client._client_config()

        assert config["security.protocol"] == SECURITY_PROTOCOL
        assert config["sasl.mechanisms"] == SASL_MECHANISM
        assert config["sasl.username"] == FAKE_ACCOUNT_ID
        assert config["sasl.password"] == FAKE_API_KEY
        assert config["bootstrap.servers"] == FAKE_BOOTSTRAP

    def test_tls_verification_is_on_and_uses_the_system_ca_store(self, client):
        config = client._client_config()

        assert config["enable.ssl.certificate.verification"] is True
        assert "ssl.ca.location" not in config

    def test_librdkafka_output_is_routed_through_the_redacting_logger(self, client):
        config = client._client_config()

        assert config["logger"].name == "aperiodic.streaming.rdkafka"
        assert any(isinstance(f, RedactingLogFilter) for f in config["logger"].filters)
        assert "debug" not in config

    def test_every_call_is_bounded(self, client):
        config = client._consumer_config(client.build_group_id("unit"))

        assert config["socket.timeout.ms"] > 0
        assert config["session.timeout.ms"] >= 6000

    def test_non_positive_timeout_rejected(self):
        with pytest.raises(StreamConfigError):
            KafkaStreamClient(
                bootstrap_servers=FAKE_BOOTSTRAP,
                account_id=FAKE_ACCOUNT_ID,
                api_key=FAKE_API_KEY,
                timeout=0,
            )


class TestConsumerGroupPrefix:
    def test_suffix_is_prefixed_with_the_account_id(self, client):
        assert client.build_group_id("research") == f"{FAKE_ACCOUNT_ID}-research"

    def test_already_qualified_id_is_left_alone(self, client):
        qualified = f"{FAKE_ACCOUNT_ID}-research"

        assert client.build_group_id(qualified) == qualified

    def test_consumer_config_carries_the_qualified_group(self, client):
        config = client._consumer_config(client.build_group_id("research"))

        assert config["group.id"].startswith(f"{FAKE_ACCOUNT_ID}-")

    @pytest.mark.parametrize("suffix", ["", "   "])
    def test_empty_suffix_rejected(self, client, suffix):
        with pytest.raises(StreamGroupIdError):
            client.build_group_id(suffix)

    @pytest.mark.parametrize("suffix", ["my group", "a,b"])
    def test_malformed_suffix_rejected(self, client, suffix):
        with pytest.raises(StreamGroupIdError):
            client.build_group_id(suffix)

    @pytest.mark.parametrize("suffix", ["a1b2c3d4-worker", "deadbeef-1", "run-2024-01"])
    def test_hex_looking_suffixes_are_accepted(self, client, suffix):
        """A suffix built from a short commit sha is ordinary, not a foreign
        account id — only a full UUID shape is rejected."""
        assert client.build_group_id(suffix) == f"{FAKE_ACCOUNT_ID}-{suffix}"

    def test_another_accounts_group_rejected(self, client):
        """The broker answers GROUP_AUTHORIZATION_FAILED; fail before the round trip."""
        with pytest.raises(StreamGroupIdError):
            client.build_group_id("9c2d5e10-1111-2222-3333-444455556666-research")

    @pytest.mark.parametrize("group_id", ["research", "other-account-research", ""])
    def test_validate_group_id_rejects_unprefixed(self, client, group_id):
        with pytest.raises(StreamGroupIdError):
            client.validate_group_id(group_id)

    def test_validate_group_id_accepts_prefixed(self, client):
        qualified = f"{FAKE_ACCOUNT_ID}-research"

        assert client.validate_group_id(qualified) == qualified

    def test_stream_validates_the_group_before_connecting(self, client):
        """The generator body must reject a bad group on first ``next()``,
        before any consumer is constructed."""
        with pytest.raises(StreamGroupIdError):
            next(client.stream("ohlcv.binance-futures.m1", group_suffix=""))

    def test_group_errors_do_not_echo_the_account_id(self, client):
        with pytest.raises(StreamGroupIdError) as exc_info:
            client.build_group_id("9c2d5e10-1111-2222-3333-444455556666-research")

        assert_scrubbed(str(exc_info.value))


class TestTopicName:
    def test_parses_the_documented_format(self):
        topic = TopicName.parse("ohlcv.binance-futures.m1")

        assert topic.dataset == "ohlcv"
        assert topic.exchange == "binance-futures"
        assert topic.interval == "m1"
        assert str(topic) == "ohlcv.binance-futures.m1"

    @pytest.mark.parametrize(
        "topic", ["ohlcv", "ohlcv.binance-futures", "ohlcv..m1", "a.b.c.d"]
    )
    def test_rejects_malformed_names(self, topic):
        with pytest.raises(ValueError, match="Malformed topic name"):
            TopicName.parse(topic)


def _fake_kafka_error():
    """A librdkafka transport error carrying the endpoint, as the real one does."""
    return _StubKafkaError(-195, "_TRANSPORT", RDKAFKA_CONNECT_FAILURE)


class _StubKafkaError:
    """Stands in for ``confluent_kafka.KafkaError``, which cannot be constructed
    with an arbitrary message. Deliberately free of any confluent-kafka import,
    so the redaction tests run in the matrix jobs without the streaming extra."""

    def __init__(self, code: int, name: str, message: str) -> None:
        self._code = code
        self._name = name
        self._message = message

    def code(self) -> int:
        return self._code

    def name(self) -> str:
        return self._name

    def str(self) -> str:
        return self._message
