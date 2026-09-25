"""Acceptance test for live metric stream authentication and entitlements.

Runs against a real cluster with a dedicated throwaway test account, and
asserts on **access control** rather than message flow: that the account
connects, sees only the datasets its tier covers, and is refused everything
else.

Skips — never fails — when the ``APERIODIC_KAFKA_*`` variables are absent, as
they are for fork pull requests and outside contributors.

Two rules hold throughout this file:

- Every call is bounded. A hung acceptance test inside a release workflow is
  worse than a failing one.
- No assertion may embed the endpoint, the account id or the API key. pytest
  prints the operands of a failing assertion, so comparisons involving those
  values are reduced to a boolean first.
"""

from __future__ import annotations

import os

import pytest

from aperiodic.streaming import (
    REQUIRED_ENV_VARS,
    KafkaStreamClient,
    StreamAuthError,
    StreamAuthorizationError,
    StreamGroupIdError,
    TopicName,
    is_configured,
)

pytestmark = pytest.mark.skipif(
    not is_configured(),
    reason=(
        "live stream acceptance test requires "
        f"{', '.join(REQUIRED_ENV_VARS)} — absent for fork pull requests"
    ),
)

TIMEOUT = float(os.environ.get("APERIODIC_KAFKA_TIMEOUT", "20"))
MESSAGE_FLOW_TIMEOUT = float(os.environ.get("APERIODIC_KAFKA_MESSAGE_TIMEOUT", "10"))

# A dataset no tier grants, so no ACL can match it. Public catalog names only —
# nothing here identifies infrastructure.
UNENTITLED_TOPIC = os.environ.get(
    "APERIODIC_KAFKA_UNENTITLED_TOPIC", "acceptance-probe.binance-futures.m1"
)

# Optional, non-secret: the dataset prefixes the test account's tier covers.
# When set, the listing is checked to be a subset of it.
ENTITLED_DATASETS = frozenset(
    dataset.strip()
    for dataset in os.environ.get("APERIODIC_KAFKA_ENTITLED_DATASETS", "").split(",")
    if dataset.strip()
)

WRONG_PASSWORD = "not-a-valid-api-key"


@pytest.fixture(scope="module")
def stream():
    with KafkaStreamClient.from_env(timeout=TIMEOUT) as client:
        yield client


@pytest.fixture(scope="module")
def entitled_topics(stream):
    # Internal Kafka topics (__consumer_offsets and friends) are not part of the
    # dataset namespace and would not parse as <dataset>.<exchange>.<interval>.
    return [
        topic
        for topic in stream.list_topics(timeout=TIMEOUT)
        if not topic.startswith("_")
    ]


def test_connects_and_fetches_cluster_metadata(stream):
    """A metadata round trip proves DNS, TLS and SASL end to end.

    ``ClusterInfo`` carries a broker count rather than broker names precisely so
    that this assertion is safe to fail in a public log.
    """
    info = stream.cluster_metadata(timeout=TIMEOUT)

    assert info.broker_count > 0


def test_topic_listing_is_well_formed(entitled_topics):
    assert entitled_topics, "the test account should be entitled to at least one topic"

    for topic in entitled_topics:
        TopicName.parse(topic)


def test_topic_listing_contains_only_entitled_datasets(entitled_topics):
    """Listing is entitlement-filtered server-side; anything else is a leak."""
    datasets = {TopicName.parse(topic).dataset for topic in entitled_topics}

    assert UNENTITLED_TOPIC not in entitled_topics

    if not ENTITLED_DATASETS:
        pytest.skip(
            "set APERIODIC_KAFKA_ENTITLED_DATASETS to check the listing exactly"
        )

    unexpected = datasets - ENTITLED_DATASETS

    assert not unexpected, (
        f"listing exposed datasets outside the account's tier: {sorted(unexpected)}"
    )


def test_unentitled_dataset_is_denied(stream):
    with pytest.raises(StreamAuthorizationError) as exc_info:
        stream.check_topic_access(UNENTITLED_TOPIC, timeout=TIMEOUT)

    assert exc_info.value.code == "TOPIC_AUTHORIZATION_FAILED"


def test_wrong_password_is_rejected(stream):
    """Same account id, wrong key. The account id is never asserted on."""
    with (
        KafkaStreamClient(
            bootstrap_servers=os.environ["APERIODIC_KAFKA_BOOTSTRAP"],
            account_id=stream.account_id,
            api_key=WRONG_PASSWORD,
            timeout=TIMEOUT,
        ) as impostor,
        pytest.raises(StreamAuthError) as exc_info,
    ):
        impostor.cluster_metadata(timeout=TIMEOUT)

    assert exc_info.value.code in {"_AUTHENTICATION", "SASL_AUTHENTICATION_FAILED"}


def test_consumer_group_outside_the_account_prefix_is_rejected(stream):
    """The broker answers GROUP_AUTHORIZATION_FAILED for any group not named
    ``<account-id>-<suffix>``. The wrapper refuses to build one, so the round
    trip never happens — assert the wrapper raises."""
    with pytest.raises(StreamGroupIdError):
        stream.validate_group_id("acceptance-test-group")

    with pytest.raises(StreamGroupIdError):
        stream.build_group_id("")

    # Reduced to a boolean so a failure cannot print the account id.
    is_prefixed = stream.build_group_id("acceptance").startswith(stream.group_prefix)

    assert is_prefixed, "build_group_id must apply the account-id prefix"


def test_entitled_topic_is_readable_even_when_quiet(stream, entitled_topics):
    """Bounded read of a real topic.

    Staging metric topics are currently empty, so an empty result is a pass —
    this asserts we are *allowed* to read, not that data is flowing. Refusal or
    an unreachable cluster raises out of ``consume``.
    """
    topic = entitled_topics[0]

    messages = stream.consume(
        topic,
        group_suffix="acceptance",
        timeout=MESSAGE_FLOW_TIMEOUT,
        max_messages=1,
    )

    assert isinstance(messages, list)
