"""Read a live metric stream.

    pip install aperiodic[streaming]
    python examples/streaming.py

Needs three environment variables — see the "Live Streams" section of the
README:

    APERIODIC_KAFKA_BOOTSTRAP    host:port endpoint, provided by Aperiodic
    APERIODIC_KAFKA_ACCOUNT_ID   your account id (the SASL username)
    APERIODIC_KAFKA_API_KEY      your Aperiodic data API key (the SASL password)

Set APERIODIC_KAFKA_TOPIC to follow a different topic than the default.
"""

import os

from dotenv import load_dotenv

from aperiodic.streaming import KafkaStreamClient, StreamingError

load_dotenv()

TOPIC = os.environ.get("APERIODIC_KAFKA_TOPIC", "ohlcv.binance-futures.m1")


def main():
    with KafkaStreamClient.from_env() as stream:
        info = stream.cluster_metadata()
        print(f"Connected to {info.broker_count} broker(s).")

        print(f"\nEntitled topics ({len(info.topics)}):")
        for topic in info.topics:
            print(f"  {topic}")

        # The consumer group is named "<your-account-id>-example" — the account
        # id prefix is required, and applied for you.
        print(f"\nFollowing {TOPIC} — Ctrl-C to stop.")
        for message in stream.stream(TOPIC, group_suffix="example"):
            print(message.decode())


if __name__ == "__main__":
    try:
        main()
    except StreamingError as exc:
        # Already scrubbed of the endpoint, account id and API key.
        raise SystemExit(f"Stream error: {exc}") from None
    except KeyboardInterrupt:
        pass
