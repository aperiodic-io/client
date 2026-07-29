# Contributing

## Local setup

```bash
pip install -e ".[polars,tests,quality]"
```

Run the tests and the linter:

```bash
pytest tests/ --durations 0
ruff check .
```

## Live streams (Kafka)

The streaming client lives in `src/aperiodic/streaming/`. Working on it needs
the extra:

```bash
pip install -e ".[streaming,tests]"
pytest tests/test_streaming.py
```

`tests/test_streaming.py` needs no network and no credentials. It is the gate on
the redaction guarantees — **if you change `_redaction.py`, that suite is the
thing to run first.**

`tests/test_kafka_acceptance.py` talks to a real cluster and skips, rather than
fails, when the environment variables below are absent. That is what makes fork
pull requests work: outside contributors get no secrets, so the acceptance test
skips and CI stays green.

### This repository is public

The bootstrap endpoint, the test account id and its API key are secrets. None of
them may be committed, and none may reach a CI log.

The trap worth knowing about: **librdkafka puts the broker address in its error
messages and logs.** A connection failure produces
`Failed to resolve '<host>:<port>'`, and an unconfigured client writes that
straight to stderr from C. GitHub's secret masking does not save you here — it
only knows the literal secret value, and a cluster advertises brokers under
hostnames that were never configured, so masking has nothing to match.

The wrapper handles this by routing librdkafka through a redacting logger and
scrubbing anything address-shaped out of its own errors, `repr()` and
tracebacks. When touching that code:

- Never log or print the client config dict — it holds the endpoint and the key.
- Re-raise translated librdkafka errors with `from None`. `from exc` reprints
  the original message, broker address and all, in the traceback.
- Keep assertions free of the endpoint, account id and API key. pytest prints
  the operands of a failing assertion; reduce such comparisons to a boolean
  first.
- In workflows: no `set -x`, no `echo` of any of these values, and
  `::add-mask::` anything *derived* from a secret — masking only covers the
  literal value.

## CI secrets

Configured in **this repository's** settings. Names only below; values live in
the secret store.

| Secret | Purpose |
|--------|---------|
| `APERIODIC_KAFKA_BOOTSTRAP` | Kafka bootstrap endpoint, `host:port` |
| `APERIODIC_KAFKA_ACCOUNT_ID` | Test account id — the SASL username |
| `APERIODIC_KAFKA_API_KEY` | That account's Aperiodic data API key — the SASL password |

Use a dedicated throwaway test account, never a real customer's.

There is one optional **variable** (not a secret), used to check the topic
listing exactly rather than loosely:

| Variable | Purpose |
|----------|---------|
| `APERIODIC_KAFKA_ENTITLED_DATASETS` | Comma-separated dataset prefixes the test account's tier covers, e.g. `ohlcv,l1_liquidity` |

The existing REST integration secrets (`APERIODIC_API_KEY`,
`APERIODIC_STAGING_API_KEY`, `CF_ACCESS_CLIENT_ID`, `CF_ACCESS_CLIENT_SECRET`)
are unchanged.

### Router repository

`dream-faster/unravel-router` dispatches both `tests.yaml` and
`kafka-acceptance.yaml` here from its bump workflow, using the existing
`APERIODIC_INTEGRATION_TESTS_PAT` secret. That PAT needs "Actions: write" on
this repository. No new secret is required on the router side — the Kafka
credentials stay here.
