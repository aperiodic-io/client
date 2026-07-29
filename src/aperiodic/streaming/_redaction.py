"""Redaction of infrastructure identifiers from streaming errors and logs.

librdkafka embeds the broker address in nearly every error string and log
record it emits — ``Failed to resolve 'host:9093'``, or
``sasl_ssl://host:9093/bootstrap: Connect to ipv4#10.0.0.1:9093 failed``. The
Aperiodic streaming endpoint is not published, this repository is public, and
its CI logs are world-readable, so no librdkafka-authored text may reach a
caller or a log handler verbatim.

Scrubbing runs in three passes, in this order:

1. Literal replacement of the credentials we were handed (API key, account id,
   bootstrap endpoint). Exact, and always correct.
2. Structural replacement of anything *shaped* like a network address. This is
   the pass that matters: a cluster advertises its brokers under hostnames the
   client never configured, so literal matching cannot catch them — and neither
   can GitHub's secret masking, which only knows the literal secret value.
3. Literal replacement of the bare hostnames and their parent domains, catching
   single-label internal names that pass 2 untouched.

The API key is replaced in pass 1 rather than 3 so that a key containing dots
cannot be partially exposed by the hostname rules.

Pass 2 is deliberately eager. It can swallow a dotted token that was not a
hostname at all (``ssl.ca.location`` becomes ``[redacted-host]``); losing a
word from an error message is the acceptable side of that trade.
"""

from __future__ import annotations

import logging
import re
import traceback
import weakref
from dataclasses import dataclass

API_KEY_PLACEHOLDER = "[redacted-api-key]"
ACCOUNT_ID_PLACEHOLDER = "[redacted-account-id]"
BOOTSTRAP_PLACEHOLDER = "[redacted-bootstrap]"
ENDPOINT_PLACEHOLDER = "[redacted-endpoint]"
HOST_PLACEHOLDER = "[redacted-host]"
ADDRESS_PLACEHOLDER = "[redacted-address]"

_IPV6 = re.compile(r"\[[0-9A-Fa-f]{0,4}(?::[0-9A-Fa-f]{0,4}){2,7}\](?::\d{1,5})?")
_IPV4 = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d{1,5})?\b")

# host:port. The lookahead requires a dot or hyphen in the first label, which
# keeps ordinary "Word:1234" prose and topic:partition notation intact while
# still catching single-label internal names such as "redpanda-0:9093".
_HOST_PORT = re.compile(
    r"\b(?=[A-Za-z0-9-]*[.-])"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)*"
    r":\d{2,5}\b"
)

# Bare FQDN. The final label must be purely alphabetic, so dataset topic names
# such as "ohlcv.binance-futures.m1" (final label carries a digit) survive. The
# surrounding lookaround matters as much as the pattern: without it "ohlcv" +
# "binance" reads as a two-label hostname inside that same topic name.
_FQDN = re.compile(
    r"(?<![A-Za-z0-9._-])"
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,24}"
    r"(?![A-Za-z0-9-])"
)


def scrub_structural(text: str) -> str:
    """Replace anything shaped like a host, address or endpoint in ``text``."""
    text = _IPV6.sub(ADDRESS_PLACEHOLDER, text)
    text = _IPV4.sub(ADDRESS_PLACEHOLDER, text)
    text = _HOST_PORT.sub(ENDPOINT_PLACEHOLDER, text)
    return _FQDN.sub(HOST_PLACEHOLDER, text)


def _parent_domains(host: str) -> list[str]:
    """Return ``host``'s parent domains, stopping above the two-label suffix.

    ``kafka-0.stream.example.net`` yields ``stream.example.net`` and
    ``example.net``. The bare public suffix is never returned — it is too
    generic to replace safely.
    """
    labels = host.split(".")
    return [".".join(labels[i:]) for i in range(1, len(labels) - 1)]


def _split_hosts(bootstrap_servers: str) -> list[str]:
    """Extract the hostnames from a ``host:port[,host:port...]`` list."""
    hosts = []
    for entry in bootstrap_servers.split(","):
        host = entry.strip().rsplit(":", 1)[0].strip("[]")
        if host:
            hosts.append(host)
    return hosts


@dataclass(frozen=True)
class Redactor:
    """Scrubs a known set of secrets, and anything shaped like an address.

    Instances register themselves in a process-wide weak registry so that
    :func:`scrub_text` — used by the log filter, which has no reference to any
    particular client — can strip the secrets of every live client.
    """

    literals: tuple[tuple[str, str], ...] = ()
    host_literals: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        _ACTIVE_REDACTORS.add(self)

    @classmethod
    def for_credentials(
        cls,
        *,
        bootstrap_servers: str,
        account_id: str,
        api_key: str,
    ) -> Redactor:
        literals = [
            (api_key, API_KEY_PLACEHOLDER),
            (account_id, ACCOUNT_ID_PLACEHOLDER),
            (bootstrap_servers, BOOTSTRAP_PLACEHOLDER),
        ]

        host_literals = []
        for host in _split_hosts(bootstrap_servers):
            host_literals.append((host, HOST_PLACEHOLDER))
            host_literals.extend(
                (parent, HOST_PLACEHOLDER) for parent in _parent_domains(host)
            )

        return cls(
            literals=tuple(_ordered(literals)),
            host_literals=tuple(_ordered(host_literals)),
        )

    def scrub(self, text: str) -> str:
        """Return ``text`` with every known secret and address removed."""
        for literal, placeholder in self.literals:
            text = text.replace(literal, placeholder)

        text = scrub_structural(text)

        for literal, placeholder in self.host_literals:
            text = re.sub(re.escape(literal), placeholder, text, flags=re.IGNORECASE)

        return text


def _ordered(literals: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Drop blanks and sort longest-first so no literal shadows a longer one."""
    return sorted(
        {
            (literal, placeholder)
            for literal, placeholder in literals
            if literal.strip()
        },
        key=lambda pair: len(pair[0]),
        reverse=True,
    )


_ACTIVE_REDACTORS: weakref.WeakSet[Redactor] = weakref.WeakSet()


def scrub_text(text: str) -> str:
    """Scrub ``text`` using every live redactor, plus the structural rules.

    Applying more redactors than strictly belong to the caller is safe: the
    passes are idempotent, and over-redacting is the failure mode we want.
    """
    redactors = tuple(_ACTIVE_REDACTORS)

    if not redactors:
        return scrub_structural(text)

    for redactor in redactors:
        text = redactor.scrub(text)

    return text


class RedactingLogFilter(logging.Filter):
    """Scrubs every record passing through it, including attached tracebacks."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()

        if record.exc_info:
            message = (
                f"{message}\n{''.join(traceback.format_exception(*record.exc_info))}"
            )
            record.exc_info = None
            record.exc_text = None

        record.msg = scrub_text(message)
        record.args = ()

        return True


def install_log_filter(logger: logging.Logger) -> logging.Logger:
    """Attach the redacting filter to ``logger`` exactly once."""
    if not any(isinstance(existing, RedactingLogFilter) for existing in logger.filters):
        logger.addFilter(RedactingLogFilter())

    return logger


__all__ = [
    "ACCOUNT_ID_PLACEHOLDER",
    "ADDRESS_PLACEHOLDER",
    "API_KEY_PLACEHOLDER",
    "BOOTSTRAP_PLACEHOLDER",
    "ENDPOINT_PLACEHOLDER",
    "HOST_PLACEHOLDER",
    "RedactingLogFilter",
    "Redactor",
    "install_log_filter",
    "scrub_structural",
    "scrub_text",
]
