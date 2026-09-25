"""Exceptions raised by the streaming client.

Every message reaching these constructors has already passed through
:mod:`aperiodic.streaming._redaction`. ``code`` carries librdkafka's symbolic
error name (``TOPIC_AUTHORIZATION_FAILED``, ``_TRANSPORT``, ...), which is safe
to surface — unlike the error *string*, it never embeds a broker address.
"""

from __future__ import annotations

from ..client import AperiodicDataError


class StreamingError(AperiodicDataError):
    """Base exception for the Aperiodic streaming client."""

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.code = code


class StreamConfigError(StreamingError):
    """Raised when the streaming client is missing or given invalid settings."""


class StreamGroupIdError(StreamConfigError):
    """Raised when a consumer group id does not carry the account id prefix."""


class StreamConnectionError(StreamingError):
    """Raised when the cluster could not be reached — DNS, TLS or timeout."""


class StreamAuthError(StreamingError):
    """Raised when the account id / API key pair was rejected by the broker."""


class StreamAuthorizationError(StreamingError):
    """Raised when the account is authenticated but not entitled to a resource."""


__all__ = [
    "StreamAuthError",
    "StreamAuthorizationError",
    "StreamConfigError",
    "StreamConnectionError",
    "StreamGroupIdError",
    "StreamingError",
]
