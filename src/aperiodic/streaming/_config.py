"""Environment-driven configuration for the streaming client.

The bootstrap endpoint, the account id and the API key are all treated as
secrets. They are read from the environment and never carry a default: a
hard-coded fallback in this repository would publish the value it falls back
to. Errors name the missing *variable*, never its value.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from ._errors import StreamConfigError
from ._redaction import (
    ACCOUNT_ID_PLACEHOLDER,
    API_KEY_PLACEHOLDER,
    BOOTSTRAP_PLACEHOLDER,
)

ENV_BOOTSTRAP = "APERIODIC_KAFKA_BOOTSTRAP"
ENV_ACCOUNT_ID = "APERIODIC_KAFKA_ACCOUNT_ID"
ENV_API_KEY = "APERIODIC_KAFKA_API_KEY"

REQUIRED_ENV_VARS = (ENV_BOOTSTRAP, ENV_ACCOUNT_ID, ENV_API_KEY)


@dataclass(frozen=True, repr=False)
class StreamCredentials:
    """Everything needed to authenticate against the live metric streams.

    ``api_key`` is the account's ordinary Aperiodic data API key — the same
    credential the REST API takes. There is no separate streaming password.
    """

    bootstrap_servers: str
    account_id: str
    api_key: str

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> StreamCredentials:
        env = os.environ if env is None else env

        missing = [name for name in REQUIRED_ENV_VARS if not env.get(name, "").strip()]

        if missing:
            raise StreamConfigError(
                "Missing environment variable(s) for the Aperiodic streaming "
                f"client: {', '.join(missing)}. Set {ENV_BOOTSTRAP} to the "
                f"host:port bootstrap endpoint, {ENV_ACCOUNT_ID} to the account "
                f"id, and {ENV_API_KEY} to that account's Aperiodic data API key."
            )

        return cls(
            bootstrap_servers=env[ENV_BOOTSTRAP].strip(),
            account_id=env[ENV_ACCOUNT_ID].strip(),
            api_key=env[ENV_API_KEY].strip(),
        )

    def __repr__(self) -> str:
        return (
            f"StreamCredentials(bootstrap_servers='{BOOTSTRAP_PLACEHOLDER}', "
            f"account_id='{ACCOUNT_ID_PLACEHOLDER}', api_key='{API_KEY_PLACEHOLDER}')"
        )


def is_configured(env: Mapping[str, str] | None = None) -> bool:
    """Whether every streaming environment variable is present and non-empty.

    Lets callers — the acceptance test in particular — skip instead of fail
    when the secrets are absent, as they are for fork pull requests.
    """
    env = os.environ if env is None else env

    return all(env.get(name, "").strip() for name in REQUIRED_ENV_VARS)


__all__ = [
    "ENV_ACCOUNT_ID",
    "ENV_API_KEY",
    "ENV_BOOTSTRAP",
    "REQUIRED_ENV_VARS",
    "StreamCredentials",
    "is_configured",
]
