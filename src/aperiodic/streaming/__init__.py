"""Live metric streams over Kafka.

Requires the streaming extra:

```bash
pip install aperiodic[streaming]
```

See :class:`KafkaStreamClient` for usage, and the "Live Streams" section of the
README for the connection contract and topic naming.
"""

from ._config import (
    ENV_ACCOUNT_ID,
    ENV_API_KEY,
    ENV_BOOTSTRAP,
    REQUIRED_ENV_VARS,
    StreamCredentials,
    is_configured,
)
from ._errors import (
    StreamAuthError,
    StreamAuthorizationError,
    StreamConfigError,
    StreamConnectionError,
    StreamGroupIdError,
    StreamingError,
)
from .client import (
    DEFAULT_STREAM_TIMEOUT,
    SASL_MECHANISM,
    SECURITY_PROTOCOL,
    ClusterInfo,
    KafkaStreamClient,
    TopicName,
)

__all__ = [
    "DEFAULT_STREAM_TIMEOUT",
    "ENV_ACCOUNT_ID",
    "ENV_API_KEY",
    "ENV_BOOTSTRAP",
    "REQUIRED_ENV_VARS",
    "SASL_MECHANISM",
    "SECURITY_PROTOCOL",
    "ClusterInfo",
    "KafkaStreamClient",
    "StreamAuthError",
    "StreamAuthorizationError",
    "StreamConfigError",
    "StreamConnectionError",
    "StreamCredentials",
    "StreamGroupIdError",
    "StreamingError",
    "TopicName",
    "is_configured",
]
