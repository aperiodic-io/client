import os

DEFAULT_BASE_URL = os.environ.get("APERIODIC_API_URL") or "https://aperiodic.io/api/v1"

# Shared public demo key. Preview data (preview=True) is served against this key
# so users can query the whitelisted preview slice without signing up. Keep in
# sync with the `preview@aperiodic.io` credential advertised on aperiodic.io.
DEMO_API_KEY = "DEMO-KEY"


def get_headers(api_key: str) -> dict[str, str]:
    """Build request headers, optionally including Cloudflare Access service token."""
    headers = {"X-API-KEY": api_key}
    cf_client_id = os.environ.get("CF_ACCESS_CLIENT_ID")
    cf_client_secret = os.environ.get("CF_ACCESS_CLIENT_SECRET")
    if cf_client_id and cf_client_secret:
        headers["CF-Access-Client-Id"] = cf_client_id
        headers["CF-Access-Client-Secret"] = cf_client_secret
    return headers

TIMESTAMP_COL = "timestamp"

MAX_CONCURRENT_DOWNLOADS = 10

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # Base for exponential backoff (seconds)
DEFAULT_TIMEOUT = 60.0  # Default HTTP timeout (seconds)

# Statuses that clear on their own: rate limits, and the 503 the API returns
# when one of its own upstreams (e.g. the Atlas symbol source) is momentarily
# unavailable. Anything else is a real answer and is surfaced to the caller.
RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})

# Ceiling on a single backoff wait, including a server-sent `Retry-After`.
# Calls are synchronous by default, so an honest-but-long upstream hint must
# not stall a notebook (or a test suite) for minutes.
MAX_RETRY_DELAY = 10.0
