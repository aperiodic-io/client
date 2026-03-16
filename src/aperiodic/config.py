import os

DEFAULT_BASE_URL = os.environ.get("APERIODIC_API_URL") or "https://aperiodic.io/api/v1"


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
