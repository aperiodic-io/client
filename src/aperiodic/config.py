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


# The `timestamp=` query parameter. Names the timestamp *type* an object was built
# against ("exchange" or "true") — it is not a column in the data.
TIMESTAMP_PARAM = "timestamp"

# The interval-start column every metric parquet carries, written as a timestamp.
#
# Kept deliberately separate from TIMESTAMP_PARAM. One constant used to serve both
# roles, which meant the trailing sort looked for a column named "timestamp" that the
# parquets never had — so it silently never ran — while an object that *did* carry a
# stray "timestamp" column got its partition value parsed as a datetime instead.
TIME_COLUMN = "time"

MAX_CONCURRENT_DOWNLOADS = 10

MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # Base for exponential backoff (seconds)
DEFAULT_TIMEOUT = 60.0  # Default HTTP timeout (seconds)
