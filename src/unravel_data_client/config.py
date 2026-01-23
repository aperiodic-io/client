# Default API base URL
# DEFAULT_BASE_URL = "https://unravel.finance/api/v1"
DEFAULT_BASE_URL = "http://localhost:6173/api/v1"

# Maximum concurrent downloads
MAX_CONCURRENT_DOWNLOADS = 10

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # Base for exponential backoff (seconds)
DEFAULT_TIMEOUT = 60.0  # Default HTTP timeout (seconds)
