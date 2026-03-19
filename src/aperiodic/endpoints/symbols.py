from __future__ import annotations

from .._compat import fetch_json
from ..client import run_async
from ..config import DEFAULT_BASE_URL, get_headers
from ..types import Exchange, SymbolsResponse


async def get_symbols_async(
    api_key: str,
    exchange: Exchange,
    base_url: str = DEFAULT_BASE_URL,
) -> list[str]:
    """
    Get list of available symbols for an exchange.

    Retrieves all trading pair symbols available in the specified bucket
    for the given exchange.

    Args:
        api_key: Your Aperiodic API key
        exchange: Source exchange ('binance-futures', 'binance')
        bucket: Data bucket (default: 'ohlcv')
        base_url: API base URL (default: https://aperiodic.io/api/v1)

    Returns:
        list[str]: List of available symbol names in Atlas unified symbology
                   (https://github.com/aperiodic-io/atlas), eg. 'perpetual-BTC-USDT:USDT'

    Raises:
        APIError: If the API returns an error response

    Example:
        >>> from aperiodic import get_symbols
        >>>
        >>> symbols = get_symbols(
        ...     api_key="your-api-key",
        ...     exchange="binance-futures",
        ... )
        >>> print(f"Found {len(symbols)} symbols")
        >>> print(symbols[:10])  # First 10 symbols
    """
    url = f"{base_url}/metadata/symbols"
    params = {"exchange": exchange}
    headers = get_headers(api_key)

    data: SymbolsResponse = await fetch_json(url, params=params, headers=headers)
    return data["symbols"]


def get_symbols(
    api_key: str,
    exchange: Exchange,
    base_url: str = DEFAULT_BASE_URL,
) -> list[str]:
    return run_async(
        get_symbols_async(
            api_key=api_key,
            exchange=exchange,
            base_url=base_url,
        )
    )
