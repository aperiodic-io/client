from __future__ import annotations

from ..client import get_http_client, handle_api_error, run_async
from ..config import DEFAULT_BASE_URL
from ..types import Exchange, SymbolsResponse


async def get_symbols_async(
    api_key: str,
    exchange: Exchange,
    bucket: str = "ohlcv",
    base_url: str = DEFAULT_BASE_URL,
) -> list[str]:
    """
    Get list of available symbols for an exchange.

    Retrieves all trading pair symbols available in the specified bucket
    for the given exchange.

    Args:
        api_key: Your Unravel API key
        exchange: Source exchange ('binance-futures', 'binance')
        bucket: Data bucket (default: 'ohlcv')
        base_url: API base URL (default: https://unravel.finance/api/v1)

    Returns:
        list[str]: List of available symbol names (lowercase)

    Raises:
        APIError: If the API returns an error response

    Example:
        >>> from unravel_data_client import get_symbols
        >>>
        >>> symbols = get_symbols(
        ...     api_key="your-api-key",
        ...     exchange="binance-futures",
        ... )
        >>> print(f"Found {len(symbols)} symbols")
        >>> print(symbols[:10])  # First 10 symbols
    """
    async with get_http_client(timeout=30.0) as client:
        url = f"{base_url}/data/symbols"
        params = {
            "exchange": exchange,
            "bucket": bucket,
        }
        headers = {"X-API-KEY": api_key}

        response = await client.get(url, params=params, headers=headers)
        await handle_api_error(response)

        data: SymbolsResponse = response.json()
        return data["symbols"]


def get_symbols(
    api_key: str,
    exchange: Exchange,
    bucket: str = "ohlcv",
    base_url: str = DEFAULT_BASE_URL,
) -> list[str]:
    return run_async(
        get_symbols_async(
            api_key=api_key,
            exchange=exchange,
            bucket=bucket,
            base_url=base_url,
        )
    )
