import asyncio
import os
from datetime import date

from dotenv import load_dotenv

from aperiodic import get_symbols_async
from src.aperiodic.endpoints.derivative import get_derivative_metrics_async

load_dotenv()


async def main():
    metric = "l1_liquidity"
    symbols = await get_symbols_async(
        api_key=os.environ["APERIODIC_API_KEY"],
        exchange="binance-futures",
    )
    symbols = [s for s in symbols if s.startswith("perpetual-")]
    print(len(symbols))
    for symbol in symbols:
        df = await get_derivative_metrics_async(
            api_key=os.environ["APERIODIC_API_KEY"],
            metric=metric,
            timestamp="true",
            interval="1h",
            exchange="binance-futures",
            symbol=symbol,
            start_date=date(2020, 1, 1),
            end_date=date(2026, 1, 1),
        )
        print(df.head())


df = asyncio.run(main())
