"""Raw per-tick data: trades, quotes and derivative ticks (Prime + Raw plan).

History before 2026-08 is served as one Parquet file per calendar month, and
one file per day from 2026-08-01 on. ``get_raw`` loads a range into one
DataFrame; ``download_raw`` streams the files to disk in the bucket's own
folder layout, which suits ranges too large for memory.
"""

from __future__ import annotations

import asyncio
import os
import warnings
from calendar import monthrange
from contextlib import AsyncExitStack
from datetime import UTC, date, datetime, time, timedelta
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, overload

from tqdm.auto import tqdm

from .._compat import get_backend_module
from ..client import (
    HAS_HTTPX,
    AperiodicDataError,
    DownloadError,
    download_parquet_bytes,
    fetch_json,
    run_async,
)
from ..config import (
    DEFAULT_BASE_URL,
    MAX_CONCURRENT_DOWNLOADS,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    get_headers,
)
from ..types import Exchange, OutputFormat, RawDataset, RawFileInfo
from .utils import _resolve_api_key

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl

# The API serves at most this many days per request; longer ranges are split.
RAW_MAX_RANGE_DAYS = 366

# Above this, get_raw warns that download_raw is the better fit.
LARGE_RAW_BYTES = 2 * 1024**3

EXCHANGE_TIMESTAMP = "exchange_timestamp"

DEFAULT_RAW_DOWNLOADS = 4

# ``:`` (in every symbol) is not a legal file-name character on Windows.
_ESCAPE_COLON = os.name == "nt"


# ---------------------------------------------------------------------------
# Periods and paths
# ---------------------------------------------------------------------------


def _period_bounds(period: str) -> tuple[date, date]:
    """First and last day covered by a ``YYYY-MM`` or ``YYYY-MM-DD`` period."""
    parts = [int(p) for p in period.split("-")]
    if len(parts) == 3:
        day = date(parts[0], parts[1], parts[2])
        return day, day
    year, month = parts
    return date(year, month, 1), date(year, month, monthrange(year, month)[1])


def _period_parts(period: str) -> tuple[int, int, int | None]:
    parts = [int(p) for p in period.split("-")]
    return parts[0], parts[1], parts[2] if len(parts) == 3 else None


def _chunks(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """Split an inclusive range into windows the API accepts."""
    if end_date < start_date:
        raise AperiodicDataError("end_date must be on or after start_date")
    windows = []
    cursor = start_date
    while cursor <= end_date:
        last = min(cursor + timedelta(days=RAW_MAX_RANGE_DAYS - 1), end_date)
        windows.append((cursor, last))
        cursor = last + timedelta(days=1)
    return windows


def raw_file_path(
    output_dir: str | Path,
    *,
    dataset: str,
    exchange: str,
    symbol: str,
    period: str,
) -> Path:
    """Where ``download_raw`` writes one file: the bucket's Hive layout.

    ``:`` is not allowed in Windows file names, so there it is written as
    ``%3A``, which Hive-partition readers decode back.
    """
    if _ESCAPE_COLON:
        symbol = symbol.replace(":", "%3A")
    year, month, day = _period_parts(period)
    path = (
        Path(output_dir)
        / dataset
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"year={year}"
        / f"month={month:02d}"
    )
    if day is not None:
        path = path / f"day={day:02d}"
    return path / "data.parquet"


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------


class _RawRequest:
    """One call's parameters, so an expired URL can be re-requested."""

    def __init__(
        self,
        *,
        api_key: str,
        dataset: RawDataset,
        exchange: str,
        symbol: str,
        base_url: str,
        preview: bool,
    ):
        self.api_key = api_key
        self.dataset = dataset
        self.exchange = exchange
        self.symbol = symbol
        self.base_url = base_url
        self.preview = preview

    async def files(self, start_date: date, end_date: date) -> list[RawFileInfo]:
        """Every file overlapping the range, deduplicated, in period order."""
        headers = get_headers(self.api_key)
        if self.preview:
            response = await fetch_json(
                f"{self.base_url}/data/raw/preview/{self.dataset}",
                params={"exchange": self.exchange, "symbol": self.symbol},
                headers=headers,
            )
            return sorted(response["files"], key=lambda f: f["period"])

        by_period: dict[str, RawFileInfo] = {}
        for window_start, window_end in _chunks(start_date, end_date):
            response = await fetch_json(
                f"{self.base_url}/data/raw/{self.dataset}",
                params={
                    "exchange": self.exchange,
                    "symbol": self.symbol,
                    "start_date": window_start.isoformat(),
                    "end_date": window_end.isoformat(),
                },
                headers=headers,
            )
            # A monthly file can overlap two windows; keep one copy.
            for file in response["files"]:
                by_period[file["period"]] = file
        return [by_period[p] for p in sorted(by_period)]

    async def refreshed(self, period: str) -> RawFileInfo:
        """A fresh presigned URL for one period (after a 403)."""
        start, end = _period_bounds(period)
        for file in await self.files(start, end):
            if file["period"] == period:
                return file
        raise AperiodicDataError(f"{period} is no longer listed by the API")


async def _with_refresh(request: _RawRequest, file: RawFileInfo, attempt):
    """Run ``attempt(file)``; on a 403, retry it once with a fresh URL."""
    try:
        return await attempt(file)
    except DownloadError as error:
        if error.status_code != 403:
            raise
    return await attempt(await request.refreshed(file["period"]))


def _http_client() -> Any:
    import httpx

    from ..config import DEFAULT_TIMEOUT

    return httpx.AsyncClient(timeout=httpx.Timeout(DEFAULT_TIMEOUT))


# ---------------------------------------------------------------------------
# get_raw
# ---------------------------------------------------------------------------


@overload
async def get_raw_async(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = ...,
    show_progress: bool = ...,
    max_concurrent: int = ...,
    output: Literal["polars"] = ...,
    preview: bool = ...,
) -> pl.DataFrame: ...


@overload
async def get_raw_async(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = ...,
    show_progress: bool = ...,
    max_concurrent: int = ...,
    output: Literal["pandas"] = ...,
    preview: bool = ...,
) -> pd.DataFrame: ...


async def get_raw_async(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
    output: OutputFormat = "polars",
    preview: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load raw data for one symbol and date range into a DataFrame.

    Every file overlapping the range is downloaded into memory, concatenated
    and trimmed to ``[start_date, end_date]`` on ``exchange_timestamp`` (UTC,
    inclusive). Timestamps are timezone-aware UTC. For ranges too large for
    memory, use ``download_raw``.

    Args:
        api_key: Your Aperiodic API key (Prime + Raw plan). Optional when
            preview=True.
        dataset: 'trades', 'quotes', 'mark_price', 'index_price',
            'funding_rate' or 'open_interest'. Hyperliquid serves trades and
            quotes only.
        exchange: 'binance-futures', 'okx-perps' or 'hyperliquid-perps'.
        symbol: Atlas symbol, e.g. 'perpetual-BTC-USDT:USDT'.
        start_date: First day (exchange-time UTC).
        end_date: Last day, inclusive. Ranges over 366 days are split into
            several requests.
        preview: Fetch the free preview file (June 2025, each venue's BTC
            perpetual) with the shared demo key instead.

    Returns:
        DataFrame with ``exchange_timestamp``, ``local_timestamp``,
        ``local_timestamp_kind`` and the dataset's columns.

    Raises:
        APIError: e.g. 403 with ``code="raw_not_in_plan"`` when the plan lacks
            raw data.
        DownloadError: If a file download fails after all retries.
    """
    api_key = _resolve_api_key(api_key, preview)
    backend = get_backend_module(output)
    request = _RawRequest(
        api_key=api_key,
        dataset=dataset,
        exchange=exchange,
        symbol=symbol,
        base_url=base_url,
        preview=preview,
    )

    files = await request.files(start_date, end_date)
    if not files:
        return backend.empty_dataframe()

    total_bytes = sum(file["size"] for file in files)
    if total_bytes > LARGE_RAW_BYTES:
        warnings.warn(
            f"This range is {total_bytes / 1024**3:.1f} GiB of Parquet, all "
            "loaded into memory. download_raw() streams the files to disk "
            "instead.",
            stacklevel=2,
        )

    semaphore = asyncio.Semaphore(max_concurrent)
    progress = tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        desc=f"{symbol} {dataset}",
        disable=not show_progress,
    )

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(_http_client()) if HAS_HTTPX else None

        async def fetch(file: RawFileInfo) -> bytes:
            year, month, day = _period_parts(file["period"])
            _, _, raw = await download_parquet_bytes(
                file["url"],
                {},
                year=year,
                month=month,
                day=day,
                semaphore=semaphore,
                client=client,
            )
            progress.update(file["size"])
            return raw

        try:
            payloads = await asyncio.gather(
                *(_with_refresh(request, file, fetch) for file in files)
            )
        finally:
            progress.close()

    frames = [backend.read_parquet(BytesIO(raw)) for raw in payloads]
    _require_uniform_columns(backend, frames, files)
    combined = backend.concat(frames)

    if backend.has_column(combined, EXCHANGE_TIMESTAMP):
        combined = backend.filter_datetime_range(
            combined,
            datetime.combine(start_date, time.min, tzinfo=UTC),
            datetime.combine(end_date, time.max, tzinfo=UTC),
            column=EXCHANGE_TIMESTAMP,
        )
    return combined


def _require_uniform_columns(backend, frames, files: list[RawFileInfo]) -> None:
    expected = backend.column_names(frames[0])
    for frame, file in zip(frames[1:], files[1:], strict=True):
        found = backend.column_names(frame)
        if found != expected:
            raise AperiodicDataError(
                f"Inconsistent columns across raw files: {file['period']} has "
                f"{found}, expected {expected}. This is a defect in the stored "
                "file, not in the query."
            )


@overload
def get_raw(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = ...,
    show_progress: bool = ...,
    max_concurrent: int = ...,
    output: Literal["polars"] = ...,
    preview: bool = ...,
) -> pl.DataFrame: ...


@overload
def get_raw(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = ...,
    show_progress: bool = ...,
    max_concurrent: int = ...,
    output: Literal["pandas"] = ...,
    preview: bool = ...,
) -> pd.DataFrame: ...


def get_raw(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = MAX_CONCURRENT_DOWNLOADS,
    output: OutputFormat = "polars",
    preview: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Load raw data into a DataFrame. See ``get_raw_async``."""
    return run_async(
        get_raw_async(
            api_key,
            dataset=dataset,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            base_url=base_url,
            show_progress=show_progress,
            max_concurrent=max_concurrent,
            output=output,  # type: ignore[arg-type]
            preview=preview,
        )
    )


# ---------------------------------------------------------------------------
# download_raw
# ---------------------------------------------------------------------------


async def _stream_to_file(
    client: Any,
    file: RawFileInfo,
    path: Path,
    *,
    semaphore: asyncio.Semaphore,
    progress: tqdm,
    max_retries: int = MAX_RETRIES,
    backoff_base: float = RETRY_BACKOFF_BASE,
) -> Path:
    """Stream one file to ``path`` via a temporary file, retrying transient errors."""
    import httpx

    year, month, day = _period_parts(file["period"])
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(path.name + ".part")

    async with semaphore:
        last_error: Exception | None = None
        for attempt in range(max_retries + 1):
            written = 0
            try:
                async with client.stream("GET", file["url"]) as response:
                    if response.status_code == 403:
                        raise DownloadError(
                            year,
                            month,
                            RuntimeError("403 Forbidden (URL expired or refused)"),
                            day=day,
                            status_code=403,
                        )
                    response.raise_for_status()
                    with partial.open("wb") as handle:
                        async for chunk in response.aiter_bytes():
                            handle.write(chunk)
                            written += len(chunk)
                            progress.update(len(chunk))
                partial.replace(path)
                return path
            except DownloadError:
                partial.unlink(missing_ok=True)
                raise
            except (httpx.HTTPError, OSError) as error:
                last_error = error
                progress.update(-written)
                partial.unlink(missing_ok=True)
                if attempt < max_retries:
                    await asyncio.sleep(backoff_base * (2**attempt))

    raise DownloadError(year, month, last_error or Exception("Unknown error"), day=day)


async def download_raw_async(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    output_dir: str | Path,
    overwrite: bool = False,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = DEFAULT_RAW_DOWNLOADS,
    preview: bool = False,
) -> list[Path]:
    """Stream raw files for a symbol and date range to disk.

    Files land in ``output_dir/{dataset}/exchange=…/symbol=…/year=…/month=…
    [/day=…]/data.parquet``, the bucket's own layout: one file per month of
    history, one per day from 2026-08-01. Monthly files are written whole, so
    they can reach past the range; filter on ``exchange_timestamp`` when
    reading. A file that already exists with the expected size is skipped
    unless ``overwrite=True``. Read the folder with a glob, e.g.
    ``pl.scan_parquet(f"{output_dir}/**/data.parquet")``.

    CPython only (needs httpx).

    Returns:
        The path of every file in the range, downloaded or already present,
        in period order.
    """
    if not HAS_HTTPX:
        raise AperiodicDataError(
            "download_raw needs httpx and a filesystem (CPython). In "
            "Pyodide/marimo use get_raw instead."
        )

    api_key = _resolve_api_key(api_key, preview)
    request = _RawRequest(
        api_key=api_key,
        dataset=dataset,
        exchange=exchange,
        symbol=symbol,
        base_url=base_url,
        preview=preview,
    )
    files = await request.files(start_date, end_date)

    planned = [
        (
            file,
            raw_file_path(
                output_dir,
                dataset=dataset,
                exchange=exchange,
                symbol=symbol,
                period=file["period"],
            ),
        )
        for file in files
    ]
    pending = [
        (file, path)
        for file, path in planned
        if overwrite or not (path.exists() and path.stat().st_size == file["size"])
    ]

    semaphore = asyncio.Semaphore(max_concurrent)
    progress = tqdm(
        total=sum(file["size"] for file, _ in pending),
        unit="B",
        unit_scale=True,
        desc=f"{symbol} {dataset}",
        disable=not show_progress,
    )

    try:
        async with _http_client() as client:
            paths = {file["period"]: path for file, path in pending}

            async def save(file: RawFileInfo) -> Path:
                return await _stream_to_file(
                    client,
                    file,
                    paths[file["period"]],
                    semaphore=semaphore,
                    progress=progress,
                )

            await asyncio.gather(
                *(_with_refresh(request, file, save) for file, _ in pending)
            )
    finally:
        progress.close()

    return [path for _, path in planned]


def download_raw(
    api_key: str | None = None,
    *,
    dataset: RawDataset,
    exchange: Exchange,
    symbol: str,
    start_date: date,
    end_date: date,
    output_dir: str | Path,
    overwrite: bool = False,
    base_url: str = DEFAULT_BASE_URL,
    show_progress: bool = True,
    max_concurrent: int = DEFAULT_RAW_DOWNLOADS,
    preview: bool = False,
) -> list[Path]:
    """Stream raw files to disk. See ``download_raw_async``."""
    return run_async(
        download_raw_async(
            api_key,
            dataset=dataset,
            exchange=exchange,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            output_dir=output_dir,
            overwrite=overwrite,
            base_url=base_url,
            show_progress=show_progress,
            max_concurrent=max_concurrent,
            preview=preview,
        )
    )


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------


async def get_raw_coverage_async(*, base_url: str = DEFAULT_BASE_URL) -> dict:
    """Raw datasets, their columns and per-symbol coverage (no key needed).

    Returns the ``/metadata/raw`` response: ``datasets``, ``summary`` (one row
    per dataset and venue) and ``coverage`` (dataset → exchange → symbol →
    first, last, days, missing, bytes).
    """
    headers = {
        key: value for key, value in get_headers("").items() if key != "X-API-KEY"
    }
    return await fetch_json(f"{base_url}/metadata/raw", params={}, headers=headers)


def get_raw_coverage(*, base_url: str = DEFAULT_BASE_URL) -> dict:
    """Raw datasets and coverage. See ``get_raw_coverage_async``."""
    return run_async(get_raw_coverage_async(base_url=base_url))
