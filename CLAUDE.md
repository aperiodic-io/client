# CLAUDE.md - Aperiodic Python Client Skill

## Core Project Context
- **Name:** Aperiodic Python Client
- **Description:** Client library for Aperiodic.io - institutional-grade market microstructure, liquidity and order flow metrics.
- **Key Concepts:** Retrieves pre-computed derivative and microstructure metrics with parallel downloads. Uses `httpx` for networking and parses data into `polars` DataFrames (optional pandas support).
- **Symbology:** Atlas unified symbology string formats (e.g., `perpetual-BTC-USDT:USDT`).

## Tech Stack & Tooling
- **Language:** Python 3.11+
- **Build System:** Hatch (`hatchling.build`)
- **Linting & Formatting:** Ruff
- **Testing:** Pytest (with pytest-asyncio and pytest-cov, respx)
- **Dependencies:** `polars`, `httpx`, `tqdm`, `nest-asyncio`

## Development Commands

### Environment Setup
- Install from source with all extras: `pip install -e ".[tests,quality,polars,pandas]"`

### Code Formatting & Linting
- **Check (Lint):** `hatch run quality:check` (or `ruff check .`)
- **Format:** `hatch run quality:format` (or `ruff format .`)
- Notable Ruff config: `E501` (line lengths), `PLR2004` (magic value), and `PLR0913` (too many arguments) are ignored.

### Testing
- **Run all tests:** `hatch run test:run` (or `pytest tests/ --durations 0 -s`)

### Version Management
- Handled by `bumpver` via config in `pyproject.toml` (`bumpver update --patch` or similar).

## Key Architecture & Design Notes
- **Sync/Async Parity**: Endpoints have both sync and async variations (e.g., `get_metrics` and `get_metrics_async`).
- **Concurrent Processing**: Downloads are split into monthly parquet files server-side, fetched concurrently locally via `httpx`, and concatenated. Tunable via `max_concurrent` parameter (default 10).
- **WASM Support**: The client is expected to support Emscripten/Pyodide environments. Ensure `httpx` logic falls back safely or isn't unnecessarily blocked in browsers (uses `pyfetch` natively internally or expects no network requests directly via sockets).
- **Dependencies**: Aim to keep dependencies minimal. `polars` and `pandas` are mostly handled as optional dependencies `aperiodic[polars]` and `aperiodic[pandas]`.

## Best Practices
- Maintain the Atlas Symbology expectation for symbols.
- Respect `show_progress` using `tqdm` for long-running functions.
- Keep output neatly filtered and sorted to the exact requested `start_date` and `end_date` date range.
