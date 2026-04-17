#!/usr/bin/env node
/* eslint-disable */
/*
 * Pyodide smoke test — end-to-end verification that the aperiodic sync API
 * works inside Pyodide/WASM without needing a PyPI release.
 *
 * What this catches (and every previous PR in the run_sync / polars-fallback
 * chain shipped something that would have failed this test):
 *
 *   1. PyodideTask leak — if `run_async()` ever goes back to
 *      `loop.run_until_complete()` instead of `pyodide.ffi.run_sync()`,
 *      `get_ohlcv()` returns a Task instead of a DataFrame. Check #1 asserts
 *      `isinstance(df, pd.DataFrame)`.
 *
 *   2. polars → pandas fallback — the default `output="polars"` call path
 *      must silently use pandas when polars isn't installed (always the case
 *      in Pyodide: polars has Rust bindings that can't build for wasm32).
 *      Check #2 asserts the no-output call returns a real pandas DataFrame.
 *
 *   3. sys.platform === "emscripten" in Pyodide — anchor for fallback logic
 *      (and documentation). Check #3 asserts the sentinel.
 *
 * Runs in a few minutes on GitHub-hosted runners. No API key, no network
 * traffic against aperiodic.io (we monkey-patch the transport inside
 * Pyodide so the sync path completes without hitting the real API).
 *
 * Requires Node 22+ with `--experimental-wasm-jspi` — `pyodide.ffi.run_sync`
 * (Pyodide ≥ 0.26) needs WebAssembly JavaScript Promise Integration. In
 * real browsers JSPI is default-on from Chrome 123 / Safari 18.2, so users
 * don't need the flag — only our CI runner does.
 */

const { loadPyodide } = require("pyodide");
const fs = require("fs");
const path = require("path");

async function main() {
  console.log("→ booting Pyodide");
  // Allow PYODIDE_INDEX_URL for offline/local runs (e.g. sandbox with
  // jsdelivr blocked); default to the npm-bundled jsdelivr path in CI.
  const opts = process.env.PYODIDE_INDEX_URL
    ? { indexURL: process.env.PYODIDE_INDEX_URL }
    : {};
  const py = await loadPyodide(opts);
  console.log("  pyodide:", py.version);

  console.log("→ loading base packages (micropip, pandas, pyarrow)");
  await py.loadPackage(["micropip", "pandas", "pyarrow"]);

  console.log("→ locating wheel built by the preceding `hatch build` step");
  const distDir = path.resolve(__dirname, "..", "..", "dist");
  const wheels = fs.existsSync(distDir)
    ? fs.readdirSync(distDir).filter((f) => f.endsWith(".whl"))
    : [];
  if (wheels.length !== 1) {
    console.error(
      `  expected exactly one wheel in ${distDir}/, found ${wheels.length}: ${wheels}`,
    );
    process.exit(1);
  }
  const wheel = wheels[0];
  console.log(`  wheel: ${wheel}`);

  console.log("→ installing aperiodic + deps inside Pyodide");
  py.FS.writeFile(`/tmp/${wheel}`, fs.readFileSync(path.join(distDir, wheel)));
  await py.runPythonAsync(`
import micropip
# tqdm is a hard aperiodic dep, not in the default Pyodide dist.
await micropip.install("tqdm")
# Install our freshly-built wheel on top; deps=False because we already
# loaded pandas/pyarrow/tqdm above and the wheel itself only needs those.
await micropip.install("emfs:/tmp/${wheel}", deps=False)
`);

  console.log("→ running smoke-test cell");
  const result = await py.runPythonAsync(`
import sys
import aperiodic.endpoints.utils as _u
import pandas as pd
from datetime import date

# Monkey-patch the pyfetch transport so the sync path completes without
# hitting aperiodic.io — we don't want CI to need an API key and we're
# testing the sync/backend layer, not the data pipeline.
async def _fake_fetch_json(*a, **kw):
    return {"files": []}

async def _fake_download(*a, **kw):
    return (0, 0, b"")

_u.fetch_json = _fake_fetch_json
_u.download_parquet_bytes = _fake_download

from aperiodic import get_ohlcv

# --- Check #1: sync wrapper returns real DataFrame, not PyodideTask ---
df = get_ohlcv(
    api_key="fake",
    timestamp="exchange",
    interval="5m",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2025, 5, 1),
    end_date=date(2025, 5, 1),
    output="pandas",
    show_progress=False,
)

# --- Check #2: default output="polars" silently falls back to pandas ---
df_default = get_ohlcv(
    api_key="fake",
    timestamp="exchange",
    interval="5m",
    exchange="binance-futures",
    symbol="perpetual-BTC-USDT:USDT",
    start_date=date(2025, 5, 1),
    end_date=date(2025, 5, 1),
    show_progress=False,
)

{
    "platform": sys.platform,
    "explicit_pandas_type": type(df).__name__,
    "explicit_pandas_is_dataframe": isinstance(df, pd.DataFrame),
    "default_output_type": type(df_default).__name__,
    "default_output_is_dataframe": isinstance(df_default, pd.DataFrame),
}
`);

  const r = result.toJs({ dict_converter: Object.fromEntries });
  console.log("  result:", r);

  const failures = [];
  if (r.platform !== "emscripten") {
    failures.push(`sys.platform was ${JSON.stringify(r.platform)}, expected "emscripten"`);
  }
  if (!r.explicit_pandas_is_dataframe) {
    failures.push(
      `explicit output="pandas" returned ${r.explicit_pandas_type}, not pandas.DataFrame`,
    );
  }
  if (!r.default_output_is_dataframe) {
    failures.push(
      `default output (implicit "polars") returned ${r.default_output_type}, ` +
        `expected pandas.DataFrame via the Pyodide fallback`,
    );
  }

  if (failures.length > 0) {
    console.error("\nFAIL:");
    for (const f of failures) console.error("  - " + f);
    process.exit(1);
  }
  console.log("\nALL CHECKS PASSED ✓");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
