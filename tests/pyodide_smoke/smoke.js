#!/usr/bin/env node
/* eslint-disable */
/*
 * Pyodide smoke test — end-to-end verification that the aperiodic sync API
 * works inside Pyodide/WASM without needing a PyPI release.
 *
 * Core regression guard: if `run_async` ever goes back to
 * `loop.run_until_complete` (or anything else that can't actually block
 * Pyodide's browser-owned event loop) instead of `pyodide.ffi.run_sync`,
 * the sync wrappers leak a pending Task and callers get an opaque
 * `PyodideTask`/"await wasn't used with future" error. This test catches
 * that by calling `get_ohlcv(..., output="pandas")` inside real Pyodide
 * and asserting the result is a real `pandas.DataFrame`.
 *
 * Network is monkey-patched inside Pyodide so CI needs neither
 * APERIODIC_API_KEY nor egress to aperiodic.io — we're testing the
 * sync/backend layer, not the data pipeline.
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

# --- Check #1: sync wrapper returns a real DataFrame, not a PyodideTask ---
# Main regression guard. Before the pyodide.ffi.run_sync migration
# (client 4.0.8), the sync wrappers leaked a pending Task because
# loop.run_until_complete cannot actually block Pyodide's browser-owned
# event loop. If that ever gets reverted, the isinstance check fails.
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

{
    "platform": sys.platform,
    "explicit_pandas_type": type(df).__name__,
    "explicit_pandas_is_dataframe": isinstance(df, pd.DataFrame),
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
      `get_ohlcv(output="pandas") returned ${r.explicit_pandas_type}, not pandas.DataFrame`,
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
