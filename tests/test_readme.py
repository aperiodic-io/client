"""
Validates every Python code block in README.md.

Strategy:
- Extract all ```python blocks from README.md.
- Compile every block — catches syntax errors.
- Exec blocks that contain `api_key=` in a shared namespace pre-loaded with
  all aperiodic imports and stdlib helpers. Calls with `"your-api-key"` are
  expected to raise APIError(401); any other outcome fails the test.
"""

import asyncio
import re
from datetime import date
from pathlib import Path

import pytest

import aperiodic
from aperiodic import APIError
from aperiodic._compat import HAS_POLARS

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

README = Path(__file__).parent.parent / "README.md"


def _extract_python_blocks(path: Path) -> list[tuple[int, str]]:
    """Return (block_index, source) for every ```python block in path."""
    content = path.read_text()
    blocks = re.findall(r"```python\n(.*?)```", content, re.DOTALL)
    return list(enumerate(blocks, start=1))


def _exec_namespace() -> dict:
    """Shared execution namespace that mirrors README imports."""
    ns: dict = {"date": date, "asyncio": asyncio}
    # expose every public name from the aperiodic package
    for name in aperiodic.__all__:
        ns[name] = getattr(aperiodic, name)
    return ns


# ---------------------------------------------------------------------------
# Parametrized tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("index", "source"), _extract_python_blocks(README))
def test_readme_block_compiles(index, source):
    """Every Python block in README must be syntactically valid."""
    try:
        compile(source, f"README.md block {index}", "exec")
    except SyntaxError as exc:
        pytest.fail(f"README.md block {index} has a syntax error: {exc}")


@pytest.mark.skipif(
    not HAS_POLARS,
    reason="README examples use the default output='polars'; not applicable in pandas-only environments",
)
@pytest.mark.parametrize(
    ("index", "source"),
    [
        (i, s)
        for i, s in _extract_python_blocks(README)
        # skip preview blocks until the preview endpoint is deployed in production
        if "api_key=" in s and "preview=True" not in s
    ],
)
def test_readme_block_runs(index, source):
    """
    Blocks containing api_key= are executed. With the placeholder key
    'your-api-key' the API must reject the request with HTTP 401.
    Any other exception (e.g. ImportError, TypeError, KeyError) is a
    README bug and fails the test.
    """
    ns = _exec_namespace()
    try:
        exec(source, ns)
    except APIError as exc:
        if exc.status_code != 401:
            pytest.fail(f"README.md block {index}: expected 401, got {exc.status_code}")
    except Exception as exc:
        pytest.fail(f"README.md block {index} raised unexpected error: {exc!r}")
