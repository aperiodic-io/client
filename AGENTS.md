# Agent Instructions

Guidelines for automated contributors (Claude Code, Codex, Copilot, etc.)
working in `aperiodic-io/client`.

## Optional-dependency / feature detection

**Always use `importlib.util.find_spec` to check whether an optional
package is installed. Do not use `try: import X / except ImportError`
for this purpose.**

`find_spec` is the project-wide convention — see the `HAS_POLARS` /
`HAS_PYARROW` detection in `src/aperiodic/_compat.py` and the
`_HAS_PYODIDE_FFI` probe in `src/aperiodic/_backends/_pyfetch_transport.py`.

Why we prefer it over `try/except ImportError`:

- It does not execute the module as a side-effect of the probe — heavy
  optional deps (polars, pyarrow) or runtime-special modules
  (`pyodide.ffi`) stay un-imported until we actually need them.
- It cannot accidentally swallow a real `ImportError` originating from
  inside a downstream module. `try: import X / except ImportError` hides
  *any* `ImportError` raised during `X`'s initialisation, not just the
  top-level "X is not installed" case.
- It produces a boolean flag at module-import time that is cheap to
  re-check at call sites, which is easier to reason about than branching
  on exception flow.

### Pattern

```python
from importlib.util import find_spec

_HAS_FOO = find_spec("foo") is not None

def use_foo():
    if not _HAS_FOO:
        raise RuntimeError("foo is required. Install with: pip install foo")
    import foo                # deferred — only hit when the feature is used
    return foo.do_something()
```

For sub-modules, pass the dotted path: `find_spec("foo.bar")`.

### When `try/except ImportError` *is* acceptable

Only when the import itself has meaningful side-effects we want to
observe — e.g. a plugin that deliberately raises `ImportError` from
inside `__init__.py` to signal an incompatible environment. These cases
should be explicitly called out in a comment.
