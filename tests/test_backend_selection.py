from __future__ import annotations

import builtins
import importlib
import sys

import pytest


def test_compat_import_does_not_require_polars(monkeypatch):
    """Importing _compat should still work when polars import fails."""
    real_import = builtins.__import__

    def guarded_import(name: str, *args, **kwargs):
        if name == "polars":
            raise ImportError("simulated polars import failure")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    sys.modules.pop("aperiodic._compat", None)

    compat = importlib.import_module("aperiodic._compat")

    assert hasattr(compat, "get_backend_module")

    if importlib.util.find_spec("pyarrow") is not None:
        pandas_backend = compat.get_backend_module("pandas")
        assert pandas_backend.__name__.endswith("._pandas")


def test_polars_request_raises_on_cpython_without_polars(monkeypatch):
    """On CPython, `output='polars'` with polars missing still raises loudly —
    the install hint is the only useful feedback the user can act on."""
    real_import = builtins.__import__

    def guarded_import(name: str, *args, **kwargs):
        if name == "polars" or name.startswith("polars."):
            raise ImportError("simulated polars import failure")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(sys, "platform", "linux")
    sys.modules.pop("aperiodic._compat", None)
    compat = importlib.import_module("aperiodic._compat")

    with pytest.raises(ImportError, match="polars is not installed"):
        compat.get_backend_module("polars")


def test_polars_request_falls_back_to_pandas_in_pyodide(monkeypatch):
    """In Pyodide (`sys.platform == 'emscripten'`), polars can never be
    installed — its Rust bindings don't compile for wasm32. When a caller
    asks for polars output there, silently fall back to pandas so notebooks
    with the default `output='polars'` just work in the browser."""
    real_import = builtins.__import__

    def guarded_import(name: str, *args, **kwargs):
        if name == "polars" or name.startswith("polars."):
            raise ImportError("simulated polars import failure")
        return real_import(name, *args, **kwargs)

    if importlib.util.find_spec("pyarrow") is None:
        pytest.skip("pyarrow is required for the pandas fallback")

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    monkeypatch.setattr(sys, "platform", "emscripten")
    sys.modules.pop("aperiodic._compat", None)
    compat = importlib.import_module("aperiodic._compat")

    backend = compat.get_backend_module("polars")
    assert backend.__name__.endswith("._pandas")
