from __future__ import annotations

import builtins
import importlib
import importlib.util
import sys


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
