"""Signing in without copying keys around.

``aperiodic.login()`` (or ``python -m aperiodic login``) gets this machine a
key of its own through aperiodic.io/device, on a laptop, over SSH or in a
remote notebook alike, and saves it where the Aperiodic CLI saves its key.
Every call that needs a key then finds it on its own: an ``api_key`` passed
in, else ``APERIODIC_API_KEY``, else the saved key.

Only the standard library is used, so this works on a bare install.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import webbrowser
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .client import AperiodicDataError
from .config import DEFAULT_BASE_URL

KEYS_URL = "https://aperiodic.io/home/api"


def credentials_path() -> Path:
    """Where the key is saved; the same place as the Aperiodic CLI.

    ``$APERIODIC_CONFIG_DIR/credentials``, else
    ``$XDG_CONFIG_HOME/aperiodic/credentials``, else
    ``%APPDATA%\\aperiodic\\credentials`` on Windows and
    ``~/.config/aperiodic/credentials`` elsewhere, macOS included.
    """
    if config_dir := os.environ.get("APERIODIC_CONFIG_DIR"):
        return Path(config_dir) / "credentials"
    if xdg := os.environ.get("XDG_CONFIG_HOME"):
        return Path(xdg) / "aperiodic" / "credentials"
    if sys.platform == "win32" and (appdata := os.environ.get("APPDATA")):
        return Path(appdata) / "aperiodic" / "credentials"
    return Path.home() / ".config" / "aperiodic" / "credentials"


def _load_credentials() -> dict[str, Any] | None:
    try:
        data = json.loads(credentials_path().read_text())
    except (OSError, ValueError, RuntimeError):
        # No file, unreadable, or no home directory (Pyodide): no saved key.
        return None
    return data if isinstance(data, dict) and data.get("api_key") else None


def default_api_key() -> str | None:
    """The key calls use when none is passed: ``APERIODIC_API_KEY``, else the saved one."""
    if key := os.environ.get("APERIODIC_API_KEY"):
        return key
    credentials = _load_credentials()
    return str(credentials["api_key"]) if credentials else None


def _save_credentials(credentials: dict[str, Any]) -> Path:
    path = credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name + ".tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(credentials, handle, indent=2)
        handle.write("\n")
    os.replace(tmp, path)
    return path


def _request(
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    api_key: str | None = None,
) -> tuple[int, dict[str, Any]]:
    """One JSON call to /api/v1; answers with 4xx carry a body worth reading."""
    if sys.platform == "emscripten":
        raise AperiodicDataError(
            "Signing in needs a regular Python, not Pyodide/WASM. Pass api_key= instead."
        )
    base = os.environ.get("APERIODIC_API_URL") or DEFAULT_BASE_URL
    headers = {"Content-Type": "application/json", "User-Agent": "aperiodic-python"}
    if api_key:
        headers["X-API-KEY"] = api_key
    cf_id = os.environ.get("CF_ACCESS_CLIENT_ID")
    cf_secret = os.environ.get("CF_ACCESS_CLIENT_SECRET")
    if cf_id and cf_secret:
        headers["CF-Access-Client-Id"] = cf_id
        headers["CF-Access-Client-Secret"] = cf_secret
    request = urllib.request.Request(
        base.rstrip("/") + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as error:
        try:
            return error.code, json.loads(error.read() or b"{}")
        except ValueError:
            return error.code, {}


def _in_notebook() -> bool:
    return "ipykernel" in sys.modules


def _can_open_browser() -> bool:
    if os.environ.get("SSH_CONNECTION") or os.environ.get("SSH_TTY"):
        return False
    if _in_notebook():
        # The kernel may run on another machine than the browser showing it.
        return False
    if sys.platform.startswith("linux"):
        return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))
    return True


@dataclass(frozen=True)
class LoginResult:
    """What ``login()`` saved. The key itself is not repeated here, so it never lands in a notebook's output."""

    key_name: str
    plan_name: str
    path: Path


def login(
    name: str | None = None,
    *,
    open_browser: bool | None = None,
    sleep=time.sleep,
) -> LoginResult:
    """Sign in and save a key for this machine.

    Prints a link and a code: open the link on any device, check the code
    matches, and allow it. A free account works, and covers every metric for
    2025. The key is named after this machine (``name`` overrides it) and can
    be revoked at aperiodic.io/home/api.

    Args:
        name: Name for this machine's key. Defaults to the host name.
        open_browser: Open the link in a browser here. Defaults to yes on a
            desktop, no over SSH and in notebooks.
    """
    import platform

    key_name = name or f"{platform.node() or 'this computer'} (aperiodic Python)"
    status, start = _request("POST", "/cli/login", body={"name": key_name})
    if status != 200 or "device_code" not in start:
        raise AperiodicDataError(
            f"Could not start the login: {start.get('error') or status}"
        )

    link = start["verification_uri_complete"]
    if open_browser if open_browser is not None else _can_open_browser():
        webbrowser.open(link)
    print(f"To sign in, open {link}")
    print(f"and check it shows the code {start['user_code']}. Waiting…", flush=True)

    interval = max(int(start.get("interval", 5)), 1)
    deadline = time.monotonic() + max(int(start.get("expires_in", 600)), 60)
    while time.monotonic() < deadline:
        sleep(interval)
        try:
            status, answer = _request(
                "POST", "/cli/token", body={"device_code": start["device_code"]}
            )
        except OSError:
            continue  # A dropped connection is not an answer: try again.
        error = answer.get("error")
        if status == 200 and answer.get("api_key"):
            path = _save_credentials(
                {
                    "api_key": answer["api_key"],
                    "key_name": answer.get("key_name", key_name),
                    "plan": answer.get("plan", ""),
                    "plan_name": answer.get("plan_name", ""),
                    "created_at": datetime.now(UTC).isoformat(),
                }
            )
            result = LoginResult(
                key_name=answer.get("key_name", key_name),
                plan_name=answer.get("plan_name", ""),
                path=path,
            )
            print(
                f"Signed in on {result.plan_name}. This machine's key is saved in {path}."
            )
            if os.environ.get("APERIODIC_API_KEY"):
                print(
                    "Note: APERIODIC_API_KEY is set, and it is used before the saved key."
                )
            return result
        if error == "authorization_pending":
            continue
        if error == "slow_down":
            interval += 5
            continue
        if error == "access_denied":
            raise AperiodicDataError("The login was denied in the browser.")
        if error == "expired_token":
            break
        raise AperiodicDataError(
            answer.get("error_description") or f"Login failed: {error or status}"
        )
    raise AperiodicDataError("The code expired. Run aperiodic.login() again.")


def logout() -> bool:
    """Revoke this machine's saved key and remove it. False when there was none."""
    credentials = _load_credentials()
    if not credentials:
        return False
    revoked = False
    try:
        status, answer = _request("DELETE", "/cli/key", api_key=credentials["api_key"])
        revoked = status == 200 and bool(answer.get("revoked"))
    except OSError:
        pass
    credentials_path().unlink(missing_ok=True)
    if not revoked:
        print(f"Removed the key from this machine. Revoke it at {KEYS_URL} too.")
    return True


def whoami(api_key: str | None = None) -> dict[str, Any]:
    """Which key is in use and what its plan covers."""
    key = api_key or default_api_key()
    if not key:
        raise AperiodicDataError(
            "No API key. Run aperiodic.login(), set APERIODIC_API_KEY, or pass api_key=."
        )
    status, answer = _request("GET", "/cli/whoami", api_key=key)
    if status != 200:
        raise AperiodicDataError(
            answer.get("error") or f"whoami failed with HTTP {status}"
        )
    return answer
