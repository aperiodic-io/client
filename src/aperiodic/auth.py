"""Signing in without copying keys around.

``aperiodic.login()`` (or ``python -m aperiodic login``) gets this machine a
key of its own through aperiodic.io/device, on a laptop, over SSH or in a
remote notebook alike, and saves it where the Aperiodic CLI saves its key.
Every call that needs a key then finds it on its own: an ``api_key`` passed
in, else ``APERIODIC_API_KEY``, else, for ``preview=True``, the shared demo
key, else the saved key. A saved key never takes over a preview request, so a
revoked one cannot break the free sample.

Only the standard library is used, so this works on a bare install.
"""

from __future__ import annotations

import contextlib
import http.client
import json
import os
import sys
import tempfile
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


def _saved_api_key() -> str | None:
    credentials = _load_credentials()
    return str(credentials["api_key"]) if credentials else None


def rejected_saved_key_message(api_key: str | None) -> str | None:
    """The way forward when the server rejects ``api_key`` and it came from the credentials file.

    None for a key passed in or set in ``APERIODIC_API_KEY``: those keep the
    server's own error, since logging in again would not change them.
    """
    if not api_key or os.environ.get("APERIODIC_API_KEY") == api_key:
        return None
    if _saved_api_key() != api_key:
        return None
    return (
        f"The key saved in {credentials_path()} was rejected (revoked?). "
        "Run aperiodic.login() again."
    )


def explain_rejected_key(error: Exception, api_key: str | None) -> Exception:
    """A transport's 401 ``APIError`` for the saved key, reworded as the way forward."""
    if getattr(error, "status_code", None) != 401:
        return error
    message = rejected_saved_key_message(api_key)
    if not message:
        return error
    return type(error)(
        message,
        401,
        details=getattr(error, "details", None),
        code=getattr(error, "code", None),
    )


def _save_credentials(credentials: dict[str, Any]) -> Path:
    path = credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    # mkstemp makes a fresh 0600 file, so two logins at once never share it.
    fd, tmp = tempfile.mkstemp(prefix="credentials-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(credentials, handle, indent=2)
            handle.write("\n")
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return path


class _NetworkError(AperiodicDataError):
    """The request never got an answer: offline, DNS, a reset. Retried while polling."""


def _json_body(raw: bytes) -> dict[str, Any]:
    """A response body as a dict; an HTML error page or captive portal reads as empty."""
    try:
        body = json.loads(raw or b"{}")
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


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
            return response.status, _json_body(response.read())
    except urllib.error.HTTPError as error:
        try:
            return error.code, _json_body(error.read())
        except (OSError, http.client.HTTPException):
            return error.code, {}
    except (urllib.error.URLError, OSError, http.client.HTTPException) as error:
        reason = getattr(error, "reason", None) or error
        raise _NetworkError(f"Could not reach {base}: {reason}") from None


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


def _still_signed_in(saved: dict[str, Any]) -> LoginResult | None:
    """The saved key's login when the server still accepts it, else None."""
    try:
        status, answer = _request("GET", "/cli/whoami", api_key=str(saved["api_key"]))
    except _NetworkError:
        return None  # Cannot tell; logging in again is the way forward.
    if status != 200:
        return None
    key = answer.get("key") or {}
    return LoginResult(
        key_name=key.get("name") or saved.get("key_name") or "this machine's key",
        plan_name=answer.get("plan_name") or saved.get("plan_name") or "",
        path=credentials_path(),
    )


def _revoke_quietly(api_key: str) -> None:
    """Best effort: a replaced key is revoked so it does not count against the account's keys."""
    with contextlib.suppress(AperiodicDataError):
        _request("DELETE", "/cli/key", api_key=api_key)


def login(
    name: str | None = None,
    *,
    open_browser: bool | None = None,
    force: bool = False,
) -> LoginResult:
    """Sign in and save a key for this machine.

    Prints a link and a code: open the link on any device, check the code
    matches, and allow it. A free account works, and covers every metric for
    2025 on BTC, ETH, SOL and 10 RWA perps. The key is named after this
    machine (``name`` overrides it) and can be revoked at aperiodic.io/home/api.

    When this machine is already signed in with a key the server accepts, it
    says so and returns that login without making another key. ``force=True``
    makes a new key and revokes the one it replaces.

    Args:
        name: Name for this machine's key. Defaults to the host name.
        open_browser: Open the link in a browser here. Defaults to yes on a
            desktop, no over SSH and in notebooks.
        force: Replace a saved key that still works.
    """
    return _login(
        name,
        open_browser=open_browser,
        force=force,
        force_hint="Call aperiodic.login(force=True) to replace it.",
    )


def _login(
    name: str | None,
    *,
    open_browser: bool | None,
    force: bool,
    force_hint: str,
) -> LoginResult:
    import platform

    saved = _load_credentials()
    if saved and not force:
        current = _still_signed_in(saved)
        if current:
            print(
                f'Already signed in as "{current.key_name}" on {current.plan_name}. '
                f"{force_hint}"
            )
            return current

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

    answer = _poll_for_key(start)
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
    if saved and saved.get("api_key") != answer["api_key"]:
        _revoke_quietly(str(saved["api_key"]))
    print(f"Signed in on {result.plan_name}. This machine's key is saved in {path}.")
    if os.environ.get("APERIODIC_API_KEY"):
        print("Note: APERIODIC_API_KEY is set, and it is used before the saved key.")
    return result


def _poll_for_key(start: dict[str, Any]) -> dict[str, Any]:
    """Poll /cli/token as RFC 8628 says until the login is allowed; the answer carries the key."""
    interval = max(int(start.get("interval", 5)), 1)
    deadline = time.monotonic() + max(int(start.get("expires_in", 600)), 60)
    while time.monotonic() < deadline:
        time.sleep(interval)
        try:
            status, answer = _request(
                "POST", "/cli/token", body={"device_code": start["device_code"]}
            )
        except _NetworkError:
            continue  # A dropped connection is not an answer: try again.
        if status == 429 or status >= 500:
            continue  # A rate limit or a proxy's error page is not an answer either.
        if status == 200 and answer.get("api_key"):
            return answer
        error = answer.get("error")
        if error == "slow_down":
            interval += 5
        elif error == "access_denied":
            raise AperiodicDataError("The login was denied in the browser.")
        elif error == "expired_token":
            break
        elif error != "authorization_pending":
            raise AperiodicDataError(
                answer.get("error_description") or f"Login failed: {error or status}"
            )
    raise AperiodicDataError("The code expired. Run aperiodic.login() again.")


@dataclass(frozen=True)
class LogoutResult:
    """What ``logout()`` did: whether the saved key was removed and revoked, in one sentence."""

    removed: bool
    revoked: bool
    message: str


def logout() -> LogoutResult:
    """Revoke this machine's saved key and remove it from this machine."""
    credentials = _load_credentials()
    if not credentials:
        return LogoutResult(
            removed=False,
            revoked=False,
            message=f"Not signed in ({credentials_path()}).",
        )
    try:
        status, answer = _request("DELETE", "/cli/key", api_key=credentials["api_key"])
    except _NetworkError as error:
        status, answer = 0, {"error_description": str(error)}
    credentials_path().unlink(missing_ok=True)

    revoked = status == 200 and bool(answer.get("revoked"))
    if revoked:
        message = "Signed out: this machine's key is revoked and removed."
    elif status in (200, 401):
        message = "Removed the key from this machine. It was already revoked."
    elif answer.get("error") == "not_a_named_key":
        message = (
            "Removed the key from this machine. It is the account's default key, "
            f"which is not revoked from here: rotate it at {KEYS_URL}."
        )
    else:
        reason = answer.get("error_description") or answer.get("error") or status
        message = (
            f"Removed the key from this machine, but could not revoke it ({reason}). "
            f"Revoke it at {KEYS_URL}."
        )
    return LogoutResult(removed=True, revoked=revoked, message=message)


def whoami(api_key: str | None = None) -> dict[str, Any]:
    """Which key is in use and what its plan covers."""
    key = api_key or default_api_key()
    if not key:
        raise AperiodicDataError(
            "No API key. Run aperiodic.login(), set APERIODIC_API_KEY, or pass api_key=."
        )
    status, answer = _request("GET", "/cli/whoami", api_key=key)
    if status != 200:
        if status == 401 and (message := rejected_saved_key_message(key)):
            raise AperiodicDataError(message)
        raise AperiodicDataError(
            answer.get("error_description")
            or answer.get("error")
            or f"whoami failed with HTTP {status}"
        )
    return answer
