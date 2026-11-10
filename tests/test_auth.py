"""Signing in: aperiodic.login(), the saved key, and how calls find a key."""

from __future__ import annotations

import asyncio
import json
import socket
import stat
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import aperiodic
from aperiodic import AperiodicDataError, APIError
from aperiodic import auth as auth_module
from aperiodic.__main__ import main
from aperiodic.auth import (
    _request,
    credentials_path,
    default_api_key,
    login,
    logout,
    whoami,
)
from aperiodic.config import DEMO_API_KEY
from aperiodic.endpoints.symbols import get_symbols, get_symbols_async
from aperiodic.endpoints.utils import _resolve_api_key

NAMED_KEY = "apk_" + "k" * 43
OLD_KEY = "apk_" + "o" * 43
DEAD_KEY = "apk_" + "d" * 43


class FakeAperiodic:
    """/api/v1/cli/* as aperiodic.io answers it: pending for a few polls, then the key."""

    def __init__(self, pending_polls: int = 1, final_error: str | None = None):
        self.pending_polls = pending_polls
        self.final_error = final_error
        self.polls = 0
        self.login_names: list[str] = []
        self.revoked: list[str] = []
        # Keys the server accepts, with the name whoami reports for each.
        self.accepted = {NAMED_KEY: "research-vm", OLD_KEY: "old-vm"}
        # Raw answers to the first polls, before the usual pending/key ones.
        self.poll_script: list[tuple[int, bytes]] = []
        # A non-default answer to DELETE /cli/key, by key.
        self.delete_replies: dict[str, tuple[int, dict]] = {}
        self.symbol_keys: list[str] = []
        # A non-default answer to GET /cli/whoami, by key.
        self.whoami_replies: dict[str, tuple[int, dict]] = {}

    def handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status: int, body: dict | bytes):
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                self.send_header(
                    "Content-Type",
                    "text/html" if isinstance(body, bytes) else "application/json",
                )
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _body(self) -> dict:
                length = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(length) or b"{}")

            def do_POST(self):
                body = self._body()
                if self.path == "/api/v1/cli/login":
                    fake.login_names.append(body.get("name", ""))
                    return self._reply(
                        200,
                        {
                            "device_code": "secret-device-code",
                            "user_code": "BCDF-GHJK",
                            "verification_uri": "https://aperiodic.io/device",
                            "verification_uri_complete": "https://aperiodic.io/device?code=BCDF-GHJK",
                            "expires_in": 600,
                            "interval": 5,
                        },
                    )
                if self.path == "/api/v1/cli/token":
                    assert body["device_code"] == "secret-device-code"
                    fake.polls += 1
                    if fake.poll_script:
                        status, raw = fake.poll_script.pop(0)
                        return self._reply(status, raw)
                    if fake.polls <= fake.pending_polls:
                        return self._reply(400, {"error": "authorization_pending"})
                    if fake.final_error:
                        return self._reply(400, {"error": fake.final_error})
                    return self._reply(
                        200,
                        {
                            "api_key": NAMED_KEY,
                            "key_name": "research-vm (aperiodic Python)",
                            "plan": "loggedInNoSubscription",
                            "plan_name": "Free",
                        },
                    )
                return self._reply(404, {})

            def do_GET(self):
                key = self.headers.get("X-API-KEY") or ""
                if self.path.startswith("/api/v1/metadata/symbols"):
                    return self._symbols(key)
                if self.path == "/api/v1/html":
                    return self._reply(200, b"<html>Just a moment...</html>")
                if self.path == "/api/v1/cli/whoami":
                    return self._whoami(key)
                return self._reply(404, {})

            def _symbols(self, key: str):
                fake.symbol_keys.append(key)
                if key not in fake.accepted:
                    return self._reply(401, {"error": "Access denied"})
                return self._reply(200, {"symbols": ["perpetual-BTC-USDT:USDT"]})

            def _whoami(self, key: str):
                if key in fake.whoami_replies:
                    return self._reply(*fake.whoami_replies[key])
                if key not in fake.accepted:
                    return self._reply(401, {"error": "Access denied"})
                return self._reply(
                    200,
                    {
                        "plan": "loggedInNoSubscription",
                        "plan_name": "Free",
                        "key": {
                            "kind": "named",
                            "name": fake.accepted[key],
                            "hint": key[-4:],
                        },
                        "covers": {
                            "datasets_in_full": 19,
                            "datasets_total": 19,
                            "markets": "BTC, ETH, SOL and 10 RWA perps",
                        },
                    },
                )

            def do_DELETE(self):
                key = self.headers.get("X-API-KEY") or ""
                fake.revoked.append(key)
                if key in fake.delete_replies:
                    return self._reply(*fake.delete_replies[key])
                if key not in fake.accepted:
                    return self._reply(401, {"error": "Access denied"})
                return self._reply(200, {"revoked": True})

        return Handler


@pytest.fixture
def server(monkeypatch, tmp_path):
    def start(fake: FakeAperiodic):
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), fake.handler())
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        monkeypatch.setenv(
            "APERIODIC_API_URL", f"http://127.0.0.1:{httpd.server_port}/api/v1"
        )
        return httpd

    monkeypatch.setenv("APERIODIC_CONFIG_DIR", str(tmp_path))
    monkeypatch.delenv("APERIODIC_API_KEY", raising=False)
    started = []
    yield lambda fake: started.append(start(fake)) or started[-1]
    for httpd in started:
        httpd.shutdown()


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Polls wait for real in use; here they are instant, and their waits recorded."""
    waits: list[float] = []
    monkeypatch.setattr(auth_module.time, "sleep", waits.append)
    return waits


def save_key(key: str) -> None:
    credentials_path().parent.mkdir(parents=True, exist_ok=True)
    credentials_path().write_text(json.dumps({"api_key": key}))


def test_login_saves_a_key_only_this_user_can_read(server, capsys):
    fake = FakeAperiodic(pending_polls=2)
    server(fake)

    result = login(open_browser=False)

    assert result.plan_name == "Free"
    assert result.path == credentials_path()
    assert fake.polls == 3
    assert fake.login_names[0].endswith("(aperiodic Python)")
    out = capsys.readouterr().out
    assert "BCDF-GHJK" in out
    assert "https://aperiodic.io/device?code=BCDF-GHJK" in out
    # The key itself is never printed or returned, so it never lands in a notebook.
    assert NAMED_KEY not in out
    assert NAMED_KEY not in repr(result)

    saved = json.loads(credentials_path().read_text())
    assert saved["api_key"] == NAMED_KEY
    assert stat.S_IMODE(credentials_path().stat().st_mode) == 0o600


def test_login_stops_when_denied_and_saves_nothing(server):
    server(FakeAperiodic(pending_polls=0, final_error="access_denied"))

    with pytest.raises(AperiodicDataError, match="denied"):
        login(open_browser=False)
    assert not credentials_path().exists()


def test_login_backs_off_when_told_to_slow_down(server, monkeypatch):
    fake = FakeAperiodic(pending_polls=0, final_error="slow_down")
    server(fake)
    waits: list[float] = []

    def record(seconds):
        waits.append(seconds)
        if len(waits) == 2:
            fake.final_error = None

    monkeypatch.setattr(auth_module.time, "sleep", record)
    login(open_browser=False)
    assert waits[1] > waits[0]


def test_login_keeps_polling_through_a_proxy_error_page_and_a_rate_limit(server):
    fake = FakeAperiodic(pending_polls=0)
    fake.poll_script = [
        (502, b"<html>502 Bad Gateway</html>"),
        (429, b""),
        (520, b"<html>Web server is returning an unknown error</html>"),
    ]
    server(fake)

    result = login(open_browser=False)

    assert result.plan_name == "Free"
    assert fake.polls == 4
    assert json.loads(credentials_path().read_text())["api_key"] == NAMED_KEY


def test_a_page_that_is_not_json_reads_as_an_empty_answer(server):
    server(FakeAperiodic())

    assert _request("GET", "/html") == (200, {})


def test_offline_login_and_whoami_fail_with_a_message_not_a_traceback(
    server, monkeypatch, capsys
):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
    monkeypatch.setenv("APERIODIC_API_URL", f"http://127.0.0.1:{closed_port}/api/v1")
    save_key(NAMED_KEY)

    with pytest.raises(AperiodicDataError, match="Could not reach"):
        login(open_browser=False, force=True)
    assert main(["whoami"]) == 1
    assert main(["login", "--no-browser", "--force"]) == 1
    err = capsys.readouterr().err
    assert err.count("Error: Could not reach") == 2
    assert "Traceback" not in err


def test_login_again_with_a_key_that_still_works_keeps_it(server, capsys):
    fake = FakeAperiodic()
    server(fake)
    save_key(OLD_KEY)

    result = login(open_browser=False)

    assert fake.login_names == []
    assert fake.revoked == []
    assert result.key_name == "old-vm"
    assert result.plan_name == "Free"
    assert json.loads(credentials_path().read_text())["api_key"] == OLD_KEY
    assert capsys.readouterr().out.strip() == (
        'Already signed in as "old-vm" on Free. '
        "Call aperiodic.login(force=True) to replace it."
    )


def test_force_login_saves_a_new_key_and_revokes_the_old_one(server):
    fake = FakeAperiodic()
    server(fake)
    save_key(OLD_KEY)

    login(open_browser=False, force=True)

    assert len(fake.login_names) == 1
    assert json.loads(credentials_path().read_text())["api_key"] == NAMED_KEY
    assert fake.revoked == [OLD_KEY]


def test_login_after_the_saved_key_was_revoked_starts_over(server):
    fake = FakeAperiodic()
    server(fake)
    save_key(DEAD_KEY)

    login(open_browser=False)

    assert len(fake.login_names) == 1
    assert json.loads(credentials_path().read_text())["api_key"] == NAMED_KEY
    # Best effort, and its 401 is not an error.
    assert fake.revoked == [DEAD_KEY]


def test_command_line_login_names_its_own_force_flag(server, capsys):
    fake = FakeAperiodic()
    server(fake)
    save_key(OLD_KEY)

    assert main(["login", "--no-browser"]) == 0
    assert "Run python -m aperiodic login --force to replace it." in (
        capsys.readouterr().out
    )
    assert fake.login_names == []

    assert main(["login", "--no-browser", "--force"]) == 0
    assert fake.revoked == [OLD_KEY]
    assert json.loads(credentials_path().read_text())["api_key"] == NAMED_KEY


def test_saving_never_reuses_a_stale_temporary_file(server):
    server(FakeAperiodic())
    credentials_path().parent.mkdir(parents=True, exist_ok=True)
    stale = credentials_path().with_name("credentials.tmp")
    stale.write_text("{}")
    stale.chmod(0o644)

    login(open_browser=False)

    assert stat.S_IMODE(credentials_path().stat().st_mode) == 0o600
    assert not list(credentials_path().parent.glob("credentials-*.tmp"))


def test_calls_find_a_key_passed_in_then_the_environment_then_the_saved_one(
    server, monkeypatch
):
    server(FakeAperiodic())
    with pytest.raises(AperiodicDataError, match=r"aperiodic\.login\(\)"):
        _resolve_api_key(None, preview=False)

    credentials_path().parent.mkdir(parents=True, exist_ok=True)
    credentials_path().write_text(json.dumps({"api_key": NAMED_KEY}))
    assert default_api_key() == NAMED_KEY
    assert _resolve_api_key(None, preview=False) == NAMED_KEY

    monkeypatch.setenv("APERIODIC_API_KEY", "from-env")
    assert _resolve_api_key(None, preview=False) == "from-env"
    assert _resolve_api_key("passed-in", preview=False) == "passed-in"


def test_preview_calls_stay_on_the_demo_key(server):
    server(FakeAperiodic())
    credentials_path().parent.mkdir(parents=True, exist_ok=True)
    credentials_path().write_text(json.dumps({"api_key": NAMED_KEY}))

    assert _resolve_api_key(None, preview=True) == DEMO_API_KEY


def test_logout_revokes_this_machines_key_and_removes_it(server):
    fake = FakeAperiodic()
    server(fake)
    save_key(NAMED_KEY)

    result = logout()

    assert (result.removed, result.revoked) == (True, True)
    assert result.message == "Signed out: this machine's key is revoked and removed."
    assert fake.revoked == [NAMED_KEY]
    assert not credentials_path().exists()
    assert logout().removed is False


@pytest.mark.parametrize(
    ("reply", "says"),
    [
        (
            (
                400,
                {
                    "error": "not_a_named_key",
                    "error_description": "This is not a named key.",
                },
            ),
            "It is the account's default key, which is not revoked from here: "
            "rotate it at https://aperiodic.io/home/api.",
        ),
        ((401, {"error": "Access denied"}), "It was already revoked."),
        ((200, {"revoked": False}), "It was already revoked."),
        (
            (500, {"error": "internal"}),
            "could not revoke it (internal). Revoke it at https://aperiodic.io/home/api.",
        ),
    ],
)
def test_logout_says_what_happened_to_the_key_in_one_line(server, capsys, reply, says):
    fake = FakeAperiodic()
    fake.delete_replies[NAMED_KEY] = reply
    server(fake)
    save_key(NAMED_KEY)

    assert main(["logout"]) == 0

    out = capsys.readouterr().out
    assert out.count("\n") == 1
    assert out.startswith("Removed the key from this machine")
    assert says in out
    assert not credentials_path().exists()


def test_get_symbols_finds_the_saved_key(server):
    fake = FakeAperiodic()
    httpd = server(fake)
    base_url = f"http://127.0.0.1:{httpd.server_port}/api/v1"
    save_key(NAMED_KEY)

    assert get_symbols(exchange="binance-futures", base_url=base_url) == [
        "perpetual-BTC-USDT:USDT"
    ]
    assert asyncio.run(
        get_symbols_async(exchange="binance-futures", base_url=base_url)
    ) == ["perpetual-BTC-USDT:USDT"]
    assert fake.symbol_keys == [NAMED_KEY, NAMED_KEY]


def test_a_rejected_saved_key_says_where_it_is_and_to_log_in_again(server, monkeypatch):
    fake = FakeAperiodic()
    httpd = server(fake)
    base_url = f"http://127.0.0.1:{httpd.server_port}/api/v1"
    save_key(DEAD_KEY)
    expected = (
        f"The key saved in {credentials_path()} was rejected (revoked?). "
        "Run aperiodic.login() again."
    )

    with pytest.raises(APIError) as info:
        get_symbols(exchange="binance-futures", base_url=base_url)
    assert info.value.status_code == 401
    assert info.value.message == expected
    with pytest.raises(AperiodicDataError, match=r"was rejected \(revoked\?\)"):
        whoami()

    # A key passed in or set in the environment keeps the server's own error.
    with pytest.raises(APIError) as passed:
        get_symbols("apk_other", exchange="binance-futures", base_url=base_url)
    assert passed.value.message == "Authorization Required"
    monkeypatch.setenv("APERIODIC_API_KEY", DEAD_KEY)
    with pytest.raises(AperiodicDataError, match=r"^Access denied$"):
        whoami()


def test_whoami_prefers_the_servers_explanation_to_its_error_code(server):
    fake = FakeAperiodic()
    fake.whoami_replies["apk_lapsed"] = (
        403,
        {"error": "plan_lapsed", "error_description": "Your trial ended."},
    )
    server(fake)

    with pytest.raises(AperiodicDataError, match=r"^Your trial ended\.$"):
        whoami("apk_lapsed")


def test_whoami_and_the_command_line(server, capsys):
    server(FakeAperiodic())
    save_key(NAMED_KEY)

    assert whoami()["plan_name"] == "Free"
    assert main(["whoami"]) == 0
    out = capsys.readouterr().out
    assert "19 of 19" in out
    assert "Markets:  BTC, ETH, SOL and 10 RWA perps" in out


def test_login_helpers_are_exported():
    for name in (
        "login",
        "logout",
        "whoami",
        "default_api_key",
        "credentials_path",
        "LoginResult",
        "LogoutResult",
    ):
        assert hasattr(aperiodic, name)
