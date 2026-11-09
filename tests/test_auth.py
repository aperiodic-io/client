"""Signing in: aperiodic.login(), the saved key, and how calls find a key."""

from __future__ import annotations

import json
import stat
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import aperiodic
from aperiodic import AperiodicDataError
from aperiodic.__main__ import main
from aperiodic.auth import credentials_path, default_api_key, login, logout, whoami
from aperiodic.config import DEMO_API_KEY
from aperiodic.endpoints.utils import _resolve_api_key

NAMED_KEY = "apk_" + "k" * 43


class FakeAperiodic:
    """/api/v1/cli/* as aperiodic.io answers it: pending for a few polls, then the key."""

    def __init__(self, pending_polls: int = 1, final_error: str | None = None):
        self.pending_polls = pending_polls
        self.final_error = final_error
        self.polls = 0
        self.login_names: list[str] = []
        self.revoked: list[str] = []

    def handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _reply(self, status: int, body: dict):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
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
                if self.path == "/api/v1/cli/whoami":
                    if self.headers.get("X-API-KEY") != NAMED_KEY:
                        return self._reply(401, {"error": "Access denied"})
                    return self._reply(
                        200,
                        {
                            "plan": "loggedInNoSubscription",
                            "plan_name": "Free",
                            "key": {
                                "kind": "named",
                                "name": "research-vm",
                                "hint": "kkkk",
                            },
                            "covers": {"datasets_in_full": 19, "datasets_total": 19},
                        },
                    )
                return self._reply(404, {})

            def do_DELETE(self):
                fake.revoked.append(self.headers.get("X-API-KEY"))
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


def no_sleep(_seconds):
    pass


def test_login_saves_a_key_only_this_user_can_read(server, capsys):
    fake = FakeAperiodic(pending_polls=2)
    server(fake)

    result = login(open_browser=False, sleep=no_sleep)

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
        login(open_browser=False, sleep=no_sleep)
    assert not credentials_path().exists()


def test_login_backs_off_when_told_to_slow_down(server):
    fake = FakeAperiodic(pending_polls=0, final_error="slow_down")
    server(fake)
    waits: list[float] = []

    def record(seconds):
        waits.append(seconds)
        if len(waits) == 2:
            fake.final_error = None

    login(open_browser=False, sleep=record)
    assert waits[1] > waits[0]


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
    credentials_path().parent.mkdir(parents=True, exist_ok=True)
    credentials_path().write_text(json.dumps({"api_key": NAMED_KEY}))

    assert logout() is True
    assert fake.revoked == [NAMED_KEY]
    assert not credentials_path().exists()
    assert logout() is False


def test_whoami_and_the_command_line(server, capsys):
    server(FakeAperiodic())
    credentials_path().parent.mkdir(parents=True, exist_ok=True)
    credentials_path().write_text(json.dumps({"api_key": NAMED_KEY}))

    assert whoami()["plan_name"] == "Free"
    assert main(["whoami"]) == 0
    assert "19 of 19" in capsys.readouterr().out


def test_login_helpers_are_exported():
    for name in ("login", "logout", "whoami", "default_api_key", "credentials_path"):
        assert hasattr(aperiodic, name)
