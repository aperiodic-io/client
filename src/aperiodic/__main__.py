"""``python -m aperiodic login | logout | whoami``, for machines without the Aperiodic CLI."""

from __future__ import annotations

import argparse
import sys

from .auth import _login, logout, whoami
from .client import AperiodicDataError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m aperiodic")
    commands = parser.add_subparsers(dest="command", required=True)
    login_cmd = commands.add_parser(
        "login", help="Sign in and save a key for this machine"
    )
    login_cmd.add_argument("--name", help="Name for this machine's key")
    login_cmd.add_argument(
        "--no-browser",
        action="store_true",
        help="Print the link instead of opening a browser",
    )
    login_cmd.add_argument(
        "--force",
        action="store_true",
        help="Replace a saved key that still works (the old one is revoked)",
    )
    commands.add_parser("logout", help="Revoke this machine's key and remove it")
    commands.add_parser(
        "whoami", help="Show which key is in use and what the plan covers"
    )
    args = parser.parse_args(argv)

    try:
        if args.command == "login":
            _login(
                args.name,
                open_browser=False if args.no_browser else None,
                force=args.force,
                force_hint="Run python -m aperiodic login --force to replace it.",
            )
        elif args.command == "logout":
            print(logout().message)
        else:
            me = whoami()
            covers = me.get("covers", {})
            key = me.get("key", {})
            print(f"Plan:     {me.get('plan_name')}")
            print(f"Key:      {key.get('name') or key.get('kind')}")
            print(
                f"Datasets: {covers.get('datasets_in_full')} of {covers.get('datasets_total')} in full"
            )
            if covers.get("markets"):
                print(f"Markets:  {covers['markets']}")
            if covers.get("history"):
                print(f"History:  {covers['history']}")
            if covers.get("live"):
                print(f"Live:     {covers['live']}")
    except AperiodicDataError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
