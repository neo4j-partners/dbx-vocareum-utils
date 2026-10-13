"""End a Vocareum lab session, and read what one is doing before you do.

This exists because a Databricks lab session does not stop on its own in any
way a reader can rely on. A session that failed keeps counting: ``totaltime``
on a session recorded as ``Failed`` was still incrementing hours later, so
"failed" and "finished" are different states and only one of them is free.
Ending a session was, until this file, a call nobody could make without writing
it out by hand each time, which meant the cheapest and most necessary operation
in the whole toolkit was the one with no tool.

Two things it does, and the split between them is the point:

``--status``   read the session record and say what it is doing. Free, safe,
               and the default, because a tool whose default is destructive is
               a tool that ends the wrong session eventually.
``--end``      end it. Explicit, and it says what it is about to end and waits
               for a typed confirmation unless ``--yes`` is passed.

Only ``end`` is implemented. Vocareum's reference documents the body as
``{"action":"end|terminate"}`` and says nothing about how the two differ, and
only ``end`` has ever been exercised here. ``terminate`` is refused rather than
guessed at, because the difference between them is unmeasured and the failure
mode of guessing is a student's work.

Usage:

    uv run dbx-vocareum-session                  # what is this session doing
    uv run dbx-vocareum-session --end            # end it, with a confirmation
    uv run dbx-vocareum-session --end --yes      # end it, for scripted runs
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from .client import ApiError, request_json
from .config import ConfigError, repository_root
from .databricks import find_field_entry
from .diagnose import Config, load_config
from .output import rule, section
from .security import redact_text

EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_API = 2
EXIT_REFUSED = 3

# The body Vocareum's reference documents for this call. Held as a constant
# rather than written inline so that the one action this tool is willing to
# send is visible in one place next to the one it refuses.
END_ACTION = "end"
UNMEASURED_ACTION = "terminate"

# What the record calls a session that is no longer running. Compared
# case-insensitively: these are Vocareum's words, not a documented enum.
# ``Terminated`` is the one measured here; the rest are kept
# because guessing wrong in this direction only costs a redundant end call.
ENDED_STATES = frozenset({"terminated", "ended", "stopped"})

# What the record reads in the seconds between the end call returning success
# and the session settling. Measured: the read-back immediately after a 200
# said ``EndLab``, and twenty seconds later the same record said ``Terminated``.
# It is called out separately because a tool that reports it as "still running"
# tells the operator their end call failed when it did not.
ENDING_STATES = frozenset({"endlab"})

# The fields worth reading before ending something. state and totaltime are the
# two that answer "is this costing money", and the rest identify which session
# is about to be ended, which is the question a confirmation prompt exists for.
STATUS_FIELDS = (
    "state",
    "sessionstart",
    "sessionend",
    "totaltime",
    "useremail",
    "workspaceid",
)


def read_status(config: Config) -> tuple[Any, dict[str, str]]:
    """Return the session record and the status fields found in it.

    The record's shape is undocumented and its nesting is not stable, so every
    field is searched for by name at whatever depth it sits, the same way the
    diagnostic reads it. A field that is absent is simply not in the mapping,
    which keeps "Vocareum did not send this" distinct from "it sent an empty
    value", and those mean different things on a session record.
    """
    payload = request_json("GET", config.resource_url(), config.api_key)
    found: dict[str, str] = {}
    for name in STATUS_FIELDS:
        entry = find_field_entry(payload, name)
        if entry is not None and entry[1] is not None:
            found[name] = str(entry[1])
    return payload, found


def report_status(found: dict[str, str]) -> None:
    """Print the status fields, naming the ones that were not there."""
    for name in STATUS_FIELDS:
        value = found.get(name)
        if value is None:
            print(f"  {name:14s} not present")
            continue
        print(f"  {name:14s} {redact_text(value) or '(empty)'}")


def session_is_running(found: dict[str, str]) -> bool | None:
    """Say whether this session is still running, or ``None`` if it cannot tell.

    ``None`` rather than ``False`` for a record with no state, because a tool
    that reports "not running" from an unreadable record invites someone to
    walk away from a session that is still spending.
    """
    state = found.get("state", "").strip()
    if not state:
        return None
    return state.lower() not in ENDED_STATES


def confirm(config: Config, found: dict[str, str]) -> bool:
    """Ask for a typed confirmation naming the part, and return the answer.

    The part id has to be typed rather than a yes accepted, because the values
    in ``.env`` are the only thing that says which lab this is, and a wrong
    ``VOCAREUM_PART_ID`` produces a call that succeeds against somebody else's
    session and reports success.
    """
    print(
        f"About to end the session for user {config.user_id} on part "
        f"{config.part_id} of course {config.course_id}."
    )
    email = found.get("useremail")
    if email:
        print(f"The lab identity on this session is {email}.")
    if session_is_running(found) is False:
        print(
            f"Its state already reads {found.get('state', '')!r}, so this may "
            "have ended already. Ending it again is harmless."
        )
    if not sys.stdin.isatty():
        print(
            "Refusing: nothing is attached to answer, and --yes was not "
            "passed. Pass --yes to end a session from a script."
        )
        return False
    answer = input(f"Type the part id ({config.part_id}) to confirm: ").strip()
    if answer != config.part_id.strip():
        print("That is not the part id. Nothing was ended.")
        return False
    return True


def end_session(config: Config) -> Any:
    """Send the end call and return the response.

    This call is synchronous. It answers 200 with a status and a message and no
    ``transactionid``, unlike the part update, so there is nothing to poll and a
    caller that waits for a transaction here waits forever.
    """
    return request_json(
        "DELETE",
        config.resource_url(),
        config.api_key,
        {"action": END_ACTION},
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read or end the Vocareum lab session named in .env."
    )
    parser.add_argument(
        "--end",
        action="store_true",
        help="end the session. Without this the tool only reads.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="skip the confirmation prompt. For scripted runs.",
    )
    parser.add_argument(
        "--action",
        default=END_ACTION,
        help=(
            f"the action to send. Only {END_ACTION!r} is implemented; "
            f"{UNMEASURED_ACTION!r} is documented by Vocareum and unmeasured, "
            "so it is refused rather than guessed at."
        ),
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="path to the .env file. Defaults to the repository root.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.action != END_ACTION:
        print(
            f"Refusing to send action {args.action!r}. Vocareum documents "
            f"{UNMEASURED_ACTION!r} alongside {END_ACTION!r} and does not say "
            "how they differ, and nothing here has ever measured it. Ending a "
            "Databricks session leaves the workspace standing, and a call that "
            "might not is not one to make on a lab with a student's work in "
            "it. See docs/vocareum-api.md, 'Ending a session'.",
            file=sys.stderr,
        )
        return EXIT_REFUSED

    try:
        env_path = args.env_file or repository_root() / ".env"
        config = load_config(env_path)
    except ConfigError as error:
        print(str(error), file=sys.stderr)
        return EXIT_CONFIG

    section("The session")
    print(f"GET {config.resource_url()}")
    try:
        _, found = read_status(config)
    except ApiError as error:
        print(str(error), file=sys.stderr)
        return EXIT_API
    rule()
    report_status(found)

    running = session_is_running(found)
    if running is None:
        print(
            "\nThe record carries no state, which is what a session that has "
            "never started looks like. There is probably nothing to end."
        )
    elif running:
        print(
            f"\nThis session is running. totaltime reads "
            f"{found.get('totaltime', 'nothing')} and keeps counting on a "
            "failed session as well as on a healthy one."
        )
    else:
        print(f"\nThis session reads {found.get('state', '')!r} and is not running.")

    if not args.end:
        print("\nRead only. Pass --end to end this session.")
        return EXIT_OK

    rule()
    if not args.yes and not confirm(config, found):
        return EXIT_REFUSED

    section("Ending it")
    print(f'DELETE {config.resource_url()}  {{"action": "{END_ACTION}"}}')
    try:
        response = end_session(config)
    except ApiError as error:
        print(str(error), file=sys.stderr)
        return EXIT_API
    print(f"Response: {redact_text(str(response))}")

    # Read it back rather than trusting the response. The call is synchronous,
    # so the record is authoritative the moment it returns, and this is the one
    # cheap check that the session that ended is the session that was named.
    rule()
    print("Reading the record back:")
    try:
        _, after = read_status(config)
    except ApiError as error:
        print(f"  could not re-read it: {error}")
        return EXIT_OK
    report_status(after)
    state = after.get("state", "").strip()
    if state.lower() in ENDING_STATES:
        print(
            f"\nIt reads {state!r}, which is the transitional state between the "
            "end call and the session settling. It becomes 'Terminated' within "
            "a minute or so. Read it again to confirm."
        )
    elif session_is_running(after):
        print(
            "\nIt still does not read as ended. Vocareum answered success, so "
            "either the state lags the call or the session is not the one that "
            "was ended. Read it again before starting another."
        )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
