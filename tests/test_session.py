"""Tests for the session tool, which is the one tool here that ends something.

The reads are covered lightly and the refusals heavily, because a wrong read
prints a wrong line and a wrong write ends a lab a student is sitting in.
"""

from __future__ import annotations

import pytest

from dbx_vocareum_tools.diagnose import Config
from dbx_vocareum_tools.session import (
    END_ACTION,
    ENDED_STATES,
    ENDING_STATES,
    EXIT_REFUSED,
    STATUS_FIELDS,
    UNMEASURED_ACTION,
    main,
    parse_args,
    read_status,
    report_status,
    session_is_running,
)

STUDENT = "labuser10000001_1785962713@vocareum.com"


def session_config() -> Config:
    """A configuration addressing part 5000002, the one this repository runs."""
    return Config(
        api_base="https://api.vocareum.com/api/v2",
        course_id="200001",
        assignment_id="5000001",
        part_id="5000002",
        user_id="5000100",
        api_key="token",
        host="https://example.cloud.databricks.com",
        account_host="https://accounts.cloud.databricks.com",
        account_id="account",
        client_id="client",
        client_secret="secret",
        expected_workspace_id="2222222222222222",
    )


def test_terminate_is_refused_before_anything_is_read(
    capsys: pytest.CaptureFixture,
) -> None:
    """The refusal has to come before the config load, or it needs credentials.

    Vocareum documents `terminate` beside `end` and does not say how they
    differ. A tool that sends it to find out is running the experiment on a
    live lab.
    """
    assert main([f"--action={UNMEASURED_ACTION}", "--end"]) == EXIT_REFUSED
    captured = capsys.readouterr()
    assert UNMEASURED_ACTION in captured.err
    assert "Refusing" in captured.err


def test_reading_is_the_default() -> None:
    """A tool whose default is destructive ends the wrong session eventually."""
    assert parse_args([]).end is False
    assert parse_args([]).action == END_ACTION
    assert parse_args(["--end"]).end is True


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ("Running", True),
        ("Failed", True),
        ("Terminated", False),
        ("terminated", False),
        ("  Ended  ", False),
    ],
)
def test_a_failed_session_still_counts_as_running(state: str, expected: bool) -> None:
    """Failed is not finished. It was still counting up hours later."""
    assert session_is_running({"state": state}) is expected


def test_the_transitional_state_is_named_rather_than_called_a_failure() -> None:
    """`EndLab` is what the record reads in the seconds after a successful end.

    Measured: the read-back immediately after a 200 said `EndLab`,
    and twenty seconds later the same record said `Terminated`. Reporting the
    first as "still running" tells the operator their end call failed.
    """
    assert "endlab" in ENDING_STATES
    assert ENDING_STATES.isdisjoint(ENDED_STATES)


def test_an_unreadable_state_is_unknown_rather_than_stopped() -> None:
    """Reporting "not running" from an empty record invites walking away from one
    that is still spending."""
    assert session_is_running({}) is None
    assert session_is_running({"state": "   "}) is None


def test_the_record_is_searched_by_name_at_any_depth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The record's nesting is undocumented and not stable, so paths are out."""
    record = {
        "resources": [
            {
                "state": "Failed",
                "totaltime": 4198,
                "databricks": {"useremail": STUDENT},
            }
        ]
    }
    monkeypatch.setattr(
        "dbx_vocareum_tools.session.request_json", lambda *a, **k: record
    )
    _, found = read_status(session_config())
    assert found["state"] == "Failed"
    assert found["totaltime"] == "4198"
    assert found["useremail"] == STUDENT
    # Absent rather than empty, which is a different fact about the record.
    assert "sessionend" not in found


def test_an_absent_field_prints_as_not_present(capsys: pytest.CaptureFixture) -> None:
    """`not present` and `(empty)` mean different things on a session record."""
    report_status({"state": "Failed", "sessionstart": ""})
    printed = capsys.readouterr().out
    assert "state          Failed" in printed
    assert "sessionstart   (empty)" in printed
    assert "sessionend     not present" in printed
    for name in STATUS_FIELDS:
        assert name in printed
