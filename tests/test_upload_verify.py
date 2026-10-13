"""What ``verify`` does with a path that is not addressable yet.

Vocareum's transaction reports ``success`` before its files endpoint can address
every path the write just created. Measured: on a deploy of a few dozen
members, over half answered "does not exist" on the very next read, and all
were byte-correct minutes later. Reporting exit ``3`` there is the worst answer
available: the rule is not to start a lab against a mismatch, so a false
mismatch stops a good deploy.

The distinction these tests pin down is that ``remote_digest`` returns ``None``
for two different things, "never written" and "written but not addressable
yet", and only ``verify`` knows it is looking at the second one. So ``None`` is
retried and a digest that came back wrong is not. Retrying a wrong digest until
it passes is how a bad deploy gets through the gate, and it is the failure this
whole verify step exists to catch.

Offline. ``remote_digest`` is replaced with a scripted one and ``sleep`` with a
recorder, so the backoff is asserted rather than waited through.
"""

from __future__ import annotations

import pytest

from dbx_vocareum_tools.upload import (
    VERIFY_SETTLE_BACKOFF,
    Config,
    Member,
    digest,
    verify,
)

CONFIG = Config(
    api_key="unused",
    course_id="1",
    assignment_id="2",
    part_id="3",
    api_base="https://example.invalid/api/v2",
)


def member(name: str, body: bytes = b"contents\n") -> Member:
    from pathlib import Path

    return Member(source=Path(name), arcname=name, payload=body)


class Files:
    """A files endpoint that answers a scripted sequence per path.

    Each path maps to a list of answers, one per read, and the last answer
    repeats forever. That is what lets one test say "absent, then absent, then
    the real digest" without reaching for a clock.
    """

    def __init__(self, answers: dict[str, list[str | None]]) -> None:
        self.answers = answers
        self.reads: list[str] = []

    def __call__(self, config: Config, remote_path: str) -> str | None:
        self.reads.append(remote_path)
        sequence = self.answers[remote_path]
        return sequence.pop(0) if len(sequence) > 1 else sequence[0]


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    recorded: list[float] = []
    monkeypatch.setattr(
        "dbx_vocareum_tools.upload.time.sleep", lambda delay: recorded.append(delay)
    )
    return recorded


def run(
    monkeypatch: pytest.MonkeyPatch,
    files: Files,
    members: list[Member],
    sleeps: list[float],
) -> list[str]:
    monkeypatch.setattr("dbx_vocareum_tools.upload.remote_digest", files)
    return verify(CONFIG, "scripts", members, sleep=lambda delay: sleeps.append(delay))


def test_a_path_that_settles_late_is_not_reported_as_a_mismatch(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # The measured failure, at its smallest: the write landed, the first read
    # could not see it, the second could.
    one = member("courseware/nodes_aircraft.csv")
    files = Files({"scripts/courseware/nodes_aircraft.csv": [None, one.digest]})

    assert run(monkeypatch, files, [one], sleeps) == []
    assert sleeps == [VERIFY_SETTLE_BACKOFF[0]]


def test_the_wrong_bytes_are_reported_at_once_and_never_read_again(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # The half that must not be retried. A path answering with the wrong bytes
    # is the real mismatch exit 3 exists for, and reading it until it agrees is
    # how a bad deploy talks its way through the gate.
    one = member("user_setup.sh")
    files = Files({"scripts/user_setup.sh": [digest(b"something else\n")]})

    assert run(monkeypatch, files, [one], sleeps) == ["user_setup.sh"]
    assert files.reads == ["scripts/user_setup.sh"]
    assert sleeps == []


def test_a_path_that_never_arrives_is_still_a_mismatch(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    one = member("workshop.py")
    files = Files({"scripts/workshop.py": [None]})

    assert run(monkeypatch, files, [one], sleeps) == ["workshop.py"]
    assert sleeps == list(VERIFY_SETTLE_BACKOFF)
    assert len(files.reads) == len(VERIFY_SETTLE_BACKOFF) + 1


def test_an_upload_that_reads_back_at_once_waits_for_nothing(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # The common case, and the reason the wait is conditional rather than a
    # fixed settle delay before the first read.
    members = [member(f"hook_{index}.sh") for index in range(3)]
    files = Files({f"scripts/{item.arcname}": [item.digest] for item in members})

    assert run(monkeypatch, files, members, sleeps) == []
    assert sleeps == []
    assert len(files.reads) == 3


def test_only_the_unsettled_paths_are_read_again(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    # 21 of 37 lagged in the measured run. Re-reading all 37 each round would
    # be 111 downloads to learn what 21 answer.
    settled = member("voclib.sh")
    late = member("courseware/dlt_fleet_etl.py")
    files = Files(
        {
            "scripts/voclib.sh": [settled.digest],
            "scripts/courseware/dlt_fleet_etl.py": [None, late.digest],
        }
    )

    assert run(monkeypatch, files, [settled, late], sleeps) == []
    assert files.reads.count("scripts/voclib.sh") == 1
    assert files.reads.count("scripts/courseware/dlt_fleet_etl.py") == 2


def test_a_mismatch_and_a_late_arrival_in_one_run_are_told_apart(
    monkeypatch: pytest.MonkeyPatch, sleeps: list[float]
) -> None:
    late = member("course.env")
    wrong = member("lab_end.sh")
    files = Files(
        {
            "scripts/course.env": [None, late.digest],
            "scripts/lab_end.sh": [digest(b"stale\n")],
        }
    )

    assert run(monkeypatch, files, [late, wrong], sleeps) == ["lab_end.sh"]
    assert files.reads.count("scripts/lab_end.sh") == 1
