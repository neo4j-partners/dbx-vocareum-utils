"""Run the shipped self-test case tables through pytest.

The tables live in ``dbx_vocareum_tools.selftest`` rather than here because
``--self-test`` has to run inside a lab environment, where this suite is not
installed. Importing them means a case added for one runner is checked by both,
so the packaged self-test and the development suite cannot drift into disagreeing
about what the classifier should do.
"""

from __future__ import annotations

import pytest

from dbx_vocareum_tools.databricks import (
    GRADE_SCRIPT,
    LIFECYCLE_SCRIPTS,
    DatabricksHostError,
    normalize_host,
    workspace_id_from_url,
)
from dbx_vocareum_tools.probe import (
    CATALOG,
    NEXT_ACTION,
    TRACKER_STATUS,
    ApiOperation,
    GuardError,
    Probe,
    Verdict,
    classify,
    guard_credentials,
    is_read_only,
    looks_empty,
)
from dbx_vocareum_tools.selftest import (
    BAD_HOST_CASES,
    CLASSIFIER_CASES,
    DENIAL_CASES,
    EMPTY_CASES,
    HOST_CASES,
    SAFETY_CASES,
    SP_GUARD_CASES,
    STATUS_CASES,
    VERDICTS_OUTSIDE_CLASSIFY,
    WORKSPACE_ID_CASES,
    make_config,
)


@pytest.mark.parametrize(("code", "message", "expected"), CLASSIFIER_CASES)
def test_error_code_classification(code: str, message: str, expected: Verdict) -> None:
    assert classify(code, message) is expected


@pytest.mark.parametrize(("message", "expected"), DENIAL_CASES)
def test_which_grant_is_missing(message: str, expected: Verdict) -> None:
    """PERMISSION_DENIED covers three grants made by three different admins.

    Only the message body says which one is missing, and each is fixed in a
    different console, so collapsing them would turn an actionable result into a
    shrug.
    """
    assert classify("PERMISSION_DENIED", message) is expected


@pytest.mark.parametrize(("verdict", "expected"), STATUS_CASES)
def test_verdict_maps_to_tracker_status(verdict: Verdict, expected: str) -> None:
    assert TRACKER_STATUS[verdict] == expected


def test_not_found_is_not_recorded_as_allowed() -> None:
    """Databricks hides rather than refuses, so not-found proves nothing.

    A principal with no permission on an object is told the object does not
    exist. The AWS classifier can read the same condition as evidence the call
    was permitted. Here it cannot.
    """
    assert TRACKER_STATUS[Verdict.NOT_FOUND_OR_HIDDEN] == "Inconclusive"


def test_every_verdict_has_a_status_and_a_next_step() -> None:
    for verdict in Verdict:
        assert verdict in TRACKER_STATUS, f"{verdict} has no tracker status"
        assert NEXT_ACTION.get(verdict), f"{verdict} has no next step written"


def test_every_verdict_is_reachable() -> None:
    """A verdict nothing can produce is a claim the tool cannot make."""
    produced = {classify(code, message) for code, message, _ in CLASSIFIER_CASES}
    produced |= {classify("PERMISSION_DENIED", message) for message, _ in DENIAL_CASES}
    unreachable = set(Verdict) - produced - VERDICTS_OUTSIDE_CLASSIFY
    assert not unreachable, f"unreachable verdicts: {sorted(unreachable)}"


@pytest.mark.parametrize(("raw", "expected"), HOST_CASES)
def test_host_normalization(raw: str, expected: str) -> None:
    assert normalize_host(raw) == expected


@pytest.mark.parametrize("raw", BAD_HOST_CASES)
def test_unusable_hosts_are_refused(raw: str) -> None:
    with pytest.raises(DatabricksHostError):
        normalize_host(raw)


@pytest.mark.parametrize(("url", "expected"), WORKSPACE_ID_CASES)
def test_workspace_id_from_url(url: str, expected: str | None) -> None:
    assert workspace_id_from_url(url) == expected


@pytest.mark.parametrize(("label", "response", "expected"), EMPTY_CASES)
def test_empty_response_detection(label: str, response: object, expected: bool) -> None:
    assert looks_empty(response) is expected


@pytest.mark.parametrize(("label", "client_id", "client_secret"), SP_GUARD_CASES)
def test_credential_guard_refuses_blanks(
    label: str, client_id: str, client_secret: str
) -> None:
    """A credential present in name and absent in fact is the dangerous case.

    The SDK reads a blank as "not supplied" and falls through its provider chain
    to an ambient credential, so the run would report on a principal nobody
    chose.
    """
    with pytest.raises(GuardError):
        guard_credentials(make_config(client_id=client_id, client_secret=client_secret))


def test_credential_guard_accepts_a_complete_pair() -> None:
    guard_credentials(make_config())


def test_credential_guard_refuses_a_blank_lab_user_token() -> None:
    with pytest.raises(GuardError):
        guard_credentials(make_config(source="token", lab_user_token=""))


@pytest.mark.parametrize(("name", "method", "expected"), SAFETY_CASES)
def test_probe_safety(name: str, method: str, expected: bool) -> None:
    probe = Probe(name=name, operation=ApiOperation(method, "/api/2.0/x"))
    assert probe.is_safe() is expected


def test_the_catalog_holds_no_writes() -> None:
    """``--all`` walks the whole catalog, so nothing in it may start compute."""
    writes = sorted(
        name
        for name, operation in CATALOG.items()
        if not is_read_only(operation.method)
    )
    assert not writes, f"the built-in catalog holds writes: {writes}"


def test_lifecycle_execution_order() -> None:
    assert [script.name for script in LIFECYCLE_SCRIPTS] == [
        "workspace_init.sh",
        "lab_setup.sh",
        "user_setup.sh",
        "lab_end.sh",
    ]


def test_only_user_setup_is_required() -> None:
    """A lab with only user_setup.sh is a complete lab."""
    required = [script.name for script in LIFECYCLE_SCRIPTS if script.required]
    assert required == ["user_setup.sh"]


def test_only_two_hooks_can_redirect_the_student() -> None:
    """VOC_IPC_DATA_FILE is the only channel that chooses the landing page."""
    writers = [script.name for script in LIFECYCLE_SCRIPTS if script.writes_ipc]
    assert writers == ["lab_setup.sh", "user_setup.sh"]


def test_grade_is_not_part_of_the_lifecycle() -> None:
    """grade.sh runs on Submit, not on session start, and only if enabled."""
    assert GRADE_SCRIPT.name not in {script.name for script in LIFECYCLE_SCRIPTS}
    assert not GRADE_SCRIPT.writes_ipc
