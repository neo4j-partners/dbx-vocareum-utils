"""Diagnostic interpretation tests.

There is no CloudFormation event log for a shell script, so the diagnostic reads
the workspace state and the custom-data file instead and interprets the gap.
These tests drive that interpretation from an ``Observations`` record, which is
the whole reason the observations are held apart from the calls that gather them.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.errors import DatabricksError
from requests.exceptions import RequestException

from dbx_vocareum_tools.databricks import (
    beacon_fields,
    find_field,
    find_field_entry,
    flatten_keys,
    principal_created_at,
    script_names_from_listing,
)
from dbx_vocareum_tools.diagnose import (
    Config,
    Observations,
    assignment_names,
    gate_failed,
    interpret,
    normalize_host_or_blank,
    read_beacon,
    read_expectations,
    read_user_folder,
    read_vocareum_workspace,
    report_custom_data,
    scim_lookup,
    session_has_started,
)


class ExpectStub:
    """A workspace client for step 5C whose calls answer from a table.

    An unlisted path raises the not-found error Databricks sends for an object
    that is absent, which is the case these tests are mostly about.
    """

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.api_client = self

    def do(self, method: str, path: str, query: dict[str, Any] | None = None) -> Any:
        if path not in self.answers:
            raise DatabricksError(f"{path} does not exist.", error_code="NOT_FOUND")
        return self.answers[path]


WORKSPACE = "https://dbc-33333333-3333.cloud.databricks.com"
STUDENT = "student1_200000@vocareum.com"


def healthy() -> Observations:
    """A run in which everything the tool knows how to check came back consistent."""
    return Observations(
        labtype="Databricks",
        end_lab_behavior="terminate",
        deployed_scripts=[
            "lab_end.sh",
            "lab_setup.sh",
            "user_setup.sh",
            "voclib.sh",
            "workspace_init.sh",
        ],
        scripts_listed=True,
        session_started=True,
        workspace_url=WORKSPACE,
        lab_user_email=STUDENT,
        group_name="voc-group-200000-student1",
        notebook_url=f"{WORKSPACE}/?o=3333333333333333#workspace/Users/{STUDENT}",
        user_found=True,
        user_groups=["voc-group-200000-student1"],
        clusters_for_user=["student1-cluster"],
        user_folder_exists=True,
        workspace_reached=True,
    )


def labels(observations: Observations) -> list[str]:
    return [finding.label for finding in interpret(observations)]


def test_a_consistent_run_produces_no_findings() -> None:
    assert interpret(healthy()) == []


def test_a_non_databricks_part_is_reported() -> None:
    """The one misconfiguration that starts successfully and does the wrong thing."""
    observations = healthy()
    observations.labtype = "Notebook"
    assert "The part is not a Databricks lab" in labels(observations)


def test_a_missing_required_hook_is_reported() -> None:
    """Vocareum skips an absent hook in silence, so the lab starts and does nothing."""
    observations = healthy()
    observations.deployed_scripts = ["lab_end.sh", "workspace_init.sh"]
    findings = interpret(observations)
    assert "A required lifecycle script is not deployed" in [
        finding.label for finding in findings
    ]
    detail = next(
        finding.detail
        for finding in findings
        if finding.label == "A required lifecycle script is not deployed"
    )
    assert "user_setup.sh" in detail


def test_an_optional_hook_is_not_reported_as_missing() -> None:
    """A lab with only user_setup.sh is a complete lab."""
    observations = healthy()
    observations.deployed_scripts = ["user_setup.sh"]
    assert "A required lifecycle script is not deployed" not in labels(observations)


def test_an_unreadable_listing_reports_no_missing_hook() -> None:
    """Absent and unknown are different, and only one of them means upload."""
    observations = healthy()
    observations.deployed_scripts = []
    observations.scripts_listed = False
    assert "A required lifecycle script is not deployed" not in labels(observations)


def test_an_empty_listing_that_was_read_does_report_a_missing_hook() -> None:
    """The gate must not swallow the real case it exists to qualify."""
    observations = healthy()
    observations.deployed_scripts = []
    observations.scripts_listed = True
    assert "A required lifecycle script is not deployed" in labels(observations)


def test_a_never_started_session_is_reported_instead_of_blaming_the_hook() -> None:
    """An absent notebook URL is expected before a session has ever run."""
    observations = healthy()
    observations.session_started = False
    observations.notebook_url = ""
    reported = labels(observations)
    assert "This session has never started" in reported
    assert "No notebook URL was handed back" not in reported


def test_a_script_that_reported_its_own_failure_comes_first() -> None:
    """The closest thing to a log Vocareum exposes, so it outranks inference."""
    observations = healthy()
    observations.script_error_code = "MISSING_WORKSPACE_URL"
    observations.script_message = "Vocareum did not set VOC_DB_WORKSPACE_URL."
    assert (
        interpret(observations)[0].label
        == "A lifecycle script reported its own failure"
    )


def test_no_notebook_url_is_reported() -> None:
    """The student lands in a bare workspace rather than on the workshop notebook."""
    observations = healthy()
    observations.notebook_url = ""
    assert "No notebook URL was handed back" in labels(observations)


def test_an_absent_lab_user_is_reported() -> None:
    observations = healthy()
    observations.user_found = False
    assert "The lab user does not exist in the workspace" in labels(observations)


def test_a_user_in_no_group_is_reported() -> None:
    """Permissions are granted to groups, so a student in none can do nothing."""
    observations = healthy()
    observations.user_groups = []
    assert "The lab user is in no group" in labels(observations)


def test_an_absent_workspace_folder_is_reported() -> None:
    observations = healthy()
    observations.user_folder_exists = False
    assert "The lab user has no visible workspace folder" in labels(observations)


class FailingClient:
    """A workspace client whose every call fails with one chosen error."""

    def __init__(self, error: Exception) -> None:
        self.api_client = self
        self._error = error

    def do(self, *_args: object, **_kwargs: object) -> object:
        raise self._error


def test_a_not_found_folder_is_recorded_as_absent() -> None:
    observations = healthy()
    observations.user_folder_exists = None
    read_user_folder(
        FailingClient(
            DatabricksError("path does not exist", error_code="RESOURCE_DOES_NOT_EXIST")
        ),
        observations,
    )
    assert observations.user_folder_exists is False


@pytest.mark.parametrize(
    "error",
    [
        DatabricksError("denied", error_code="PERMISSION_DENIED"),
        DatabricksError("slow down", error_code="REQUEST_LIMIT_EXCEEDED"),
        RequestException("connection reset"),
    ],
)
def test_a_folder_that_could_not_be_read_is_left_unmeasured(error: Exception) -> None:
    """A failed call is not evidence of an absent folder.

    Only a not-found answer says anything about the folder. A denial, a throttle,
    or a transport failure says something about the call, and recording one of
    those as absence would produce a finding, with guidance, about something the
    run never measured.
    """
    observations = healthy()
    observations.user_folder_exists = None
    read_user_folder(FailingClient(error), observations)
    assert observations.user_folder_exists is None
    assert "The lab user has no visible workspace folder" not in labels(observations)


def test_workspace_findings_are_suppressed_when_the_workspace_was_not_read() -> None:
    """Without DATABRICKS_HOST there is no workspace evidence, so no verdict.

    Reporting "no compute belongs to the student" after making no Databricks call
    would be a confident claim about something never measured.
    """
    observations = Observations(
        labtype="Databricks",
        deployed_scripts=["user_setup.sh"],
        lab_user_email=STUDENT,
        notebook_url=f"{WORKSPACE}/#workspace/Users/{STUDENT}",
        workspace_reached=False,
    )
    found = labels(observations)
    assert "No compute belongs to the lab user" not in found
    assert "The lab user has no visible workspace folder" not in found


class AnsweringClient:
    """A client that returns one chosen payload from every call."""

    def __init__(self, payload: object) -> None:
        self.api_client = self
        self._payload = payload

    def do(self, *_args: object, **_kwargs: object) -> object:
        return self._payload


def test_a_scim_filter_matching_nothing_is_an_answer_not_a_silence() -> None:
    """The read dbx-vocareum-probe structurally cannot make.

    A filter that matches nothing still returns a well-formed body, so the
    probe's ``looks_empty`` calls it OK and reports a successful call rather
    than an absent principal. Only opening the body separates the two.
    """
    reading = scim_lookup(AnsweringClient({"Resources": []}), "/scim", 'id eq "x"')
    assert reading.read is True
    assert reading.found is False


def test_a_scim_call_that_failed_is_not_an_answer() -> None:
    reading = scim_lookup(
        FailingClient(DatabricksError("nope", error_code="SCIM_404")),
        "/scim",
        'id eq "x"',
    )
    assert reading.read is False
    assert reading.found is False


def test_a_scim_match_carries_its_id_and_creation_time() -> None:
    reading = scim_lookup(
        AnsweringClient(
            {
                "Resources": [
                    {
                        "id": "12121212121212",
                        "displayName": STUDENT,
                        "meta": {"created": "2026-08-05T20:45:13Z"},
                    }
                ]
            }
        ),
        "/scim",
        f'userName eq "{STUDENT}"',
    )
    assert reading.found is True
    assert reading.principal_id == "12121212121212"
    assert reading.created == "2026-08-05T20:45:13Z"


def test_assignment_names_cover_every_principal_kind() -> None:
    """A group holds an assignment under group_name, never under user_name."""
    entries = [
        {"principal": {"user_name": STUDENT}, "permissions": ["USER"]},
        {"principal": {"group_name": "Group-lab-1"}, "permissions": ["USER"]},
        {"principal": {"service_principal_name": "sp-1"}, "permissions": ["ADMIN"]},
        {"principal": {}, "permissions": []},
    ]
    assert assignment_names(entries) == {STUDENT, "Group-lab-1", "sp-1"}


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("labuser10000001_1785962713@vocareum.com", "2026-08-05T20:45:13+00:00"),
        ("Group-lab-10000001-5000002-1785962712", "2026-08-05T20:45:12+00:00"),
    ],
)
def test_a_vocareum_name_carries_its_own_creation_time(
    name: str, expected: str
) -> None:
    """The only clock available over a record that timestamps nothing."""
    created = principal_created_at(name)
    assert created is not None
    assert created.isoformat() == expected


@pytest.mark.parametrize(
    "name",
    ["adminuser1000000@vocareum.com", "users-clone-2026-08-04-2144-UTC", "voc-group"],
)
def test_a_name_without_an_epoch_reports_no_time(name: str) -> None:
    """Reporting a date read out of a number that is not one would invent evidence."""
    assert principal_created_at(name) is None


def workspace_config(org_number: str = "999") -> Config:
    """A configuration addressing part 5000002, which the fixtures below name."""
    return Config(
        api_base="https://api.vocareum.com/api/v2",
        course_id="200001",
        assignment_id="5000001",
        part_id="5000002",
        user_id="5000100",
        api_key="token",
        host=WORKSPACE,
        account_host="https://accounts.cloud.databricks.com",
        account_id="account",
        client_id="client",
        client_secret="secret",
        expected_workspace_id="2222222222222222",
        org_number=org_number,
    )


WORKSPACE_RECORD = {
    "workspaces": [
        {
            "partid": "9999999",
            "status": "RUNNING",
            "num_users": "5",
            "error_info": "a different part's problem",
        },
        {
            "partid": "5000002",
            "workspace_id": "2222222222222222",
            "status": "RUNNING",
            "num_users": "2",
            "error_info": (
                "Databricks (add user to workspace) Principal not found in "
                "account. Error code: RESOURCE_DOES_NOT_EXIST"
            ),
        },
    ]
}


def test_the_workspace_record_is_filtered_to_the_configured_part(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A workspace belongs to a part, and an org holds many parts."""
    monkeypatch.setattr(
        "dbx_vocareum_tools.diagnose.request_json",
        lambda *_args, **_kwargs: WORKSPACE_RECORD,
    )
    observations = Observations()
    read_vocareum_workspace(workspace_config(), observations)
    assert observations.vocareum_num_users == "2"
    assert observations.vocareum_workspace_status == "RUNNING"
    assert "Principal not found in account" in observations.vocareum_workspace_error


def test_a_part_with_no_workspace_record_records_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "dbx_vocareum_tools.diagnose.request_json",
        lambda *_args, **_kwargs: {"workspaces": []},
    )
    observations = Observations()
    read_vocareum_workspace(workspace_config(), observations)
    assert observations.vocareum_workspace_error == ""
    assert (
        "Vocareum recorded a Databricks-side failure on this workspace"
        not in labels(observations)
    )


def test_an_unset_org_number_makes_no_call(monkeypatch: pytest.MonkeyPatch) -> None:
    """The whole diagnostic must not refuse to start over one optional endpoint."""

    def refuse(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("no call should have been made")

    monkeypatch.setattr("dbx_vocareum_tools.diagnose.request_json", refuse)
    observations = Observations()
    read_vocareum_workspace(workspace_config(org_number=""), observations)
    assert observations.vocareum_workspace_error == ""


def test_a_recorded_error_info_is_reported_with_its_caveat() -> None:
    """It carries no timestamp, so a finding that dated it would invent evidence."""
    observations = healthy()
    observations.vocareum_workspace_error = "Principal not found in account."
    observations.vocareum_workspace_status = "RUNNING"
    findings = interpret(observations)
    finding = next(
        item
        for item in findings
        if item.label == "Vocareum recorded a Databricks-side failure on this workspace"
    )
    assert "no timestamp" in finding.guidance


def test_the_org_workspace_url_uses_the_only_shape_that_answers() -> None:
    """The course-scoped and part-scoped forms both return 400 Invalid Request."""
    assert workspace_config().org_endpoints().databricks_workspaces_url() == (
        "https://api.vocareum.com/api/v2/orgs/999/databricks_workspaces"
    )


def federated() -> Observations:
    """A run whose principals step was taken and found nothing wrong.

    Both planes answered, the two ids agree, and both principals hold their
    assignment, so no principal finding should fire from it.
    """
    observations = healthy()
    observations.account_reached = True
    observations.workspace_federated = True
    observations.user_in_account = True
    observations.group_in_account = True
    observations.user_has_assignment = True
    observations.group_has_assignment = True
    observations.account_user_id = "12121212121212"
    observations.workspace_user_id = "12121212121212"
    observations.account_group_id = "4444444444444444"
    observations.workspace_group_id = "4444444444444444"
    return observations


def test_a_clean_principals_reading_produces_no_findings() -> None:
    assert interpret(federated()) == []


def test_a_workspace_without_identity_federation_is_reported() -> None:
    """The condition that produces the error verbatim however correct the id is."""
    observations = federated()
    observations.workspace_federated = False
    assert "This workspace is not identity federated" in labels(observations)


def test_a_group_missing_from_the_account_and_unassigned_is_the_missing_principal() -> (
    None
):
    observations = federated()
    observations.group_in_account = False
    observations.group_has_assignment = False
    assert "The session's group is the principal that was not found" in labels(
        observations
    )


def test_a_group_missing_from_the_account_but_assigned_is_reported_differently() -> (
    None
):
    """An absence that did not stop anything must not read as the cause.

    This is the shape the live workspace is actually in: the group resolves on
    neither account SCIM path and holds an account-level assignment regardless.
    Reporting it as the missing principal would send the reader after a step
    that succeeded.
    """
    observations = federated()
    observations.group_in_account = False
    found = labels(observations)
    assert "The session's group is workspace-local and assigned regardless" in found
    assert "The session's group is the principal that was not found" not in found


def test_principal_findings_are_suppressed_when_the_account_was_not_read() -> None:
    """Absence is only evidence once the same caller has read that plane at all.

    The account API answers a caller with no permission by saying the object does
    not exist, so an unread plane and an empty one must not produce the same
    finding.
    """
    observations = healthy()
    observations.group_in_account = False
    observations.user_in_account = False
    found = labels(observations)
    assert "The session's group is the principal that was not found" not in found
    assert "The lab user has no account-level counterpart" not in found


def test_a_run_without_the_principals_step_produces_no_principal_findings() -> None:
    """Every principal field defaults to unmeasured, and unmeasured says nothing."""
    assert interpret(healthy()) == []


def test_two_ids_for_one_username_are_reported() -> None:
    observations = federated()
    observations.workspace_user_id = "12121212121212"
    observations.account_user_id = "8888888888888"
    assert "The lab user is two principals, not one" in labels(observations)


def test_one_id_missing_is_not_read_as_two_principals() -> None:
    """A plane that did not answer leaves the ids uncompared, not unequal."""
    observations = federated()
    observations.account_user_id = ""
    assert "The lab user is two principals, not one" not in labels(observations)


def test_a_lab_user_holding_no_assignment_is_reported() -> None:
    observations = federated()
    observations.user_has_assignment = False
    assert "The lab user holds no permission assignment on this workspace" in labels(
        observations
    )


@pytest.mark.parametrize("state", ["Failed", "failed", " FAILED "])
def test_a_failed_session_dominates_the_report(state: str) -> None:
    """A failed session explains the empty fields that follow from it.

    Without this, the run prints "no notebook URL was handed back" as though it
    were a separate symptom of a lab that ran.
    """
    observations = federated()
    observations.session_state = state
    observations.notebook_url = ""
    found = labels(observations)
    assert found[0] == "Vocareum reports this session as failed"
    assert "No notebook URL was handed back" not in found


def test_a_running_session_with_no_notebook_url_still_reports_it() -> None:
    """The failed-session finding must not swallow the ordinary case."""
    observations = federated()
    observations.session_state = "Running"
    observations.notebook_url = ""
    assert "No notebook URL was handed back" in labels(observations)


def test_every_finding_carries_a_next_step() -> None:
    """A finding without guidance is an observation dressed as a diagnosis."""
    broken = Observations(
        labtype="Notebook",
        deployed_scripts=[],
        script_error_code="SOME_CODE",
        script_message="something went wrong",
        lab_user_email=STUDENT,
        user_found=False,
        workspace_reached=True,
    )
    findings = interpret(broken)
    assert findings
    for finding in findings:
        assert finding.label and finding.detail and finding.guidance


ENTRY_BEACON = (
    "https://dbc-33333333-3333.cloud.databricks.com/#voc-diag=entered"
    "&script=user_setup.sh&at=2026-08-05T21:39:52Z"
    "&vars=VOC_DB_GROUP_NAME+VOC_DB_USER_EMAIL"
)


def test_a_landing_url_is_not_read_as_a_beacon() -> None:
    """Both arrive through notebook_url, so the fragment is what separates them."""
    observations = healthy()
    read_beacon(observations)
    assert observations.beacon_state == ""
    assert observations.beacon_variables == []


def test_a_beacon_is_decoded_into_its_four_fields() -> None:
    observations = healthy()
    observations.notebook_url = ENTRY_BEACON
    read_beacon(observations)
    assert observations.beacon_state == "entered"
    assert observations.beacon_script == "user_setup.sh"
    assert observations.beacon_at == "2026-08-05T21:39:52Z"
    # Joined with + by the shell, which the query parser decodes to spaces.
    assert observations.beacon_variables == [
        "VOC_DB_GROUP_NAME",
        "VOC_DB_USER_EMAIL",
    ]


def test_a_url_carrying_some_other_fragment_yields_nothing() -> None:
    """The workspace deep-link form is a fragment too, and it is not a beacon."""
    assert beacon_fields(f"{WORKSPACE}/#workspace/Users/{STUDENT}") == {}
    assert beacon_fields("") == {}
    assert beacon_fields(f"{WORKSPACE}/#other=1&voc-diag=entered") == {}


def test_an_entry_beacon_reports_a_hook_that_did_not_finish() -> None:
    """Vocareum invoked it, and neither the landing URL nor voc_fail overwrote it."""
    observations = healthy()
    observations.beacon_state = "entered"
    observations.beacon_script = "user_setup.sh"
    assert "A lifecycle script started and did not finish" in labels(observations)


def test_a_failure_beacon_reports_the_code_it_carries() -> None:
    observations = healthy()
    observations.beacon_state = "MISSING_WORKSPACE_URL"
    observations.beacon_script = "user_setup.sh"
    found = labels(observations)
    assert "A lifecycle script failed and said so through the beacon" in found
    assert "A lifecycle script started and did not finish" not in found


def test_no_beacon_reports_neither() -> None:
    """Absence is the ordinary case for a hook that ran to completion."""
    found = labels(healthy())
    assert "A lifecycle script started and did not finish" not in found
    assert "A lifecycle script failed and said so through the beacon" not in found


def test_an_unusable_host_becomes_blank_rather_than_raising() -> None:
    """The Vocareum half of the diagnostic still works without a workspace URL.

    A record that names no workspace, or names one over plain http, should not
    stop the tool from reporting what Vocareum thinks happened.
    """
    assert normalize_host_or_blank(f"{WORKSPACE}/?o=1") == WORKSPACE
    assert normalize_host_or_blank("") == ""
    assert normalize_host_or_blank("   ") == ""
    assert normalize_host_or_blank("http://insecure.example.com") == ""


@pytest.mark.parametrize(
    "raw",
    ["not a url", "https://dbc 33333333.cloud.databricks.com", "https://host:port"],
)
def test_a_host_that_is_not_a_hostname_is_refused(raw: str) -> None:
    """urlsplit puts anything before the first slash into netloc, spaces included.

    So a malformed value would normalize into something that looks like a host,
    satisfy guard_host by comparing equal to itself, and fail several calls later
    at the SDK with a connection error naming no cause. It is refused here
    instead, and normalize_host_or_blank turns that refusal into the empty
    string, which is the value the caller already handles as "unset".
    """
    assert normalize_host_or_blank(raw) == ""


def test_a_field_is_found_wherever_it_sits() -> None:
    """The record's nesting is not stable enough to address by a fixed path."""
    record = {
        "labs": [
            {
                "databricks": {"workspace_url": WORKSPACE, "userEmail": STUDENT},
                "group_name": "voc-group-200000-student1",
            }
        ]
    }
    assert find_field(record, "workspace_url") == (
        "labs[0].databricks.workspace_url",
        WORKSPACE,
    )
    # Matched with underscores stripped, because the record mixes snake_case and
    # camelCase in the same object.
    assert find_field(record, "user_email") == (
        "labs[0].databricks.userEmail",
        STUDENT,
    )
    assert find_field(record, "notebook_url") is None


def test_an_empty_value_is_not_a_find() -> None:
    """A key present and blank is the same as absent, and worse to report."""
    assert find_field({"notebook_url": ""}, "notebook_url") is None
    assert find_field({"notebook_url": None}, "notebook_url") is None


CREDENTIALS_FOUND = "~/.databrickscfg databricks-cli curl python3"


def custom_data_record(**fields: str) -> dict[str, object]:
    """The session record with voccustomdata.txt nested somewhere inside it.

    The path is a guess, because the Databricks record's shape is undocumented.
    That is exactly why the search is by field name, and it is what these tests
    pin: the nesting is free to change and the reporting is not.
    """
    return {"resources": [{"databricks": [dict(fields)]}]}


def test_a_successful_run_reports_what_the_script_said_about_itself() -> None:
    """The three keys Vocareum itself reads nothing from, so nothing used to.

    Only error_code and message are acted on by Vocareum, so a clean run left
    what ran and what it could reach in the record and out of the output.
    """
    observations = Observations()
    report_custom_data(
        custom_data_record(
            script="user_setup.sh",
            ran_at="2026-08-05T12:00:00Z",
            credentials_found=CREDENTIALS_FOUND,
        ),
        observations,
    )
    assert observations.script_name == "user_setup.sh"
    assert observations.script_ran_at == "2026-08-05T12:00:00Z"
    assert observations.credentials_found == CREDENTIALS_FOUND


def test_an_absent_credentials_found_is_printed_rather_than_skipped(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Absence is the finding here, unlike an absent error_code.

    Either user_setup.sh did not run or Vocareum does not carry
    voccustomdata.txt in the record at all, and those call for opposite next
    steps. A skipped line reports the second as silence.
    """
    observations = Observations()
    report_custom_data({"state": "Running"}, observations)
    printed = capsys.readouterr().out
    assert "credentials_found not present" in printed
    assert observations.credentials_found == ""


def test_a_credential_shaped_value_is_still_masked_in_what_is_recorded() -> None:
    """The name is exempt from redaction. The value is not."""
    token = "dapi" + "0123456789abcdef" * 2
    observations = Observations()
    report_custom_data(
        custom_data_record(credentials_found=f"DATABRICKS_TOKEN={token}"),
        observations,
    )
    assert token not in observations.credentials_found


def test_flatten_keys_addresses_list_members() -> None:
    assert flatten_keys({"a": [{"b": 1}, {"b": 2}]}) == {"a[0].b": 1, "a[1].b": 2}


def test_an_empty_value_is_a_find_when_presence_is_the_question() -> None:
    """Present and empty is the observation that says a lab never started."""
    assert find_field_entry({"state": ""}, "state") == ("state", "")
    assert find_field_entry({"state": None}, "state") == ("state", None)
    assert find_field_entry({"other": "x"}, "state") is None


# A real listing, as returned by GET .../files?dir=/voc/scripts&list=true, with
# its identifiers replaced. Names arrive as bare strings under "files", so the
# flattened paths are files[0] upward and carry no field name anywhere.
REAL_LISTING = {
    "courseid": "200001",
    "assignmentid": "5000001",
    "partid": "5000002",
    "directory": "/voc/scripts",
    "files": [
        "grade.sh",
        "lab_end.sh",
        "lab_setup.sh",
        "user_setup.sh",
        "voclib.sh",
        "workspace_init.sh",
    ],
}


def test_a_listing_of_bare_names_is_read() -> None:
    """The shape the endpoint actually returns, which reported empty before."""
    assert script_names_from_listing(REAL_LISTING) == [
        "grade.sh",
        "lab_end.sh",
        "lab_setup.sh",
        "user_setup.sh",
        "voclib.sh",
        "workspace_init.sh",
    ]


def test_the_directory_itself_is_not_read_as_a_file() -> None:
    """The directory field would otherwise arrive as a file called scripts."""
    assert "scripts" not in script_names_from_listing(REAL_LISTING)


def test_a_listing_of_objects_is_read_without_sibling_fields() -> None:
    """The other shape this endpoint has returned, without picking up a size."""
    payload = {"files": [{"name": "user_setup.sh", "size": "2048"}]}
    assert script_names_from_listing(payload) == ["user_setup.sh"]


def test_a_zip_transaction_response_names_no_files() -> None:
    """What the same path answers without list=true, which is not a listing."""
    payload = {
        "status": "success",
        "message": "Started",
        "transactionid": "100001",
        "objid": "4000003",
    }
    assert script_names_from_listing(payload) == []


# A real record for an enrolled student whose session has never run, with its
# identifiers replaced. It carries no credential in this state.
NEVER_STARTED_RECORD = {
    "status": "success",
    "resources": [
        {
            "courseid": "200001",
            "assignmentid": "5000001",
            "partid": "5000002",
            "userid": "5000100",
            "sessionstart": "",
            "sessionend": "",
            "totaltime": "0",
            "type": "databricks",
            "state": "",
            "databricks": [],
        }
    ],
}


def test_a_never_started_session_is_recognised() -> None:
    assert session_has_started(NEVER_STARTED_RECORD) is False


def test_a_started_session_is_recognised() -> None:
    record = {"resources": [{"sessionstart": "Aug-05-2026 14:22", "state": "active"}]}
    assert session_has_started(record) is True


def test_a_record_without_the_fields_answers_unknown() -> None:
    """Unknown is not no, and reporting it as no invents a finding."""
    assert session_has_started({"resources": [{"userid": "5000100"}]}) is None


# ---------------------------------------------------------------------------
# Step 5C: the course's own objects
# ---------------------------------------------------------------------------
#
# The gate that replaces reading voccustomdata.txt. The Databricks session
# record was measured to carry no voccustomdata (measured on one part), so a
# hook cannot
# report its own failure and the workspace is the only remaining evidence. What
# these pin down is the exit code, because everything else in this tool reports
# and this one judges.


def with_manifest(tmp_path: Path, body: object) -> argparse.Namespace:
    """Build the arguments a --expect run is driven by."""
    path = tmp_path / "expected.json"
    path.write_text(json.dumps(body), encoding="utf-8")
    return argparse.Namespace(expect=path)


def test_an_expected_object_that_is_there_is_recorded_as_present(
    tmp_path: Path,
) -> None:
    client = ExpectStub(
        {"/api/2.1/unity-catalog/catalogs/workshop": {"name": "workshop"}}
    )
    observations = Observations()

    read_expectations(
        client, with_manifest(tmp_path, {"catalogs": ["workshop"]}).expect, observations
    )

    assert observations.expected_present == ["catalog workshop"]
    assert observations.expected_missing == []
    assert observations.expectations_checked is True


def test_an_expected_object_that_is_absent_becomes_a_finding(tmp_path: Path) -> None:
    observations = Observations()

    read_expectations(
        ExpectStub({}),
        with_manifest(tmp_path, {"catalogs": ["workshop"]}).expect,
        observations,
    )

    assert observations.expected_missing == ["catalog workshop"]
    assert "The course expects objects that are not in this workspace" in labels(
        observations
    )


def test_a_manifest_checked_against_no_workspace_concludes_nothing(
    tmp_path: Path,
) -> None:
    """The false pass this guard exists for.

    A workspace that was never reached leaves every list empty, and empty reads
    as nothing missing. Without ``expectations_checked`` the gate would pass
    hardest exactly when it measured least.
    """
    observations = Observations()

    read_expectations(
        None, with_manifest(tmp_path, {"catalogs": ["workshop"]}).expect, observations
    )

    assert observations.expectations_checked is False
    assert observations.expected_missing == []
    assert "The course expects objects that are not in this workspace" not in labels(
        observations
    )


def test_a_manifest_that_will_not_parse_leaves_the_run_unchecked(
    tmp_path: Path,
) -> None:
    path = tmp_path / "expected.json"
    path.write_text("{not json", encoding="utf-8")
    observations = Observations()

    read_expectations(ExpectStub({}), path, observations)

    assert observations.expectations_checked is False


def test_an_object_that_could_not_be_looked_up_is_reported_separately() -> None:
    """Absent and unreadable send a reader to two different places."""
    observations = Observations(expected_unchecked=["catalog workshop"])

    assert "Some expected objects could not be looked up at all" in labels(observations)
    assert "The course expects objects that are not in this workspace" not in labels(
        observations
    )


def test_the_gate_passes_only_when_every_object_read_back() -> None:
    assert not gate_failed(
        Observations(expectations_checked=True, expected_present=["catalog workshop"])
    )


@pytest.mark.parametrize(
    "observations",
    [
        Observations(expectations_checked=True, expected_missing=["catalog workshop"]),
        Observations(
            expectations_checked=True, expected_unchecked=["catalog workshop"]
        ),
        Observations(expectations_checked=False),
    ],
    ids=["absent", "unreadable", "never ran"],
)
def test_the_gate_fails_three_ways(observations: Observations) -> None:
    """Absent, unreadable, and never ran. Only the first is about the lab."""
    assert gate_failed(observations)
