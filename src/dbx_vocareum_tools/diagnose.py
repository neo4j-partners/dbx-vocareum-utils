"""Recover why a Vocareum Databricks lab did not come up, without console access.

The AWS version of this tool reads CloudFormation events. A Databricks lab has
no CloudFormation, and that difference decides everything below. Vocareum runs
four shell scripts from ``/voc/scripts`` as a student's session opens, and it
does not expose their standard output through the REST API. When one of them
fails, three things are left to read:

1. Whatever the script wrote into ``voccustomdata.txt`` before exiting. Vocareum
   reads ``error_code`` and ``message`` out of that file on a non-zero exit and
   shows them to the student. On a Databricks part the session record was
   measured not to carry them, which is why the steps below do not rely on it.
2. Whatever the script left behind in the Databricks workspace. A folder that
   exists, a group the student did not get added to, a cluster that was never
   created: the workspace is the log.
3. Whether ``vocipcdata.txt`` ever received a ``notebook_url``. A student who
   lands on a bare workspace rather than on the workshop notebook has a
   ``user_setup.sh`` that either exited early or never wrote that file.

So this tool walks six steps and prints a labeled section for each:

1. Read the part configuration and confirm its labtype is Databricks.
2. List ``/voc/scripts`` and report which lifecycle hooks are actually deployed.
3. Optionally start the lab session and follow its transaction.
4. Read the session record, report which fields it carried and where they sat,
   and pull out the workspace URL, the lab user, the group, and the notebook URL.
   Then read Vocareum's own workspace record for the part, which is the one
   place Databricks reports a failure back to Vocareum in words.
5. Read the workspace side with the service principal: does that user exist, is
   it in that group, what entitlements does it hold, and what compute, secret
   scopes, and folders exist under its name. With ``--principals``, also compare
   the account and workspace identity planes for this session's user and group,
   which is what separates a principal that is genuinely absent from one that
   the caller may not see.
6. Interpret the gap between steps 4 and 5 against a table of known causes.

Step 3 is opt-in rather than the default, which is the opposite of the AWS
tool. Starting a Databricks lab session runs ``user_setup.sh``, which is what
creates billable compute, so a diagnostic that starts one by accident is
expensive in a way its AWS counterpart is not.

Every response body this script prints is redacted first. Nothing in a Vocareum
session record belongs in a terminal transcript unfiltered: the AWS variant
carries temporary credentials, a federated console sign-in token, and an
unencrypted SSH private key, and the shape of the Databricks variant is
undocumented, so it is treated as though it carries the same.

This command is part of the repository's uv project::

    uv run dbx-vocareum-diagnose
    uv run dbx-vocareum-diagnose --start
    uv run dbx-vocareum-diagnose --raw-record
    uv run dbx-vocareum-diagnose --principals
    uv run dbx-vocareum-diagnose --expect expected.json

Configuration comes from a ``.env`` file in the repository root, or from real
environment variables, which always win. See ``.env.example``.

``--expect`` is the one flag that makes this tool judge rather than report, and
it exists because of what step 4 above cannot do. The Databricks session
record was measured to carry no ``voccustomdata`` (measured on one part), so
``error_code`` and ``message`` may have no channel back. What a hook built is
therefore read from the workspace instead of from anything the hook said, and a
course's manifest is what names it. See ``expect.py``.

Exit codes:
    0  The diagnostic ran and printed what it found.
    1  Configuration error.
    2  The Vocareum API or Databricks calls could not complete the diagnostic.
    3  --expect was given and something it named did not read back. Only ever
       returned when that flag is passed.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import DatabricksError
from requests.exceptions import RequestException

from .client import (
    TERMINAL_SUCCESS_STATES,
    TRANSACTION_POLL_SECONDS,
    TRANSACTION_TIMEOUT_SECONDS,
    ApiError,
    find_transaction_id,
    poll_transaction,
    request_json,
    wait_for_payload,
)
from .config import (
    VOCAREUM_SESSION_VARS,
    ConfigError,
    Environment,
    repository_root,
)
from .databricks import (
    BEACON_ENTERED,
    BEACON_KEY,
    CUSTOM_DATA_FAILURE_KEYS,
    CUSTOM_DATA_REPORT_KEYS,
    DEFAULT_ACCOUNT_HOST,
    IPC_NOTEBOOK_KEY,
    LIFECYCLE_SCRIPTS,
    SCRIPTS_TARGET,
    SESSION_RECORD_DATABRICKS_KEY,
    SESSION_RECORD_FIELDS,
    DatabricksHostError,
    beacon_fields,
    find_field,
    find_field_entry,
    flatten_keys,
    normalize_host,
    principal_created_at,
    script_names_from_listing,
    workspace_id_from_url,
)
from .endpoints import DEFAULT_API_BASE, OrgEndpoints, SessionEndpoints
from .expect import (
    ManifestError,
    Presence,
    Result,
    check_all,
    load_manifest,
)
from .output import dump_json, rule, section
from .probe import (
    Config as DatabricksConfig,
)
from .probe import (
    GuardError,
    Verdict,
    build_account_client,
    build_workspace_client,
    classify,
    error_pair,
    guard_credentials,
    guard_host,
    guard_identity,
)
from .security import redact_payload, redact_text, safe_url

EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_API = 2
# Only ever returned when --expect names a manifest and something in it did not
# read back. A run without the flag still exits 0 whatever it finds, because
# every other step of this tool reports rather than judges, and a diagnostic
# that started failing CI on a finding it has always printed would be a change
# nobody asked for.
EXIT_EXPECTATIONS_UNMET = 3

# A freshly started Databricks session has to create or claim a workspace seat,
# add the user, and run user_setup.sh before the record carries a notebook URL.
# The poll is slow because nothing is gained by asking more often.
RECORD_POLL_SECONDS = 10
RECORD_TIMEOUT_SECONDS = 300

MAX_LISTED = 25

# The values Vocareum puts in ``state`` for a session that did not come up.
# Compared case-insensitively, because the field is prose Vocareum does not
# document and a capitalisation change should not silently disable the finding.
FAILED_SESSION_STATES = frozenset({"failed", "error", "terminated_with_errors"})


@dataclass(frozen=True)
class Config(SessionEndpoints):
    """Everything the diagnostic needs, resolved from the environment."""

    api_key: str
    host: str
    account_host: str
    account_id: str
    client_id: str
    client_secret: str
    expected_workspace_id: str
    # Optional. Without it the org-scoped workspace record cannot be asked for,
    # and every other step still runs. Required would make the whole diagnostic
    # refuse to start over one endpoint.
    org_number: str = ""

    def org_endpoints(self) -> OrgEndpoints:
        """Return the organization-scoped paths for this configuration."""
        return OrgEndpoints(api_base=self.api_base, org_number=self.org_number)

    def databricks_config(self) -> DatabricksConfig:
        """Return the Databricks half in the shape the shared guards expect.

        The guards live in ``probe`` and are reused verbatim rather than
        reimplemented, because a diagnostic that reads the wrong workspace is
        exactly as wrong as a probe that measures one.
        """
        return DatabricksConfig(
            host=self.host,
            account_host=self.account_host,
            account_id=self.account_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            lab_user_token="",
            expected_workspace_id=self.expected_workspace_id,
            source="service-principal",
        )


@dataclass
class Observations:
    """What the run learned, in the order it learned it."""

    labtype: str = ""
    end_lab_behavior: str = ""
    deployed_scripts: list[str] = field(default_factory=list)
    # Whether step 2 actually read a listing. Without this, an unreadable
    # listing and an empty directory are the same observation, and the finding
    # they produce tells the reader to upload scripts that may already be
    # deployed. It matches the role workspace_reached plays for step 5.
    scripts_listed: bool = False
    # None until the record has been read: True if a session has run, False if
    # the record says one never has. A never-started session explains an absent
    # notebook URL completely, and blaming user_setup.sh for it is wrong.
    session_started: bool | None = None
    session_state: str = ""
    workspace_url: str = ""
    lab_user_email: str = ""
    group_name: str = ""
    notebook_url: str = ""
    script_error_code: str = ""
    script_message: str = ""
    # What a script that succeeded said about itself, as against the two above,
    # which Vocareum fills in only for one that failed. credentials_found is the
    # measurement the initial lab template exists to take: it names what a
    # lifecycle script can reach, and nothing can be written that calls the
    # Databricks API until it has an answer.
    script_name: str = ""
    script_ran_at: str = ""
    credentials_found: str = ""
    # What voc_ipc_beacon wrote into notebook_url, if anything. The one channel
    # a lifecycle script controls that has been observed reaching the session
    # record, so these are the only fields that say whether a hook ran at all.
    beacon_state: str = ""
    beacon_script: str = ""
    beacon_at: str = ""
    beacon_variables: list[str] = field(default_factory=list)
    # What Vocareum's own workspace record says, which is where the Databricks
    # side reports its failures back to Vocareum. Held apart from the session
    # record because a workspace belongs to the part rather than to a student,
    # so these describe every student on the part rather than only this one.
    vocareum_workspace_status: str = ""
    vocareum_workspace_error: str = ""
    vocareum_num_users: str = ""
    user_found: bool | None = None
    user_groups: list[str] = field(default_factory=list)
    user_entitlements: list[str] = field(default_factory=list)
    clusters_for_user: list[str] = field(default_factory=list)
    secret_scopes: list[str] = field(default_factory=list)
    user_folder_exists: bool | None = None
    workspace_reached: bool = False
    # What the account plane says about the principals Vocareum built. All
    # tri-state, because "not measured" has to stay distinct from "measured and
    # absent": the account API answers a caller with no permission by saying the
    # object does not exist, so an absence is only evidence once the same
    # caller has been shown to read that plane at all.
    account_reached: bool = False
    workspace_federated: bool | None = None
    user_in_account: bool | None = None
    group_in_account: bool | None = None
    user_has_assignment: bool | None = None
    group_has_assignment: bool | None = None
    # The same id read from both planes. Equal ids are what identity federation
    # looks like from outside; different ids mean two directories and a
    # workspace principal that no account-level call can address.
    account_user_id: str = ""
    workspace_user_id: str = ""
    account_group_id: str = ""
    workspace_group_id: str = ""
    # What --expect found, as three disjoint lists of "kind name" strings. Three
    # rather than a pass flag, because "absent" and "could not be read" send a
    # reader to two different places, and a gate that reported them as one
    # number would be the same gate the beacon already failed to be.
    expected_present: list[str] = field(default_factory=list)
    expected_missing: list[str] = field(default_factory=list)
    expected_unchecked: list[str] = field(default_factory=list)
    # Whether the manifest was read and its lookups were run at all. Without
    # this, a manifest that fails to parse and a workspace that was never
    # reached both leave the three lists empty, and empty reads as "nothing
    # missing", so the gate would pass hardest exactly when it measured least.
    expectations_checked: bool = False


@dataclass(frozen=True)
class Finding:
    """One interpretation of the gap between what was asked for and what exists."""

    label: str
    detail: str
    guidance: str


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def load_config(env_path: Path) -> Config:
    """Build a :class:`Config` from real environment variables and ``.env``."""
    environment = Environment.from_file(env_path)
    try:
        environment.require(VOCAREUM_SESSION_VARS)
    except ConfigError as error:
        raise ConfigError(
            f"{error}\nCopy .env.example to .env and fill in the values, "
            "or export them in your shell."
        ) from error

    raw_host = environment.get("DATABRICKS_HOST")
    raw_account_host = (
        environment.get("DATABRICKS_ACCOUNT_HOST") or DEFAULT_ACCOUNT_HOST
    )
    try:
        # An absent workspace URL is tolerated. The Vocareum half of the
        # diagnostic still works without it, and reporting what Vocareum thinks
        # happened is useful on its own.
        host = normalize_host(raw_host) if raw_host else ""
        account_host = normalize_host(raw_account_host)
    except DatabricksHostError as error:
        raise ConfigError(str(error)) from error

    return Config(
        api_key=environment.get("VOCAREUM_API_KEY"),
        course_id=environment.get("VOCAREUM_COURSE_ID"),
        assignment_id=environment.get("VOCAREUM_ASSIGNMENT_ID"),
        part_id=environment.get("VOCAREUM_PART_ID"),
        user_id=environment.get("VOCAREUM_USER_ID"),
        api_base=(environment.get("VOCAREUM_API_BASE") or DEFAULT_API_BASE).rstrip("/"),
        host=host,
        account_host=account_host,
        account_id=environment.get("DATABRICKS_ACCOUNT_ID"),
        client_id=environment.get("DATABRICKS_CLIENT_ID"),
        client_secret=environment.get("DATABRICKS_CLIENT_SECRET"),
        expected_workspace_id=environment.get("DATABRICKS_WORKSPACE_ID"),
        org_number=environment.get("VOCAREUM_ORG_NUMBER"),
    )


# ---------------------------------------------------------------------------
# Step 1: the part
# ---------------------------------------------------------------------------


def read_part(config: Config, observations: Observations) -> None:
    """Read the part configuration and report what kind of lab it is.

    A part whose labtype is not Databricks explains every later step returning
    nothing, and it is the one misconfiguration that produces a lab which starts
    successfully and does the wrong thing.
    """
    section("STEP 1: The part")
    print(f"GET {config.part_url()}")
    payload = request_json("GET", config.part_url(), config.api_key)

    labtype = find_field(payload, "labtype")
    if labtype is None:
        print("The part record names no labtype.")
    else:
        observations.labtype = str(labtype[1])
        print(f"labtype = {observations.labtype}   (at {labtype[0]})")
        if observations.labtype.lower() != "databricks":
            print(
                "  This part is not a Databricks lab. The lifecycle scripts in "
                "/voc/scripts will not run."
            )

    behavior = find_field(payload, "endlabbehavior") or find_field(
        payload, "end_lab_behavior"
    )
    if behavior is not None:
        observations.end_lab_behavior = str(behavior[1])
        print(f"end lab behavior = {observations.end_lab_behavior}")

    for fragment in ("maxusers", "labid", "duration"):
        found = find_field(payload, fragment)
        if found is not None:
            print(f"{fragment} = {found[1]}   (at {found[0]})")


# ---------------------------------------------------------------------------
# Step 2: the scripts
# ---------------------------------------------------------------------------


def read_scripts(config: Config, observations: Observations) -> None:
    """List ``/voc/scripts`` and say which lifecycle hooks are deployed.

    A hook that is not there does not fail. It is skipped in silence, and the
    lab that results is missing a stage rather than broken, which is the harder
    thing to notice.
    """
    section("STEP 2: The lifecycle scripts")
    # list=true is what selects the listing. The same path without it is the
    # download-the-directory-as-zip call, which answers with a transaction id,
    # names no files at all, and starts a packaging job as a side effect. Read
    # that way every hook reads as absent however much is deployed, so the
    # parameter is not a refinement of this call, it is the call.
    url = config.files_url({"dir": f"/voc/{SCRIPTS_TARGET}", "list": "true"})
    print(f"GET {url}")

    try:
        payload = request_json("GET", url, config.api_key)
    except ApiError as error:
        print(f"Could not list /voc/{SCRIPTS_TARGET}: {error}")
        return

    names = script_names_from_listing(payload)
    if not names and find_field(payload, "transactionid") is not None:
        print(
            "The response is a zip-download transaction rather than a listing, "
            "so it names no files. Nothing below could be reported as deployed "
            "whatever is really there, so this step is reporting nothing rather "
            "than reporting everything absent."
        )
        return

    observations.deployed_scripts = names
    observations.scripts_listed = True
    print(f"Files in /voc/{SCRIPTS_TARGET}: {', '.join(names) or '(none)'}")
    rule()

    for script in LIFECYCLE_SCRIPTS:
        present = script.name in names
        if present:
            print(f"  {script.name:20s} deployed, runs {script.when}")
            continue
        mark = "MISSING, required" if script.required else "absent, optional"
        print(f"  {script.name:20s} {mark}")
        if script.required:
            print(
                "    Without it Vocareum has nothing to run when a student "
                "starts the lab."
            )


# ---------------------------------------------------------------------------
# Step 3: the session
# ---------------------------------------------------------------------------


def start_session(config: Config) -> None:
    """Start or extend the lab session and follow its transaction."""
    section("STEP 3: Start the session")
    print(f"PUT {config.resource_url()}")
    payload = request_json("PUT", config.resource_url(), config.api_key)
    print("Session start requested.")

    transaction_id = find_transaction_id(payload)
    if transaction_id is None:
        print("No transaction ID came back; reading the record directly.")
        return

    result = poll_transaction(
        config.transaction_url(transaction_id),
        config.api_key,
        poll_seconds=TRANSACTION_POLL_SECONDS,
        timeout_seconds=TRANSACTION_TIMEOUT_SECONDS,
        on_state=lambda state: print(f"  transaction state={state}"),
    )
    if result.timed_out:
        print(
            f"Still pending after {TRANSACTION_TIMEOUT_SECONDS}s. Reading the "
            "record anyway; a slow start and a failed one look the same from here."
        )
        return
    if result.state.lower() not in TERMINAL_SUCCESS_STATES:
        message = ""
        if isinstance(result.payload, dict) and result.payload.get("message"):
            message = f" Message: {redact_text(str(result.payload['message']))}"
        print(f"Session start ended in state {result.state!r}.{message}")
        return
    print(f"Session start reached state {result.state!r}.")


# ---------------------------------------------------------------------------
# Step 4: the session record
# ---------------------------------------------------------------------------


def read_beacon(observations: Observations) -> None:
    """Decode a ``voc_ipc_beacon`` URL, if the notebook URL is one.

    A landing URL and a beacon arrive through the same field, so the fragment
    is what tells them apart: ``beacon_fields`` returns nothing for a URL that
    does not carry ``voc-diag``, and an ordinary landing URL says the hook ran
    all the way through. Silence here is therefore not a failure to read.

    ``vars`` holds names joined with ``+``, which the query parser decodes to
    spaces. Splitting on whitespace is what turns it back into a list, and the
    names are all this reports because a value in this field would be a
    credential in the session record.
    """
    fields = beacon_fields(observations.notebook_url)
    if not fields:
        return
    observations.beacon_state = fields.get(BEACON_KEY, "")
    observations.beacon_script = fields.get("script", "")
    observations.beacon_at = fields.get("at", "")
    observations.beacon_variables = fields.get("vars", "").split()
    print(
        f"  beacon        {observations.beacon_script or 'unknown script'} "
        f"reported {observations.beacon_state or '(no state)'} "
        f"at {observations.beacon_at or 'an unrecorded time'}"
    )
    if observations.beacon_variables:
        print(
            f"    it was handed {len(observations.beacon_variables)} VOC_ "
            f"variable(s): {' '.join(observations.beacon_variables)}"
        )


def read_record(
    config: Config, observations: Observations, *, wait: bool, raw: bool
) -> None:
    """Read the session record and pull out what it says about the lab.

    The record's shape for a Databricks part is undocumented, so every field is
    searched for by name at whatever depth it sits and reported with the path it
    was found at. The path is the useful half: it turns a one-off discovery into
    something the next reader can address directly.
    """
    section("STEP 4: The session record")
    print(f"GET {config.resource_url()}")

    payload, _ = wait_for_payload(
        lambda: request_json("GET", config.resource_url(), config.api_key),
        lambda body: find_field(body, IPC_NOTEBOOK_KEY),
        poll_seconds=RECORD_POLL_SECONDS,
        timeout_seconds=RECORD_TIMEOUT_SECONDS,
        wait=wait,
        on_retry=lambda seconds: print(
            f"  no notebook URL in the record yet; retrying in {seconds}s"
        ),
    )

    state = find_field_entry(payload, "state")
    if state is not None:
        observations.session_state = str(state[1] or "")
        shown = observations.session_state or "(empty)"
        print(f"state = {shown}   (at {state[0]})")

    observations.session_started = session_has_started(payload)
    if observations.session_started is False:
        print(
            "This session has never started: sessionstart and state are both "
            "empty. Every field below that a running lab fills in is empty for "
            "that reason rather than because something failed."
        )

    rule()
    print("Expected fields:")
    for fragment in SESSION_RECORD_FIELDS:
        found = find_field_entry(payload, fragment)
        if found is None:
            print(f"  {fragment:16s} not present")
            continue
        path, value = found
        if value is None or (isinstance(value, str) and not value.strip()):
            print(f"  {fragment:16s} present but empty   (at {path})")
            continue
        printed = safe_url(str(value)) if "url" in fragment else value
        print(f"  {fragment:16s} {printed}   (at {path})")

    # Counted by path segment rather than by substring, because the container
    # nests: a real field sits at resources[0].databricks[0].something, and any
    # prefix test on the whole path answers about resources instead.
    databricks_fields = [
        path
        for path in flatten_keys(payload)
        if any(
            segment.split("[", 1)[0] == SESSION_RECORD_DATABRICKS_KEY
            for segment in path.split(".")
        )
    ]
    print(
        f"  {SESSION_RECORD_DATABRICKS_KEY:16s} "
        f"{len(databricks_fields)} field(s) inside it"
    )

    workspace = find_field(payload, "workspace_url")
    if workspace is not None:
        observations.workspace_url = normalize_host_or_blank(str(workspace[1]))
    user_email = find_field(payload, "user_email")
    if user_email is not None:
        observations.lab_user_email = str(user_email[1])
    group = find_field(payload, "group_name")
    if group is not None:
        observations.group_name = str(group[1])
    notebook = find_field(payload, IPC_NOTEBOOK_KEY)
    if notebook is not None:
        observations.notebook_url = str(notebook[1])
        read_beacon(observations)

    for key in CUSTOM_DATA_FAILURE_KEYS:
        found = find_field(payload, key)
        if found is None:
            continue
        if key == "error_code":
            observations.script_error_code = str(found[1])
        else:
            observations.script_message = redact_text(str(found[1]))
        print(f"  script {key:9s} {redact_text(str(found[1]))}   (at {found[0]})")

    report_custom_data(payload, observations)

    # Gated on the notebook URL alone, because that is the only field the check
    # reads. Requiring a workspace_url as well would suppress the comparison for
    # a record that names the workspace only inside the notebook URL, which is a
    # shape this undocumented record is free to take.
    if observations.notebook_url:
        reached = workspace_id_from_url(observations.notebook_url)
        if reached:
            print(f"  notebook URL points at workspace {reached}")
            if (
                config.expected_workspace_id
                and reached != config.expected_workspace_id.strip()
            ):
                print(
                    f"    That is not DATABRICKS_WORKSPACE_ID "
                    f"({config.expected_workspace_id}). Vocareum put this "
                    "student in a different workspace than the tools are "
                    "pointed at, so step 5 describes the wrong environment."
                )

    if raw:
        rule()
        print("Full record, redacted:")
        print(dump_json(redact_payload(payload)))


# Which observation each report key lands in. The attribute names follow the
# script_* family the failure keys already write to, so a reader of the JSON sees
# one set of fields about the script rather than two.
CUSTOM_DATA_REPORT_ATTRIBUTES = {
    "script": "script_name",
    "ran_at": "script_ran_at",
    "credentials_found": "credentials_found",
}


def report_custom_data(payload: Any, observations: Observations) -> None:
    """Report what ``voccustomdata.txt`` says about the run that wrote it.

    Not only a run that succeeded. ``voc_fail`` writes ``script`` and ``ran_at``
    alongside the failure keys, so these fields describe a failed run too, and a
    record with neither describes no run at all. A record carrying ``script``
    proves the hook ran and reached the point that wrote it, and a record
    carrying nothing proves it did not, which is the difference between fixing
    the script and fixing the lab configuration.

    Absence is printed rather than skipped, which is the opposite of how the
    failure keys are treated, and for the same reason. A record with no
    ``error_code`` is the ordinary case and says nothing worth a line. A record
    with no ``credentials_found`` is the finding: either ``user_setup.sh`` did not
    run, or Vocareum does not carry ``voccustomdata.txt`` in the session record at
    all, and those call for opposite next steps. Skipping the line would report
    the second as silence.

    Values still go through :func:`redact_text`, because the exemption these
    field names carry in ``security`` is on the name alone. A value shaped like a
    Databricks token is masked whatever the field it arrived in is called.
    """
    for key in CUSTOM_DATA_REPORT_KEYS:
        found = find_field(payload, key)
        if found is None:
            print(f"  script {key:16s} not present")
            continue
        path, value = found
        shown = redact_text(str(value))
        setattr(observations, CUSTOM_DATA_REPORT_ATTRIBUTES[key], shown)
        print(f"  script {key:16s} {shown}   (at {path})")


def session_has_started(payload: Any) -> bool | None:
    """Say whether the record shows a session that has ever run.

    Returns ``None`` when the record does not carry the fields to decide it,
    which is not the same answer as no. Vocareum creates the record before any
    session exists and fills ``sessionstart`` and ``state`` when one runs, so
    both present and both empty is a positive statement that nothing has
    started, rather than an absence of information.
    """
    start = find_field_entry(payload, "sessionstart")
    state = find_field_entry(payload, "state")
    if start is None and state is None:
        return None
    return any(
        found is not None and str(found[1] or "").strip() for found in (start, state)
    )


# ---------------------------------------------------------------------------
# Step 4B: Vocareum's own workspace record
# ---------------------------------------------------------------------------

# What to pull out of the workspace record and print. status and error_info are
# read together on purpose: a workspace can read RUNNING and still have failed
# at adding a user, so status alone reports that everything is fine.
WORKSPACE_RECORD_FIELDS = (
    "workspace_name",
    "workspace_id",
    "workspace_url",
    "status",
    "num_users",
    "error_info",
    "region",
    "databricks_cloud_type",
    "init_script_id",
    "destroy_script_id",
)


def read_vocareum_workspace(config: Config, observations: Observations) -> None:
    """Read the workspace record Vocareum keeps for this part.

    This is where ``error_info`` lives, and it is the only place Databricks
    reports a failure back to Vocareum in words. Nothing else in the diagnostic
    reaches it: the session record carries the ids but no error, and the
    Databricks side has no idea a Vocareum call ever failed.

    The record is keyed by part rather than by student, so what it says applies
    to every student on the part. That is what makes it worth reading in every
    run rather than only when a session has failed.
    """
    section("STEP 4B: Vocareum's Databricks workspace record")
    if not config.org_number:
        print(
            "VOCAREUM_ORG_NUMBER is unset, so the workspace record was not "
            "read. It carries error_info, which is the only place Databricks "
            "reports a failure back to Vocareum in words, and the org path is "
            "the only shape that answers for it."
        )
        return

    url = config.org_endpoints().databricks_workspaces_url()
    print(f"GET {url}")
    payload = request_json("GET", url, config.api_key)

    records = payload.get("workspaces", []) if isinstance(payload, dict) else []
    if not isinstance(records, list):
        records = []
    mine = [
        record
        for record in records
        if isinstance(record, dict) and str(record.get("partid", "")) == config.part_id
    ]
    if not mine:
        print(
            f"None of the {len(records)} workspace record(s) names part "
            f"{config.part_id}. Vocareum has built no Databricks workspace for "
            "this part, which explains a session that cannot start at all."
        )
        return

    for record in mine:
        rule()
        for name in WORKSPACE_RECORD_FIELDS:
            value = str(record.get(name, "")).strip()
            shown = redact_text(value) if value else "(empty)"
            print(f"  {name:22s} {shown}")

    # The first record wins for the observations. A part with more than one is
    # unmeasured and would be worth knowing about, so it is printed above and
    # said plainly here rather than collapsed in silence.
    if len(mine) > 1:
        print(
            f"\n{len(mine)} records name this part. Only the first is carried "
            "into the findings below."
        )
    first = mine[0]
    observations.vocareum_workspace_status = str(first.get("status", "")).strip()
    observations.vocareum_workspace_error = redact_text(
        str(first.get("error_info", "")).strip()
    )
    observations.vocareum_num_users = str(first.get("num_users", "")).strip()

    if observations.vocareum_workspace_error:
        print(
            "\nerror_info carries no timestamp and names no principal, so "
            "nothing here says when this failure happened or whether it belongs "
            "to the session read above. Treat it as the most recent Databricks "
            "complaint on this workspace and no more than that."
        )


def normalize_host_or_blank(value: str) -> str:
    """Normalize a host, returning the empty string when it cannot be used."""
    try:
        return normalize_host(value)
    except DatabricksHostError:
        return ""


# ---------------------------------------------------------------------------
# Step 5: the workspace
# ---------------------------------------------------------------------------


def read_workspace(
    config: Config, observations: Observations
) -> WorkspaceClient | None:
    """Read what actually exists in the workspace, as the service principal.

    This is where the AWS tool would read CloudFormation events. There are none
    here, so the workspace state is the record of what the scripts did.

    Returns the guarded client so step 5B can reuse it. Building a second one
    would mean running the three guards twice, and a run that passed them the
    first time and failed the second would be reporting against two different
    workspaces in one report.
    """
    section("STEP 5: The workspace")
    if not config.host:
        print(
            "DATABRICKS_HOST is unset, so the workspace side was not read. "
            "Set it in .env to see what the lifecycle scripts actually built."
        )
        return None

    databricks = config.databricks_config()
    try:
        guard_credentials(databricks)
        client = build_workspace_client(databricks)
        guard_host(client, databricks)
        guard_identity(client, databricks)
    except (GuardError, DatabricksHostError) as error:
        print(f"\nThe workspace side was not read.\n{error}")
        return None

    observations.workspace_reached = True
    if observations.workspace_url and observations.workspace_url != config.host:
        print(
            f"\nNOTE: the session record names {observations.workspace_url} and "
            f"DATABRICKS_HOST is {config.host}. Everything below describes "
            f"{config.host}."
        )

    rule()
    read_lab_user(client, observations)
    rule()
    read_compute(client, observations)
    rule()
    read_secret_scopes(client, observations)
    rule()
    read_user_folder(client, observations)
    return client


@dataclass(frozen=True)
class CallOutcome:
    """What one workspace call returned, or why nothing came back."""

    payload: Any = None
    error_code: str = ""
    message: str = ""


def call_detail(
    client: WorkspaceClient | AccountClient, method: str, url_path: str, /, **query: Any
) -> CallOutcome:
    """Make one API call, keeping the reason a failure failed.

    A caller that treats every failure alike cannot tell an object that is not
    there from one it may not see, and on Databricks the first arrives as the
    same ``RESOURCE_DOES_NOT_EXIST`` as the second while a denial, a throttle,
    and an unreachable host arrive as different codes entirely. The code is
    carried out of here so the caller decides what it is evidence of.

    The first three parameters are positional-only because ``workspace/list``
    takes a query parameter named ``path``, which would otherwise collide with
    the parameter naming the endpoint and raise ``TypeError`` before any call
    went out.

    Either client is accepted. Both expose the same ``api_client.do``, and step
    5B needs the identical failure-versus-absence distinction on the account
    plane that steps 5 makes on the workspace plane; a second copy of this
    function for the other client type would be the same body with one
    annotation changed, and the two would drift.
    """
    try:
        payload = client.api_client.do(method, url_path, query=query or None)
    except DatabricksError as error:
        code, message = error_pair(error)
        print(f"  {method} {url_path} failed: {redact_text(message)}")
        return CallOutcome(error_code=code, message=message)
    except RequestException as error:
        print(f"  {method} {url_path} failed: {redact_text(str(error))}")
        return CallOutcome(message=str(error))
    return CallOutcome(payload=payload)


def call(client: WorkspaceClient, method: str, url_path: str, /, **query: Any) -> Any:
    """Make one workspace API call, returning ``None`` when it cannot be made.

    For the readings whose absence is reported as a count rather than as a
    verdict, where why the call failed changes nothing about what gets printed.
    """
    return call_detail(client, method, url_path, **query).payload


def read_lab_user(client: WorkspaceClient, observations: Observations) -> None:
    """Look the lab user up in the workspace and report its groups.

    Group membership is what Vocareum grants against. A student who exists but
    is in no group has a workspace identity with no permissions, which presents
    as a lab that starts and then refuses everything.
    """
    email = observations.lab_user_email
    if not email:
        print("No lab user email came back from the session record, so no lookup.")
        return

    print(f"Looking up {email}")
    response = call(
        client,
        "GET",
        "/api/2.0/preview/scim/v2/Users",
        filter=f'userName eq "{email}"',
    )
    if response is None:
        return

    resources = response.get("Resources", []) if isinstance(response, dict) else []
    if not resources:
        observations.user_found = False
        print("  Not found in this workspace.")
        return

    observations.user_found = True
    user = resources[0]
    print(f"  id          = {user.get('id', '(absent)')}")
    print(f"  displayName = {user.get('displayName', '(absent)')}")
    observations.user_groups = [
        str(item.get("display", item.get("value", "")))
        for item in user.get("groups", [])
        if isinstance(item, dict)
    ]
    observations.user_entitlements = [
        str(item.get("value", ""))
        for item in user.get("entitlements", [])
        if isinstance(item, dict)
    ]
    print(f"  groups      = {', '.join(observations.user_groups) or '(none)'}")
    print(f"  entitlements= {', '.join(observations.user_entitlements) or '(none)'}")

    expected_group = observations.group_name
    if expected_group and expected_group not in observations.user_groups:
        print(
            f"  The record names group {observations.group_name!r}, which this "
            "user is not in."
        )


def read_compute(client: WorkspaceClient, observations: Observations) -> None:
    """List clusters and report which ones belong to the lab user."""
    response = call(client, "GET", "/api/2.0/clusters/list")
    if response is None:
        return
    clusters = response.get("clusters", []) if isinstance(response, dict) else []
    print(f"Clusters visible: {len(clusters)}")

    email = observations.lab_user_email
    for cluster in clusters[:MAX_LISTED]:
        creator = str(cluster.get("creator_user_name", ""))
        name = str(cluster.get("cluster_name", ""))
        state = str(cluster.get("state", ""))
        mine = bool(email) and (creator == email or email in name)
        if mine:
            observations.clusters_for_user.append(name)
        print(f"  {'*' if mine else ' '} {name}  {state}  created by {creator}")
    if len(clusters) > MAX_LISTED:
        print(f"  ... {len(clusters) - MAX_LISTED} more not listed")
    if email:
        print(
            f"Clusters attributable to {email}: "
            f"{', '.join(observations.clusters_for_user) or '(none)'}"
        )


def read_secret_scopes(client: WorkspaceClient, observations: Observations) -> None:
    """List secret scopes, which is where Aura credentials belong."""
    response = call(client, "GET", "/api/2.0/secrets/scopes/list")
    if response is None:
        return
    scopes = response.get("scopes", []) if isinstance(response, dict) else []
    observations.secret_scopes = [str(scope.get("name", "")) for scope in scopes]
    print(f"Secret scopes: {', '.join(observations.secret_scopes) or '(none)'}")


def read_user_folder(client: WorkspaceClient, observations: Observations) -> None:
    """List the lab user's workspace folder, where the notebooks should be."""
    email = observations.lab_user_email
    if not email:
        print("No lab user email, so no workspace folder was listed.")
        return

    path = f"/Users/{email}"
    outcome = call_detail(client, "GET", "/api/2.0/workspace/list", path=path)
    if outcome.payload is None:
        # Only a not-found answer says anything about the folder, and even that
        # says two things at once. A denial, a throttle, or an unreachable host
        # says something about the call instead, and recording one of those as an
        # absent folder would report a finding about something never measured.
        if classify(outcome.error_code, outcome.message) is Verdict.NOT_FOUND_OR_HIDDEN:
            observations.user_folder_exists = False
        else:
            print(
                "  The folder was not read, so nothing is concluded about "
                "whether it exists."
            )
        return

    response = outcome.payload
    objects = response.get("objects", []) if isinstance(response, dict) else []
    observations.user_folder_exists = True
    print(f"{path}: {len(objects)} object(s)")
    for item in objects[:MAX_LISTED]:
        print(f"  {item.get('object_type', '?'):10s} {item.get('path', '')}")
    if len(objects) > MAX_LISTED:
        print(f"  ... {len(objects) - MAX_LISTED} more not listed")


# ---------------------------------------------------------------------------
# Step 5B: the principals
# ---------------------------------------------------------------------------

# The account Permission Assignment API, which is the call Vocareum's "add user
# to workspace" step makes. Both shapes are published and accounts differ over
# which one answers, so both are tried before the endpoint is called absent.
ASSIGNMENTS_PATHS = (
    "/api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments",
    (
        "/api/2.0/preview/accounts/{account_id}/workspaces/{workspace_id}"
        "/permissionassignments"
    ),
)
ACCOUNT_USERS_PATH = "/api/2.0/accounts/{account_id}/scim/v2/Users"
ACCOUNT_GROUPS_PATH = "/api/2.0/accounts/{account_id}/scim/v2/Groups"
WORKSPACE_USERS_PATH = "/api/2.0/preview/scim/v2/Users"
WORKSPACE_GROUPS_PATH = "/api/2.0/preview/scim/v2/Groups"

# The prefixes Vocareum gives the principals it generates per session. Used to
# pick this lab's principals out of an account that also holds everything else
# anyone has ever created in it.
LAB_USER_PREFIX = "labuser"
LAB_GROUP_PREFIX = "Group-lab"

# A verdict that says the endpoint itself is not there for this workspace, as
# against one that says the caller may not read it. Only the first is evidence
# about identity federation; the second is evidence about the service principal.
NOT_FEDERATED_VERDICTS = frozenset(
    {
        Verdict.NOT_FOUND_OR_HIDDEN,
        Verdict.FEATURE_DISABLED,
        Verdict.UNKNOWN_ENDPOINT,
    }
)


@dataclass(frozen=True)
class ScimReading:
    """What one SCIM lookup found, keeping "not read" apart from "not there".

    ``read`` is False when the call did not complete, which is not an answer
    about the principal at all. ``read`` True with an empty ``principal_id`` is
    an answer: the filter was applied and matched nothing.
    """

    read: bool = False
    principal_id: str = ""
    display_name: str = ""
    created: str = ""

    @property
    def found(self) -> bool:
        """Whether the lookup positively identified a principal."""
        return self.read and bool(self.principal_id)


def workspace_id_for(client: WorkspaceClient, config: Config) -> str:
    """Return the numeric workspace id every account-plane call is addressed by.

    The configured value is preferred because ``guard_host`` has already proved
    the client resolves to it whenever both are available, so the two cannot
    disagree by the time this runs.
    """
    if config.expected_workspace_id.strip():
        return config.expected_workspace_id.strip()
    resolver = getattr(client, "get_workspace_id", None)
    if resolver is None:
        return ""
    try:
        return str(resolver())
    except (DatabricksError, RequestException) as error:
        print(f"  The workspace id could not be resolved: {redact_text(str(error))}")
        return ""


def scim_lookup(
    client: WorkspaceClient | AccountClient, url_path: str, filter_expression: str
) -> ScimReading:
    """Look one principal up by SCIM filter and report what came back.

    This is the read ``dbx-vocareum-probe`` structurally cannot make. A SCIM
    filter matching nothing still returns a well-formed body, so ``looks_empty``
    calls it ``OK`` and the probe reports a successful call rather than an absent
    principal. The answer is in the body, so the body has to be opened.
    """
    outcome = call_detail(client, "GET", url_path, filter=filter_expression)
    if outcome.payload is None:
        verdict = classify(outcome.error_code, outcome.message)
        print(f"  {url_path} was not read: {verdict}. Nothing is concluded from it.")
        return ScimReading()

    resources = (
        outcome.payload.get("Resources", [])
        if isinstance(outcome.payload, dict)
        else []
    )
    if not resources or not isinstance(resources[0], dict):
        return ScimReading(read=True)

    first = resources[0]
    meta = first.get("meta")
    return ScimReading(
        read=True,
        principal_id=str(first.get("id", "")),
        display_name=str(first.get("displayName", "")),
        created=str(meta.get("created", "")) if isinstance(meta, dict) else "",
    )


def read_assignments(
    account: AccountClient,
    config: Config,
    workspace_id: str,
    observations: Observations,
) -> list[dict[str, Any]]:
    """List who holds an account-level permission assignment on this workspace.

    An empty list and an unavailable endpoint are different answers that read
    alike, so the distinction is drawn here rather than left to the caller: the
    endpoint answering at all is what identity federation looks like from
    outside, and it is the precondition for every other reading in this step.
    """
    for path in ASSIGNMENTS_PATHS:
        url_path = path.format(account_id=config.account_id, workspace_id=workspace_id)
        outcome = call_detail(account, "GET", url_path)
        if outcome.payload is None:
            verdict = classify(outcome.error_code, outcome.message)
            print(f"  {url_path}: {verdict}")
            if verdict not in NOT_FEDERATED_VERDICTS:
                # A denial or a throttle says something about the caller, not
                # about the workspace. Stopping here keeps the run from
                # reporting "not federated" on the strength of a credential
                # problem, which would send the reader to the wrong console.
                print(
                    "  That says something about this caller rather than about "
                    "the workspace, so federation is left unmeasured."
                )
                return []
            continue

        observations.account_reached = True
        observations.workspace_federated = True
        payload = outcome.payload
        entries = (
            payload.get("permission_assignments", [])
            if isinstance(payload, dict)
            else []
        )
        entries = [entry for entry in entries if isinstance(entry, dict)]
        print(
            f"{url_path} answered, so this workspace is identity federated and "
            "account-level principal assignment applies to it."
        )
        print(f"Permission assignments: {len(entries)}")
        for entry in entries[:MAX_LISTED]:
            print(f"  {describe_assignment(entry)}")
        if len(entries) > MAX_LISTED:
            print(f"  ... {len(entries) - MAX_LISTED} more not listed")
        return entries

    observations.workspace_federated = False
    print(
        "Neither permission-assignment path exists for this workspace, so it is "
        "not identity federated. A workspace principal is local to the workspace "
        "and has no account-level counterpart, which is the condition that "
        "produces 'Principal not found in account' verbatim."
    )
    return []


def assignment_principal(entry: dict[str, Any]) -> dict[str, Any]:
    """Return the principal half of one assignment entry."""
    principal = entry.get("principal")
    return principal if isinstance(principal, dict) else {}


def describe_assignment(entry: dict[str, Any]) -> str:
    """Render one assignment as a single readable line."""
    principal = assignment_principal(entry)
    name = (
        principal.get("user_name")
        or principal.get("group_name")
        or principal.get("service_principal_name")
        or principal.get("display_name")
        or "(unnamed)"
    )
    permissions = entry.get("permissions")
    granted = (
        ", ".join(str(item) for item in permissions)
        if isinstance(permissions, list)
        else "(none)"
    )
    return f"{principal.get('principal_id', '?')}  {name}  [{granted}]"


def assignment_names(entries: list[dict[str, Any]]) -> set[str]:
    """Return every name that holds an assignment, across all principal kinds."""
    names: set[str] = set()
    for entry in entries:
        principal = assignment_principal(entry)
        for key in ("user_name", "group_name", "service_principal_name"):
            value = str(principal.get(key, "")).strip()
            if value:
                names.add(value)
    return names


def compare_planes(
    client: WorkspaceClient,
    account: AccountClient,
    config: Config,
    observations: Observations,
    entries: list[dict[str, Any]],
) -> None:
    """Ask both planes for the same two principals and report the difference.

    The id comparison is the decisive read. One id resolving on both planes is
    what identity federation looks like from outside; two different ids, or one
    side empty, means two directories and a workspace principal that no
    account-level call can name.
    """
    held = assignment_names(entries)
    email = observations.lab_user_email
    group = observations.group_name

    if email:
        print(f"Lab user {email}")
        account_user = scim_lookup(
            account,
            ACCOUNT_USERS_PATH.format(account_id=config.account_id),
            f'userName eq "{email}"',
        )
        workspace_user = scim_lookup(
            client, WORKSPACE_USERS_PATH, f'userName eq "{email}"'
        )
        report_plane_pair(account_user, workspace_user)
        if account_user.read:
            observations.account_reached = True
            observations.user_in_account = account_user.found
        observations.account_user_id = account_user.principal_id
        observations.workspace_user_id = workspace_user.principal_id
        if observations.workspace_federated:
            observations.user_has_assignment = email in held
            print(f"  holds an assignment  = {email in held}")
    else:
        print("The session record names no lab user, so neither plane was asked.")

    if group:
        print(f"\nLab group {group}")
        account_group = scim_lookup(
            account,
            ACCOUNT_GROUPS_PATH.format(account_id=config.account_id),
            f'displayName eq "{group}"',
        )
        workspace_group = scim_lookup(
            client, WORKSPACE_GROUPS_PATH, f'displayName eq "{group}"'
        )
        report_plane_pair(account_group, workspace_group)
        if account_group.read:
            observations.account_reached = True
            observations.group_in_account = account_group.found
        observations.account_group_id = account_group.principal_id
        observations.workspace_group_id = workspace_group.principal_id
        if observations.workspace_federated:
            observations.group_has_assignment = group in held
            print(f"  holds an assignment  = {group in held}")
    else:
        print("\nThe session record names no group, so neither plane was asked.")


def report_plane_pair(account: ScimReading, workspace: ScimReading) -> None:
    """Print one principal as both planes see it, and what the pair means."""
    print(f"  account plane id     = {plane_id(account)}")
    print(f"  workspace plane id   = {plane_id(workspace)}")
    if account.created:
        print(f"  account meta.created = {account.created}")

    if not (account.read and workspace.read):
        print("  One plane did not answer, so the ids are not compared.")
        return
    if account.found and workspace.found:
        if account.principal_id == workspace.principal_id:
            print(
                "  Same id on both planes: one principal seen twice, which is "
                "what identity federation looks like."
            )
        else:
            print(
                "  Different ids on the two planes: two distinct principals. An "
                "account-level call naming the workspace id cannot resolve it."
            )
        return
    if workspace.found and not account.found:
        print(
            "  Present in the workspace and absent from the account. This is a "
            "workspace-local principal, and it is exactly what "
            "'Principal not found in account' reports."
        )
        return
    if account.found and not workspace.found:
        print(
            "  Present in the account and absent from the workspace, so the "
            "assignment that would put it there has not taken effect."
        )
        return
    print("  Absent from both planes.")


def plane_id(reading: ScimReading) -> str:
    """Render one plane's answer, distinguishing absence from silence."""
    if not reading.read:
        return "(not read)"
    return reading.principal_id or "(no match)"


def read_lab_principal_timeline(
    client: WorkspaceClient,
    account: AccountClient,
    config: Config,
    entries: list[dict[str, Any]],
) -> None:
    """List every lab principal on both planes, oldest first.

    Vocareum's names carry their own creation time, which is the only clock this
    repository has over a record that timestamps nothing. Sorted, they say
    whether the principals in play belong to the session that failed or to an
    older one, which is the difference between a live condition and a stale
    message.
    """
    held = assignment_names(entries)
    rows: list[tuple[str, str, str, bool]] = []
    sources = (
        (
            "account user",
            account,
            ACCOUNT_USERS_PATH.format(account_id=config.account_id),
            "userName",
            LAB_USER_PREFIX,
        ),
        ("workspace user", client, WORKSPACE_USERS_PATH, "userName", LAB_USER_PREFIX),
        (
            "account group",
            account,
            ACCOUNT_GROUPS_PATH.format(account_id=config.account_id),
            "displayName",
            LAB_GROUP_PREFIX,
        ),
        (
            "workspace group",
            client,
            WORKSPACE_GROUPS_PATH,
            "displayName",
            LAB_GROUP_PREFIX,
        ),
    )

    for label, source, url_path, name_key, prefix in sources:
        outcome = call_detail(source, "GET", url_path)
        if outcome.payload is None:
            print(f"  {label}s were not listed, so none appear below.")
            continue
        resources = (
            outcome.payload.get("Resources", [])
            if isinstance(outcome.payload, dict)
            else []
        )
        for item in resources:
            if not isinstance(item, dict):
                continue
            name = str(item.get(name_key, ""))
            if not name.startswith(prefix):
                continue
            created = principal_created_at(name)
            rows.append(
                (
                    created.isoformat() if created else "(no epoch in name)",
                    label,
                    name,
                    name in held,
                )
            )

    if not rows:
        print("No Vocareum-generated lab principals were found on either plane.")
        return

    rows.sort()
    print("Lab principals, oldest first. * marks one holding an assignment.")
    for created, label, name, has_assignment in rows[:MAX_LISTED]:
        print(f"  {'*' if has_assignment else ' '} {created}  {label:15s} {name}")
    if len(rows) > MAX_LISTED:
        print(f"  ... {len(rows) - MAX_LISTED} more not listed")


def read_principals(
    client: WorkspaceClient | None, config: Config, observations: Observations
) -> None:
    """Compare the account and workspace planes for this session's principals.

    Answers the one question the rest of the diagnostic cannot: which principal
    Vocareum's "add user to workspace" step could not find, and whether the
    account plane applies to this workspace at all.
    """
    section("STEP 5B: The principals")
    if client is None:
        print(
            "The workspace side was not read, so this step did not run. It "
            "compares an account principal against its workspace counterpart, "
            "and half of that comparison is not a comparison."
        )
        return

    databricks = config.databricks_config()
    try:
        account = build_account_client(databricks)
    except GuardError as error:
        print(f"\nThe account side was not read.\n{error}")
        return

    workspace_id = workspace_id_for(client, config)
    if not workspace_id:
        print(
            "The workspace's numeric id is neither set as "
            "DATABRICKS_WORKSPACE_ID nor resolvable through the SDK, and every "
            "account-plane call is addressed by it. Nothing was read."
        )
        return

    print(f"Account {config.account_id}, workspace {workspace_id}")
    rule()
    entries = read_assignments(account, config, workspace_id, observations)
    rule()
    compare_planes(client, account, config, observations, entries)
    rule()
    read_lab_principal_timeline(client, account, config, entries)


# ---------------------------------------------------------------------------
# Step 5C: the course's own objects
# ---------------------------------------------------------------------------


def read_expectations(
    client: WorkspaceClient | None, manifest: Path, observations: Observations
) -> None:
    """Check the objects a course says its hooks build, and report each one.

    This is the step that replaces reading ``voccustomdata.txt``. A hook that
    creates a catalog and then dies may have no way to tell anyone, so the
    catalog is asked about directly instead. It is the
    difference between a lab that looks correct because a student got a cluster
    and a lab that is correct because everything the course needs is there.
    """
    section("STEP 5C: The course's own objects")
    if client is None:
        print(
            "The workspace side was not read, so nothing was checked. Every "
            "expectation is a workspace lookup, and a manifest checked against "
            "no workspace would report absence about a place nobody visited."
        )
        return

    try:
        expectations = load_manifest(manifest)
    except ManifestError as error:
        print(f"The manifest was not read.\n{error}")
        return

    print(f"{manifest}: {len(expectations)} object(s) expected")
    rule()
    observations.expectations_checked = True
    for result in check_all(client, expectations):
        record_expectation(result, observations)


def gate_failed(observations: Observations) -> bool:
    """Whether a ``--expect`` run should exit non-zero.

    Three ways to fail and only one way to pass, which is the asymmetry the
    whole gate rests on. An object that is absent fails for the obvious reason.
    An object that could not be looked up fails because a gate passing on "the
    call did not complete" would pass on a revoked credential, and that is the
    one failure every check in a run is most likely to share. A manifest that
    never ran fails because empty lists otherwise read as nothing missing, so
    the gate would pass hardest exactly when it measured least.
    """
    return bool(
        not observations.expectations_checked
        or observations.expected_missing
        or observations.expected_unchecked
    )


def record_expectation(result: Result, observations: Observations) -> None:
    """Print one result and file it under the right observation list."""
    label = str(result.expectation)
    detail = f"   {result.detail}" if result.detail else ""
    print(f"  {result.presence.value:18s} {label}{detail}")

    if result.presence is Presence.PRESENT:
        observations.expected_present.append(label)
    elif result.presence is Presence.ABSENT_OR_HIDDEN:
        observations.expected_missing.append(label)
    else:
        observations.expected_unchecked.append(label)


# ---------------------------------------------------------------------------
# Step 6: interpretation
# ---------------------------------------------------------------------------


def principal_findings(observations: Observations) -> list[Finding]:
    """Turn the step 5B readings into findings, most decisive first.

    Every one is guarded on the reading actually having been taken. A run
    without ``--principals`` leaves all of these fields at their defaults, and an
    ungated test on a default would report a principal as absent from an account
    nobody asked.
    """
    findings: list[Finding] = []

    if observations.workspace_federated is False:
        findings.append(
            Finding(
                "This workspace is not identity federated",
                "Neither account permission-assignment path answers for it.",
                "So a workspace principal is local to the workspace and has no "
                "account-level counterpart, and every account-level assignment "
                "call against one fails with RESOURCE_DOES_NOT_EXIST however "
                "correct the id is. Nothing in the lifecycle scripts can fix "
                "this. Enable identity federation on the workspace, or have the "
                "scripts use workspace-level SCIM throughout.",
            )
        )

    if (
        observations.account_reached
        and observations.group_in_account is False
        and observations.group_has_assignment is False
    ):
        detail = f"{observations.group_name!r} is absent from account SCIM."
        if observations.workspace_group_id:
            detail += (
                f" It exists in the workspace as id {observations.workspace_group_id}."
            )
        findings.append(
            Finding(
                "The session's group is the principal that was not found",
                detail + " It holds no assignment on this workspace either.",
                "The account plane was read successfully for other objects in "
                "this same run, so the absence is a real one rather than this "
                "caller being unable to see it, and the assignment that would "
                "have followed did not happen. Do not assign the group by hand: "
                "that clears the symptom for one student and leaves the next "
                "with the same failure. Vocareum creates the group, so the fix "
                "is on their side, and the question to put to them is which "
                "principal_id their assignment call passed.",
            )
        )

    # The interesting case, and the one that reads as a contradiction until it is
    # named: a principal the account directory cannot resolve, holding an
    # account-plane assignment on the workspace anyway. It means the error
    # message is literally true and still did not stop anything, so a reader who
    # sees only the absence would chase a step that succeeded.
    if (
        observations.account_reached
        and observations.group_in_account is False
        and observations.group_has_assignment
    ):
        findings.append(
            Finding(
                "The session's group is workspace-local and assigned regardless",
                f"{observations.group_name!r} does not resolve in account SCIM, "
                f"yet id {observations.workspace_group_id} appears in this "
                "workspace's account-level assignment list.",
                "So 'Principal not found in account' is a true statement about "
                "this group and is not what failed the session: the assignment "
                "exists. Treat the message as a report from a step that "
                "recovered, and look for the failure elsewhere. Worth "
                "confirming this is Vocareum's normal shape by checking whether "
                "an earlier session's group looks identical, which the timeline "
                "above prints.",
            )
        )

    if observations.account_reached and observations.user_in_account is False:
        findings.append(
            Finding(
                "The lab user has no account-level counterpart",
                f"{observations.lab_user_email} is absent from account SCIM.",
                "An account-level permission assignment naming this user cannot "
                "resolve it. Check that the service principal still holds "
                "Account admin, which is the credential Vocareum uses to create "
                "account users.",
            )
        )

    if (
        observations.account_user_id
        and observations.workspace_user_id
        and observations.account_user_id != observations.workspace_user_id
    ):
        findings.append(
            Finding(
                "The lab user is two principals, not one",
                f"account id {observations.account_user_id}, workspace id "
                f"{observations.workspace_user_id}, same userName.",
                "Two directories hold a user of the same name, so an id read "
                "from one plane means nothing on the other. Any tooling that "
                "reads a workspace id and passes it to an account call, "
                "including Vocareum's, fails here by construction.",
            )
        )

    if observations.user_has_assignment is False:
        findings.append(
            Finding(
                "The lab user holds no permission assignment on this workspace",
                f"{observations.lab_user_email} is not in the workspace's "
                "account-level assignment list.",
                "The add-user step did not take effect for this student, so the "
                "error on the record describes this session rather than an "
                "older one. The student can authenticate and reach nothing.",
            )
        )

    if observations.group_has_assignment is False and observations.group_in_account:
        findings.append(
            Finding(
                "The session's group exists in the account but is not assigned",
                f"{observations.group_name!r} resolves at account level and "
                "holds no assignment on this workspace.",
                "Unlike an absent group, this one could be assigned. It points "
                "at the assignment call failing for a reason other than the "
                "principal, so read what Vocareum's error_info says before "
                "concluding anything about the group.",
            )
        )

    return findings


def interpret(observations: Observations) -> list[Finding]:
    """Turn the observations into findings, most decisive first.

    Each finding names one gap and what closes it. A run that finds nothing
    returns an empty list rather than a reassurance, because "no known cause
    matched" and "the lab is fine" are different statements.
    """
    findings: list[Finding] = []

    if observations.labtype and observations.labtype.lower() != "databricks":
        findings.append(
            Finding(
                "The part is not a Databricks lab",
                f"labtype is {observations.labtype!r}.",
                "Set the part's labtype to Databricks in the Vocareum UI. An "
                "admin has to enable Databricks in the course's Cloud Resources "
                "settings before that option appears.",
            )
        )

    missing_required = [
        script.name
        for script in LIFECYCLE_SCRIPTS
        if script.required and script.name not in observations.deployed_scripts
    ]
    # Gated on the listing having been read, the way the workspace findings are
    # gated on workspace_reached. Ungated, a step 2 that could not read the
    # directory reports the most decisive finding in this list against a part
    # whose hooks are deployed and correct, and the obvious response to it is to
    # upload them again, which cannot help.
    if missing_required and observations.scripts_listed:
        findings.append(
            Finding(
                "A required lifecycle script is not deployed",
                f"Absent from /voc/{SCRIPTS_TARGET}: {', '.join(missing_required)}.",
                "Upload it with `uv run dbx-vocareum-upload lab/`. Vocareum "
                "skips an absent hook in silence, so the lab starts and does "
                "nothing.",
            )
        )

    if observations.script_error_code or observations.script_message:
        findings.append(
            Finding(
                "A lifecycle script reported its own failure",
                f"error_code={observations.script_error_code or '(none)'} "
                f"message={observations.script_message or '(none)'}",
                "This came from voccustomdata.txt, so a script wrote it and "
                "exited non-zero. That message is the closest thing to a log "
                "Vocareum exposes; read it before anything else here.",
            )
        )

    # Read before the session-state findings because this is the one field that
    # distinguishes a hook that ran from one Vocareum never invoked, and that
    # distinction decides whether the scripts or the lab configuration is what
    # needs fixing. The two branches are mutually exclusive: voc_ipc_beacon
    # writes "entered" once at the top of the hook and voc_fail overwrites it
    # with an error code, so whichever survives is where the hook stopped.
    if observations.beacon_state == BEACON_ENTERED:
        findings.append(
            Finding(
                "A lifecycle script started and did not finish",
                f"{observations.beacon_script or 'A hook'} wrote an entry "
                f"beacon at {observations.beacon_at or 'an unrecorded time'} "
                "and nothing overwrote it.",
                "So Vocareum did invoke the hook, and the hook did not reach "
                "its landing URL and did not call voc_fail either. That leaves "
                "a crash between the two, most likely a command failing under "
                "set -e outside a guard. The beacon lists the VOC_ variables "
                "the hook was handed, which is what says whether the value it "
                "died on was even present.",
            )
        )
    elif observations.beacon_state:
        findings.append(
            Finding(
                "A lifecycle script failed and said so through the beacon",
                f"{observations.beacon_script or 'A hook'} reported "
                f"{observations.beacon_state} at "
                f"{observations.beacon_at or 'an unrecorded time'}.",
                "voc_fail wrote this, so the hook ran, hit a guard, and exited "
                "1 with that error code. It is the same code voccustomdata.txt "
                "carries, through the channel that is proven to reach the "
                "session record.",
            )
        )

    if observations.session_started is False:
        findings.append(
            Finding(
                "This session has never started",
                "sessionstart and state are both empty on the record.",
                "So nothing here separates a hook that failed from a hook that "
                "was never invoked, and an absent notebook URL is expected "
                "rather than a symptom. Run with --start, or have the student "
                "open the lab, then read the record again.",
            )
        )
    elif observations.session_state.strip().lower() in FAILED_SESSION_STATES:
        findings.append(
            Finding(
                "Vocareum reports this session as failed",
                f"state is {observations.session_state!r} on the record.",
                "Every field a running lab fills in is empty on a failed "
                "session, so an absent notebook URL and an empty user folder "
                "follow from this rather than adding to it, and neither is "
                "separately evidence about the lifecycle scripts. What this does "
                "not say is how far the session got before it failed: the record "
                "carries no timestamp on the failure and no log. Read Vocareum's "
                "own error for this part first, then the principals step, and "
                "treat everything else below as describing the wreckage.",
            )
        )
    elif not observations.notebook_url:
        findings.append(
            Finding(
                "No notebook URL was handed back",
                "The session record carries no notebook_url.",
                "user_setup.sh either exited before writing $VOC_IPC_DATA_FILE "
                "or wrote something other than JSON with a notebook_url key. "
                "The student lands in a bare workspace rather than on the "
                "workshop notebook. Write the file as the last thing the script "
                "does, so a partial run cannot produce a URL that points at "
                "nothing. If the deployed user_setup.sh carries the entry "
                "beacon, an empty field here means more than that: the hook "
                "writes one before its first guard, so nothing at all means "
                "Vocareum never invoked it, and the fix is in the part's "
                "configuration rather than in the script.",
            )
        )

    if observations.user_found is False:
        findings.append(
            Finding(
                "The lab user does not exist in the workspace",
                f"{observations.lab_user_email} was not found by SCIM.",
                "Vocareum adds the student to the workspace before it runs "
                "user_setup.sh, so this is a failure on Vocareum's side of the "
                "handoff rather than in the scripts. Check that the service "
                "principal still holds Account admin, which is what Vocareum "
                "uses to add users.",
            )
        )
    elif observations.user_found and not observations.user_groups:
        findings.append(
            Finding(
                "The lab user is in no group",
                f"{observations.lab_user_email} exists with no group membership.",
                "Workspace permissions are granted to groups, so a student in "
                "no group has an identity that can enter the workspace and do "
                "nothing in it. Grant against $VOC_DB_GROUP_NAME in "
                "user_setup.sh rather than against the individual user.",
            )
        )

    if (
        observations.workspace_reached
        and observations.user_found
        and observations.user_folder_exists is False
    ):
        findings.append(
            Finding(
                "The lab user has no visible workspace folder",
                f"/Users/{observations.lab_user_email} was reported as not "
                "existing. Databricks answers a caller with no permission on a "
                "path the same way, so this is either an absent folder or one the "
                "service principal cannot see.",
                "If it is absent, nothing was imported for this student: check "
                "that user_setup.sh tests the exit code of workspace/mkdirs and "
                "workspace/import rather than running them in sequence and "
                "exiting 0 regardless. If it is there and hidden, the service "
                "principal is missing a grant on it, and every workspace reading "
                "above is narrower than it appears.",
            )
        )

    if observations.vocareum_workspace_error:
        findings.append(
            Finding(
                "Vocareum recorded a Databricks-side failure on this workspace",
                f"error_info = {observations.vocareum_workspace_error} "
                f"(status {observations.vocareum_workspace_status or '(empty)'}, "
                f"num_users {observations.vocareum_num_users or '(empty)'}).",
                "This is the only place Databricks reports a failure back to "
                "Vocareum in words, and it persists on a workspace whose status "
                "reads RUNNING. It carries no timestamp and names no principal, "
                "so it cannot be attributed to the session above from the API "
                "alone. The principals step says whether the condition it "
                "describes still holds; if it does not, this is a report from a "
                "step that recovered.",
            )
        )

    # Read high, because on a part where voccustomdata does not surface this is
    # the only finding that says whether the lab is actually correct. Everything
    # above it describes the student's identity and their compute, and a lab can
    # have both and still be missing the catalog the first notebook cell opens.
    if observations.expected_missing:
        findings.append(
            Finding(
                "The course expects objects that are not in this workspace",
                f"Absent or hidden: {', '.join(observations.expected_missing)}.",
                "A hook was supposed to build each of these and something did "
                "not. Which hook is the question the session record cannot "
                "answer on a Databricks part, so work back from the object: a "
                "catalog, a warehouse or a pipeline missing points at "
                "workspace_init.sh, which Vocareum runs once per workspace and "
                "never again, so a workspace created before the current scripts "
                "were deployed has never run them. Redeploying the scripts does "
                "not re-run it. Absence is reported as absent-or-hidden because "
                "Databricks answers a caller with no permission the same way; "
                "as the service principal that is unlikely and it is unmeasured.",
            )
        )

    if observations.expected_unchecked:
        findings.append(
            Finding(
                "Some expected objects could not be looked up at all",
                f"Not checked: {', '.join(observations.expected_unchecked)}.",
                "These calls failed for a reason other than the object being "
                "absent, so nothing is concluded about them and the run below "
                "is narrower than the manifest asked for. Read the detail "
                "printed beside each one in step 5C before trusting a pass.",
            )
        )

    findings.extend(principal_findings(observations))

    if (
        observations.workspace_reached
        and observations.lab_user_email
        and not observations.clusters_for_user
    ):
        findings.append(
            Finding(
                "No compute belongs to the lab user",
                f"No cluster names or is owned by {observations.lab_user_email}.",
                "Expected when the lab hands the student a shared or serverless "
                "cluster. A problem when user_setup.sh is meant to create one: "
                "check the allow-cluster-create entitlement on the group, and "
                "the workspace's cluster quota.",
            )
        )

    return findings


def report(observations: Observations, findings: list[Finding]) -> None:
    """Print the diagnosis."""
    section("STEP 6: What this adds up to")
    if not findings:
        print(
            "No known cause matched. That is not the same as the lab being "
            "correct: it means every check this tool knows how to make came "
            "back consistent."
        )
        if observations.notebook_url:
            print(f"\nThe student would be sent to {observations.notebook_url}")
        return

    for index, finding in enumerate(findings, start=1):
        print(f"{index}. {finding.label}")
        print(f"   {finding.detail}")
        print(f"   {finding.guidance}")
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Recover why a Vocareum Databricks lab did not come up, without "
            "console access."
        ),
        epilog=(
            "Configuration is read from .env in the repository root. Real "
            "environment variables take precedence. See .env.example."
        ),
    )
    parser.add_argument(
        "--start",
        action="store_true",
        help=(
            "Start or extend the lab session first. Off by default, because "
            "starting a Databricks session runs user_setup.sh and that is what "
            "creates billable compute."
        ),
    )
    parser.add_argument(
        "--raw-record",
        action="store_true",
        help=(
            "Print the whole session record, redacted. Useful for mapping the "
            "undocumented Databricks record shape."
        ),
    )
    parser.add_argument(
        "--skip-workspace",
        action="store_true",
        help="Read only the Vocareum side and make no Databricks calls.",
    )
    parser.add_argument(
        "--principals",
        action="store_true",
        help=(
            "Also compare the account and workspace identity planes for this "
            "session's user and group. Off by default because it needs "
            "DATABRICKS_ACCOUNT_ID and makes account-scoped calls; on when the "
            "record reports a principal Databricks could not find."
        ),
    )
    parser.add_argument(
        "--expect",
        type=Path,
        metavar="MANIFEST",
        help=(
            "Check that the objects named in a course's JSON manifest exist in "
            "the workspace, and exit 3 if any of them do not. This is the ship "
            "gate: voccustomdata.txt does not surface on a Databricks part, so "
            "what the hooks built is read from the workspace rather than from "
            "anything they said about themselves."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Also print the observations as JSON.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repository_root() / ".env",
        help="Path to the .env file. Defaults to the repository root.",
    )
    return parser.parse_args(argv)


def run_diagnostic(config: Config, args: argparse.Namespace) -> int:
    """Run every step in order and return the process exit code."""
    observations = Observations()

    print(f"Vocareum API base : {config.api_base}")
    print(f"Course/Assignment : {config.course_id}/{config.assignment_id}")
    print(f"Part / User       : {config.part_id} / {config.user_id}")
    print(f"Databricks host   : {config.host or '(unset)'}")

    read_part(config, observations)
    read_scripts(config, observations)

    if args.start:
        start_session(config)
    else:
        section("STEP 3: Start the session")
        print(
            "Skipped. Pass --start to run user_setup.sh for this student. "
            "Reading the existing record instead."
        )

    read_record(config, observations, wait=args.start, raw=args.raw_record)
    read_vocareum_workspace(config, observations)

    client: WorkspaceClient | None = None
    if args.skip_workspace:
        section("STEP 5: The workspace")
        print("Skipped by --skip-workspace.")
    else:
        client = read_workspace(config, observations)

    if args.principals:
        read_principals(client, config, observations)

    if args.expect is not None:
        read_expectations(client, args.expect.expanduser(), observations)

    report(observations, interpret(observations))

    if args.json:
        section("JSON")
        print(json.dumps(redact_payload(observations.__dict__), indent=2, default=str))

    if args.expect is not None and gate_failed(observations):
        return EXIT_EXPECTATIONS_UNMET
    return EXIT_OK


def main(argv: list[str] | None = None) -> int:
    """Entry point. See the module docstring for exit code meanings."""
    args = parse_args(argv)

    try:
        config = load_config(args.env_file.expanduser())
        return run_diagnostic(config, args)
    except ConfigError as error:
        print("Configuration error.")
        print(error)
        return EXIT_CONFIG
    except ApiError as error:
        section("RESULT")
        print(f"The diagnostic could not complete: {error}")
        return EXIT_API


if __name__ == "__main__":
    raise SystemExit(main())
