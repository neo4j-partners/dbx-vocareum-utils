"""Databricks constants, host handling, and the lifecycle contract Vocareum runs.

Two kinds of fact live here. The first is what Vocareum promises a Databricks
lab: four shell scripts read from ``/voc/scripts``, run in a fixed order, each
handed a documented set of environment variables. That contract is the whole
interface between this repository and a student's workspace, so it is written
down once, in one place, and the lab template and its tests both read it from
here rather than restating it.

The second is the small amount of Databricks-side parsing the tools need. A
Databricks API failure carries a machine-readable ``error_code`` beside its
prose ``message``, which is a better starting point than the AWS equivalent
where the refused action has to be recovered from free text.
"""

from __future__ import annotations

import datetime
import re
import urllib.parse
from dataclasses import dataclass
from typing import Any

DEFAULT_ACCOUNT_HOST = "https://accounts.cloud.databricks.com"

# The workspace directory Vocareum reads the lifecycle scripts from. Nothing
# else in the workspace tree is executed.
SCRIPTS_TARGET = "scripts"

# What it takes to reach a workspace, and what it takes to reach the account
# console, which is a different host. Named here rather than in config.py so the
# Vocareum variables and the Databricks variables stay separable: a run can need
# one set, the other, or both.
DATABRICKS_WORKSPACE_VARS = (
    "DATABRICKS_HOST",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
)
DATABRICKS_ACCOUNT_VARS = (*DATABRICKS_WORKSPACE_VARS, "DATABRICKS_ACCOUNT_ID")

# A personal access token belonging to the lab user, generated from inside a
# live student session. This is the only way to measure the principal a student
# actually holds, because Vocareum's session record for a Databricks part
# publishes no Databricks credential at all.
LAB_USER_TOKEN_VAR = "DATABRICKS_LAB_USER_TOKEN"


@dataclass(frozen=True)
class LifecycleScript:
    """One script Vocareum runs, and the environment it runs with."""

    name: str
    when: str
    env_vars: tuple[str, ...]
    required: bool

    @property
    def writes_ipc(self) -> bool:
        """Report whether this script may redirect the student to a notebook.

        ``VOC_IPC_DATA_FILE`` is the only channel by which a script chooses what
        the student sees on entry. Vocareum reads a ``notebook_url`` key out of
        it and sends the learner there.
        """
        return "VOC_IPC_DATA_FILE" in self.env_vars


# Execution order, and the environment each script is handed. Taken from
# Vocareum's "Databricks scripts" article. docs/databricks-labs.md
# carries the same table for a human reader, with the link.
#
# Only user_setup.sh is marked required. workspace_init.sh runs once per
# workspace rather than once per student, lab_setup.sh runs only when a lab is
# pre-warmed from the fleet dashboard, and lab_end.sh is optional because
# Vocareum's own stop-or-terminate behavior runs whether or not a script asks
# for it. A lab with only user_setup.sh is a complete lab.
LIFECYCLE_SCRIPTS: tuple[LifecycleScript, ...] = (
    LifecycleScript(
        name="workspace_init.sh",
        when="once, after a workspace is created",
        env_vars=(
            "VOC_DB_WORKSPACE_URL",
            "VOC_PARTID",
            "VOC_LABID",
            "VOC_RESOURCE_TAGS",
        ),
        required=False,
    ),
    LifecycleScript(
        name="lab_setup.sh",
        when="when a lab is pre-warmed from the fleet dashboard",
        env_vars=(
            "VOC_DB_WORKSPACE_URL",
            "VOC_PARTID",
            "VOC_LABID",
            "VOC_CUSTOM_DATA",
            "VOC_IPC_DATA_FILE",
            "VOC_DB_USER_EMAIL",
            "VOC_RESOURCE_TAGS",
        ),
        required=False,
    ),
    LifecycleScript(
        name="user_setup.sh",
        when="when a lab is started, after lab_setup.sh if that exists",
        env_vars=(
            "VOC_DB_WORKSPACE_URL",
            "VOC_CUSTOM_DATA",
            "VOC_IPC_DATA_FILE",
            "VOC_DB_GROUP_NAME",
            "VOC_DB_USER_EMAIL",
            "VOC_RESOURCE_TAGS",
        ),
        required=True,
    ),
    LifecycleScript(
        name="lab_end.sh",
        when="when a lab is ended, whether stopped or terminated",
        env_vars=(
            "VOC_DB_WORKSPACE_URL",
            "VOC_CUSTOM_DATA",
            "VOC_DB_GROUP_NAME",
            "VOC_END_LAB_BEHAVIOR",
            "VOC_DB_USER_EMAIL",
        ),
        required=False,
    ),
)

# grade.sh is held apart from the four above because it is not part of the
# lifecycle. It runs when a student clicks Submit, and only when the assignment
# allows submissions.
GRADE_SCRIPT = LifecycleScript(
    name="grade.sh",
    when="when a student clicks Submit, if submissions are enabled",
    env_vars=(
        "VOC_DB_WORKSPACE_URL",
        "VOC_CUSTOM_DATA",
        "VOC_DB_GROUP_NAME",
        "VOC_DB_USER_EMAIL",
        "vocGradeFile",
        "vocReportFile",
    ),
    required=False,
)

LIFECYCLE_SCRIPT_NAMES = tuple(script.name for script in LIFECYCLE_SCRIPTS)

# The two files a script writes to hand something onward. Both are plain JSON.
# CUSTOM_DATA_FILE reaches the next script in the sequence as VOC_CUSTOM_DATA.
# The IPC file is named by VOC_IPC_DATA_FILE at run time rather than assumed,
# because the environment variable is the contract and the filename is only
# what Vocareum's documentation happens to show.
CUSTOM_DATA_FILE = "voccustomdata.txt"

# Keys Vocareum reads back out of those files. error_code and message are read
# only from a failed run, and notebook_url only from the IPC file.
CUSTOM_DATA_FAILURE_KEYS = ("error_code", "message")
IPC_NOTEBOOK_KEY = "notebook_url"

# Keys labruntime/template/user_setup.sh writes into voccustomdata.txt on a run
# that succeeded.
# Vocareum reads none of these itself, which is exactly why they are here: the
# failure keys above are the only ones it acts on, so a clean run used to report
# nothing at all about itself. What script ran, when, and what credential it
# could reach were in the record and absent from the output, leaving --raw-record
# and a reader's eye as the only route to the one answer the first live lab start
# exists to get.
#
# workspace_id, user_email, and group_name are written alongside these three and
# are not listed, because SESSION_RECORD_FIELDS already reports the last two and
# the workspace is read back out of the notebook URL, where a mismatch against
# the configured workspace is also checked.
CUSTOM_DATA_REPORT_KEYS = ("script", "ran_at", "credentials_found")

# What "End lab behavior" can be set to on the part, and what VOC_END_LAB_BEHAVIOR
# then carries. "stop" preserves the student's data for the next session;
# "terminate" reverts the environment to its original state.
END_LAB_BEHAVIORS = frozenset({"stop", "terminate"})

# What labruntime/template/lab_end.sh does when the value is none of those two:
# unset, empty, or a word neither of them. Written here rather than only in the
# hook because it is part of the contract this module exists to state once, and
# because a test can then assert the hook and the documentation agree.
#
# "stop" rather than "terminate" or nothing at all. A part with no End lab
# behavior set was measured leaving a cluster RUNNING after its session ended,
# so doing nothing on an unrecognized value bills. Reclaiming compute costs a
# restart and keeps the cluster's configuration and libraries; deleting
# outright destroys what the session was keeping. So the default reclaims and
# does not delete.
END_LAB_BEHAVIOR_FALLBACK = "stop"

# The voclab.py subcommand each behavior maps to. clusters/delete terminates and
# keeps the configuration, clusters/permanent-delete removes it, and voclab.py
# names them the other way round from the API for a shell reader's benefit.
END_LAB_BEHAVIOR_COMMANDS = {
    "stop": "cluster-stop",
    "terminate": "cluster-remove",
}

# Fields the Vocareum session record is expected to carry for a Databricks
# part. The shape is undocumented, so the diagnostic reports which of these it
# found rather than assuming any of them are present. Each is a fragment
# matched case-insensitively against flattened key paths.
#
# Measured: a never-started Databricks session carries courseid, assignmentid,
# partid, userid, type, state, sessionstart, sessionend, totaltime, and an empty
# databricks list. The first four are already known, because they are what
# addressed the record. The rest are here.
#
# totalspend and allowedspend are not listed. They belong to the AWS variant of
# the record, so on a Databricks part they would read as two missing fields
# rather than as two fields that were never going to be there.
SESSION_RECORD_FIELDS = (
    "workspace_url",
    "user_email",
    "group_name",
    "notebook_url",
    "type",
    "sessionstart",
    "sessionend",
    "totaltime",
)

# The key holding the Databricks half of the session record. It is a list, and
# it is empty until a session has actually run, which is the plainest signal in
# the record that nothing has started yet.
SESSION_RECORD_DATABRICKS_KEY = "databricks"


class DatabricksHostError(ValueError):
    """Raised when a configured Databricks host cannot be used."""


# What a usable netloc looks like: dotted labels, optionally a port. urlsplit
# treats everything before the first slash as the netloc, spaces included, so
# without this a malformed value normalizes into something that looks like a
# host, satisfies the workspace guard by comparing equal to itself, and fails
# several calls later inside the SDK as a connection error.
HOSTNAME_PATTERN = re.compile(r"[A-Za-z0-9._-]+(:\d+)?")


def normalize_host(value: str) -> str:
    """Return a Databricks host as scheme and netloc, with nothing trailing.

    A workspace URL is copied out of a browser more often than it is typed, so
    it arrives carrying a path, a query, or a trailing slash. The SDK wants the
    bare origin. Normalizing here rather than at each call site is what lets the
    workspace guard compare two hosts for equality and have that mean something.

    A value that is not a hostname is refused here rather than passed on. The
    guard downstream compares a host with itself, so it cannot catch a malformed
    one, and the first thing that would notice is the SDK, several calls later,
    reporting a connection error that names no cause.
    """
    candidate = value.strip()
    if not candidate:
        raise DatabricksHostError("The Databricks host is empty.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"

    parts = urllib.parse.urlsplit(candidate)
    if parts.scheme != "https":
        raise DatabricksHostError(
            f"The Databricks host must be https, not {parts.scheme!r}: {value!r}"
        )
    if not parts.netloc:
        raise DatabricksHostError(f"The Databricks host has no hostname: {value!r}")
    if not HOSTNAME_PATTERN.fullmatch(parts.netloc):
        raise DatabricksHostError(
            f"The Databricks host is not a hostname: {parts.netloc!r} "
            f"from {value!r}. Expected something like "
            "https://dbc-xxxxxxxx-xxxx.cloud.databricks.com"
        )
    return f"https://{parts.netloc}"


def workspace_id_from_url(value: str) -> str | None:
    """Return the numeric workspace ID from the ``o=`` parameter of a URL.

    Vocareum's own documentation shows a ``notebook_url`` in this shape, so the
    workspace a lab actually put the student in can be read back out of the
    URL the lab handed over, and compared with the workspace the tools were
    pointed at.
    """
    parts = urllib.parse.urlsplit(value.strip())
    for name, item in urllib.parse.parse_qsl(parts.query):
        if name == "o" and item.strip().isdigit():
            return item.strip()
    return None


# The key voc_ipc_beacon puts first in the fragment of the URL it writes. Its
# presence is what separates a beacon from the landing page a completed hook
# hands back, and both arrive in the same notebook_url field.
BEACON_KEY = "voc-diag"

# What voc_ipc_beacon writes when it fires at the top of a hook rather than from
# inside voc_fail. Any other value is the error code the hook failed with.
BEACON_ENTERED = "entered"


def beacon_fields(value: str) -> dict[str, str]:
    """Return the diagnostic fields ``voc_ipc_beacon`` encoded in a URL.

    Empty for an ordinary landing URL, which is how the caller tells the two
    apart. The fields sit in the fragment because a fragment is never sent to a
    server, so a beacon that gets followed lands on an ordinary page instead of
    putting diagnostic text into somebody's access log.

    This exists because ``notebook_url`` is the only field a lifecycle script
    controls that has been observed reaching the session record. ``voc_log``
    writes to stderr, which the REST API does not expose at any depth, and
    whether ``voccustomdata.txt`` reaches the record is unverified.
    """
    fragment = urllib.parse.urlsplit(value.strip()).fragment
    if not fragment.startswith(f"{BEACON_KEY}="):
        return {}
    return dict(urllib.parse.parse_qsl(fragment, keep_blank_values=True))


# The trailing number on a name Vocareum generates. Both shapes it uses end in
# one, separated by an underscore on a user and by a hyphen on a group:
#
#   labuser10000001_1785962713@vocareum.com
#   Group-lab-10000001-5000002-1785962712
#
# Bounded to ten digits so it cannot match the lab or part identifiers earlier
# in the same name, which are of a similar size and carry no time.
VOCAREUM_PRINCIPAL_EPOCH_PATTERN = re.compile(r"[-_](\d{10})(?:@|$)")

# Vocareum's own epochs are recent. A value outside this window is a number that
# happens to have ten digits rather than a timestamp, and reporting it as a date
# would invent evidence in the one place this repository reads a date from a
# string. The lower bound is 2001-09-09, the first ten-digit epoch.
EPOCH_LOWER_BOUND = 1_000_000_000
EPOCH_UPPER_BOUND = 4_000_000_000


def principal_created_at(name: str) -> datetime.datetime | None:
    """Return when Vocareum created a principal, read off its own name.

    Vocareum stamps the creation time into every lab user and lab group name it
    generates, and it publishes no created-at field anywhere else: the session
    record carries none, and ``error_info`` on the workspace record carries no
    timestamp at all. So the name is the only clock this side of the API has.

    That makes a whole class of question answerable without a single call. A
    group and a user two seconds apart belong to the same provisioning attempt,
    and one four hours older belongs to a different one, which is what separates
    a live failure from a stale message about an earlier session.

    Returns ``None`` rather than guessing when the name carries no such stamp,
    because a date invented here would be read as measured everywhere else.
    """
    found = VOCAREUM_PRINCIPAL_EPOCH_PATTERN.search(name.strip())
    if found is None:
        return None
    epoch = int(found.group(1))
    if not EPOCH_LOWER_BOUND <= epoch <= EPOCH_UPPER_BOUND:
        return None
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.UTC)


def flatten_keys(payload: Any, prefix: str = "") -> dict[str, Any]:
    """Return a mapping of dotted key path to scalar for a nested payload.

    The Vocareum session record for a Databricks part is undocumented and its
    nesting is not stable enough to address by a fixed path. Flattening it lets
    the diagnostic search for a field by name wherever it happens to sit, and
    report the path it was found at so the next reader can address it directly.
    """
    found: dict[str, Any] = {}
    if isinstance(payload, dict):
        for name, value in payload.items():
            found.update(flatten_keys(value, f"{prefix}.{name}" if prefix else name))
    elif isinstance(payload, list):
        for index, item in enumerate(payload):
            found.update(flatten_keys(item, f"{prefix}[{index}]"))
    elif prefix:
        found[prefix] = payload
    return found


def find_field(payload: Any, fragment: str) -> tuple[str, Any] | None:
    """Find the first non-empty scalar whose key path contains ``fragment``.

    Returns the path it was found at alongside the value. The path is half the
    answer: a value with no path cannot be looked up again, and the point of
    this search is to stop needing it.
    """
    normalized = fragment.replace("_", "").lower()
    for path, value in flatten_keys(payload).items():
        if normalized not in path.replace("_", "").lower():
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if value is None:
            continue
        return path, value
    return None


def find_field_entry(payload: Any, fragment: str) -> tuple[str, Any] | None:
    """Find the first key path containing ``fragment``, empty values included.

    :func:`find_field` skips empty strings and nulls, because its callers want a
    value they can act on. Reporting what a record carries is a different
    question, and the difference is load-bearing here: a Databricks session
    record carries ``state`` and ``sessionstart`` as empty strings until a
    session has run, so treating empty as absent turns "this lab has never
    started" into "this field is missing", and those call for opposite actions.
    """
    normalized = fragment.replace("_", "").lower()
    for path, value in flatten_keys(payload).items():
        if normalized in path.replace("_", "").lower():
            return path, value
    return None


def script_names_from_listing(payload: Any) -> list[str]:
    """Return the file names in a Vocareum directory listing.

    The listing carries them in ``files`` as bare strings, so the flattened key
    paths are ``files[0]``, ``files[1]``, and so on. None of those paths contain
    a field name, which is why searching for a path containing "name" finds
    nothing at all here and reports an empty directory for a full one.

    Both shapes seen from this endpoint are accepted: a list of names, and a
    list of objects carrying a name. Values are reduced to a basename because
    the endpoint has returned paths as well as bare names, and ``/voc/scripts``
    is flat so a basename loses nothing.
    """
    names: set[str] = set()
    for path, value in flatten_keys(payload).items():
        if not isinstance(value, str) or not value.strip():
            continue
        # The last path segment, with any list index dropped: "files[0]" gives
        # "files", "files[0].name" gives "name", "files[0].size" gives "size".
        # Matching on the segment rather than the whole path is what keeps a
        # sibling field such as a size or a mode out of the file list.
        segment = path.rsplit(".", 1)[-1].split("[", 1)[0].lower()
        if segment == "files" or "name" in segment:
            names.add(value.strip().rsplit("/", 1)[-1])
    return sorted(names)
