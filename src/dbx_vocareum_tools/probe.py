"""Measure what a principal may do in the Databricks workspace behind a lab.

The AWS version of this tool measures a student's session role. The Databricks
version cannot start there, because the two clouds hand out lab credentials
differently. A Vocareum AWS lab returns an access key, a secret, and a session
token in its session record, so the student's principal is reachable from
outside. A Vocareum Databricks lab returns no Databricks credential at all. It
puts the student into a workspace as a named user in a group and hands back a
notebook URL, and the browser session is where that identity lives.

So there are two principals here and only one of them is reachable by default:

1. **The service principal.** OAuth machine-to-machine credentials from
   ``.env``. Vocareum requires it to hold Account admin, so it can do nearly
   everything. This is the principal that provisions the lab, and measuring it
   answers "will the lifecycle scripts work".
2. **The lab user.** The identity a student's notebook code runs as. Reachable
   only through a personal access token generated from inside a live student
   session and exported as ``DATABRICKS_LAB_USER_TOKEN``. Measuring it answers
   "will the student's code work", which is a different question with a
   different answer.

Every report names which one it describes, in a banner, because the first
version of the AWS tool produced a confident permissions table that described an
administrator and read as though it described a student.

Three guards run before any result is reported:

1. **Credential guard.** The credential fields must be present and non-empty.
   The Databricks SDK has its own default provider chain, so an empty secret
   makes it fall through to ``DATABRICKS_TOKEN``, then ``~/.databrickscfg``,
   then the CLI's cached OAuth login, and report on whatever the operator
   happens to hold. That is the same trap boto3 sets, and it produced one
   confident report about an unrelated account on the AWS side.
2. **Host guard.** The configured host is normalized to an origin and compared
   against the workspace the run was told to measure. When the SDK can report
   the numeric workspace ID, that is compared too.
3. **Identity guard.** ``GET /api/2.0/preview/scim/v2/Me`` runs first, and the
   identity it returns must be the one expected for the chosen source. A stray
   token in the environment cannot masquerade as the service principal.

A probe names a Databricks API operation, not an SDK method::

    clusters:List
    secrets:ListScopes
    unity-catalog:ListCatalogs

The catalog below maps each name to an HTTP method and path, and the call goes
through the SDK's own ``ApiClient`` so the OAuth exchange and its refresh are
not written twice. Probes are read-only unless ``--allow-mutating`` is passed.

One Databricks-specific caveat runs through every result. Workspace object
permissions are hiding rather than refusing: a principal with no permission on a
cluster is told the cluster does not exist, and a list call returns only the
objects the caller can see. So a 200 with an empty list and a
``RESOURCE_DOES_NOT_EXIST`` both mean less here than the equivalent AWS
responses do, and both get their own verdict rather than being folded into
"allowed".

This command is part of the repository's uv project::

    uv run dbx-vocareum-probe --self-test
    uv run dbx-vocareum-probe --probe clusters:List
    uv run dbx-vocareum-probe --catalog
    uv run dbx-vocareum-probe --source token --probes-file probes/lab-user.json

Configuration comes from ``.env`` in the repository root, or from real
environment variables, which always win. See ``.env.example``.

Exit codes:
    0  Every probe ran and was classified.
    1  Configuration error.
    2  A guard refused to report, or credentials could not be obtained.
    3  Self-test failure.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import DatabricksError
from requests.exceptions import RequestException

from .config import ConfigError, Environment, repository_root
from .databricks import (
    DATABRICKS_ACCOUNT_VARS,
    DATABRICKS_WORKSPACE_VARS,
    DEFAULT_ACCOUNT_HOST,
    LAB_USER_TOKEN_VAR,
    DatabricksHostError,
    normalize_host,
)
from .output import section
from .security import mask_secret, redact_text

EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_GUARD = 2
EXIT_SELF_TEST = 3

MAX_DETAIL_CHARS = 400

# The identity endpoint. Used by the guard rather than probed, because a run
# whose identity cannot be established reports nothing at all.
ME_PATH = "/api/2.0/preview/scim/v2/Me"

WORKSPACE_SCOPE = "workspace"
ACCOUNT_SCOPE = "account"


class GuardError(RuntimeError):
    """A guard refused to let the run report anything."""


class Verdict(StrEnum):
    """What one probe established.

    Kept deliberately finer-grained than allowed-or-denied, because the useful
    distinctions on Databricks are between things that are fixed in different
    places by different people. A workspace ACL is granted by a workspace admin,
    a Unity Catalog privilege by a metastore admin, and an entitlement by an
    account admin. All three arrive as some flavor of permission error.
    """

    OK = "OK"
    ALLOWED_EMPTY = "ALLOWED_EMPTY"
    NOT_FOUND_OR_HIDDEN = "NOT_FOUND_OR_HIDDEN"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    UNITY_CATALOG_DENIED = "UNITY_CATALOG_DENIED"
    ENTITLEMENT_MISSING = "ENTITLEMENT_MISSING"
    METASTORE_NOT_ASSIGNED = "METASTORE_NOT_ASSIGNED"
    FEATURE_DISABLED = "FEATURE_DISABLED"
    INCONCLUSIVE_BAD_INPUT = "INCONCLUSIVE_BAD_INPUT"
    THROTTLED = "THROTTLED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    ENDPOINT_UNREACHABLE = "ENDPOINT_UNREACHABLE"
    UNKNOWN_ENDPOINT = "UNKNOWN_ENDPOINT"
    UNKNOWN_OPERATION = "UNKNOWN_OPERATION"
    UNCLASSIFIED = "UNCLASSIFIED"


# The Status column a verdict maps to in docs/permissions.md. Several verdicts
# collapse here on purpose: the tracker records whether the lab can proceed,
# and the verdict records why.
TRACKER_STATUS: dict[Verdict, str] = {
    Verdict.OK: "Allowed",
    Verdict.ALLOWED_EMPTY: "Allowed",
    Verdict.NOT_FOUND_OR_HIDDEN: "Inconclusive",
    Verdict.PERMISSION_DENIED: "Denied (ACL)",
    Verdict.UNITY_CATALOG_DENIED: "Denied (UC)",
    Verdict.ENTITLEMENT_MISSING: "Denied (entitlement)",
    Verdict.METASTORE_NOT_ASSIGNED: "Blocked",
    Verdict.FEATURE_DISABLED: "Blocked",
    Verdict.INCONCLUSIVE_BAD_INPUT: "Inconclusive",
    Verdict.THROTTLED: "Untested",
    Verdict.QUOTA_EXCEEDED: "Blocked",
    Verdict.UNAUTHENTICATED: "Untested",
    Verdict.ENDPOINT_UNREACHABLE: "Untested",
    Verdict.UNKNOWN_ENDPOINT: "Untested",
    Verdict.UNKNOWN_OPERATION: "Untested",
    Verdict.UNCLASSIFIED: "Untested",
}

# What to do about each verdict. Written as the next step rather than as a
# description, because a results table nobody can act on is a report nobody
# reads twice.
NEXT_ACTION: dict[Verdict, str] = {
    Verdict.OK: "Nothing. Record it and move on.",
    Verdict.ALLOWED_EMPTY: (
        "Authorization passed and the result was empty. Databricks filters list "
        "results to the objects the caller can see, so an empty list is not "
        "evidence that the workspace is empty. Confirm against the account "
        "console before concluding anything about what exists."
    ),
    Verdict.NOT_FOUND_OR_HIDDEN: (
        "Cannot be resolved from this response alone. Databricks answers a "
        "caller with no permission on an object by saying the object does not "
        "exist, so this is either an absent resource or an invisible one. "
        "Re-run the same probe as the service principal: if it is visible "
        "there, this is a missing grant rather than a missing object."
    ),
    Verdict.PERMISSION_DENIED: (
        "A workspace ACL is missing. Grant it on the object, or on the group "
        "Vocareum puts the student in, which is named by VOC_DB_GROUP_NAME."
    ),
    Verdict.UNITY_CATALOG_DENIED: (
        "A Unity Catalog privilege is missing. These are granted on the "
        "metastore by a metastore admin, not in the workspace, so a workspace "
        "admin cannot fix it. Grant USE CATALOG and USE SCHEMA down the path to "
        "the object, since a privilege on a table is inert without them."
    ),
    Verdict.ENTITLEMENT_MISSING: (
        "The principal lacks a workspace entitlement. Add it to the group in "
        "the account console: workspace-access to enter the workspace at all, "
        "cluster-create to start compute, databricks-sql-access for warehouses."
    ),
    Verdict.METASTORE_NOT_ASSIGNED: (
        "The workspace has no Unity Catalog metastore assigned, so no grant "
        "will fix this. Assign one in the account console."
    ),
    Verdict.FEATURE_DISABLED: (
        "The feature is off for this workspace or account, which is a settings "
        "change rather than a permission grant."
    ),
    Verdict.INCONCLUSIVE_BAD_INPUT: (
        "Nothing was measured. Databricks validates the request shape before it "
        "authorizes, so a rejected parameter says nothing about permission. Fix "
        "the query or body and run it again."
    ),
    Verdict.THROTTLED: "Rate limited. Wait and re-run; nothing was measured.",
    Verdict.QUOTA_EXCEEDED: (
        "A limit was reached rather than a permission refused. Check the "
        "workspace's compute quota and the AWS account's EC2 limits in the "
        "region the workspace runs in."
    ),
    Verdict.UNAUTHENTICATED: (
        "The credential was rejected before authorization. An OAuth secret that "
        "has expired, or a lab-user token whose session has ended, both land "
        "here. Nothing was measured."
    ),
    Verdict.ENDPOINT_UNREACHABLE: (
        "The host could not be reached. Check the workspace URL and whether the "
        "workspace has an IP access list."
    ),
    Verdict.UNKNOWN_ENDPOINT: (
        "The path is wrong or the API version has moved. Nothing was measured. "
        "Check the path in the catalog against the current REST reference."
    ),
    Verdict.UNKNOWN_OPERATION: (
        "The operation is not in the built-in catalog and no method and path "
        "were supplied. Add it to a probes file with explicit method and path."
    ),
    Verdict.UNCLASSIFIED: (
        "An error code the classifier does not know. Read the detail and add a "
        "mapping so the next run is not a surprise."
    ),
}


@dataclass(frozen=True)
class ApiOperation:
    """One Databricks REST call, addressed by method and path."""

    method: str
    path: str
    scope: str = WORKSPACE_SCOPE
    query: dict[str, Any] = field(default_factory=dict)
    body: dict[str, Any] = field(default_factory=dict)
    note: str = ""


# The operations a Neo4j workshop lab depends on, grouped by what they establish.
#
# Every entry is a GET, so the default run creates nothing. The mutating
# counterparts a lab actually needs, cluster creation and secret scope creation,
# are reachable through a probes file with --allow-mutating, and are deliberately
# not here: a probe list that can be run without thinking should not be able to
# start compute that bills.
CATALOG: dict[str, ApiOperation] = {
    # Identity and membership. What the lifecycle scripts read to find the
    # student they were invoked for.
    "scim:Me": ApiOperation(
        "GET", ME_PATH, note="The caller's own identity. Always allowed."
    ),
    "scim:ListUsers": ApiOperation(
        "GET",
        "/api/2.0/preview/scim/v2/Users",
        query={"count": 1},
        note="Needed to resolve VOC_DB_USER_EMAIL to a workspace user.",
    ),
    "scim:ListGroups": ApiOperation(
        "GET",
        "/api/2.0/preview/scim/v2/Groups",
        query={"count": 1},
        note="Needed to resolve VOC_DB_GROUP_NAME to a group to grant against.",
    ),
    # The workspace file tree. Where user_setup.sh puts the student's notebooks.
    "workspace:List": ApiOperation(
        "GET", "/api/2.0/workspace/list", query={"path": "/"}
    ),
    "workspace:GetStatus": ApiOperation(
        "GET", "/api/2.0/workspace/get-status", query={"path": "/"}
    ),
    # Compute. A lab that cannot start a cluster is a lab that cannot run.
    "clusters:List": ApiOperation("GET", "/api/2.0/clusters/list"),
    "clusters:SparkVersions": ApiOperation(
        "GET",
        "/api/2.0/clusters/spark-versions",
        note="Runtime versions available, which bounds the Spark connector build.",
    ),
    "clusters:ListNodeTypes": ApiOperation("GET", "/api/2.0/clusters/list-node-types"),
    "clusters:Get": ApiOperation(
        "GET",
        "/api/2.0/clusters/get",
        query={"cluster_id": "0000-000000-doesnotexist"},
        note=(
            "Deliberately names a cluster that does not exist. On AWS a "
            "not-found proves authorization passed; on Databricks it does not, "
            "because a cluster the caller cannot see reports the same thing."
        ),
    ),
    "policies:List": ApiOperation(
        "GET",
        "/api/2.0/policies/clusters/list",
        note="A cluster policy is how a lab caps what a student can spend.",
    ),
    "instance-pools:List": ApiOperation("GET", "/api/2.0/instance-pools/list"),
    # Libraries. The Neo4j Spark connector reaches a cluster as a library.
    "libraries:AllClusterStatuses": ApiOperation(
        "GET", "/api/2.0/libraries/all-cluster-statuses"
    ),
    # Secrets. Where the Aura credentials belong, rather than in notebook text.
    "secrets:ListScopes": ApiOperation("GET", "/api/2.0/secrets/scopes/list"),
    # Jobs and SQL, for labs that run something on a schedule or through a
    # warehouse rather than interactively.
    "jobs:List": ApiOperation("GET", "/api/2.2/jobs/list", query={"limit": 1}),
    "warehouses:List": ApiOperation("GET", "/api/2.0/sql/warehouses"),
    # Unity Catalog. Granted in a different place by a different admin, which is
    # why its denials get their own verdict.
    "unity-catalog:CurrentMetastore": ApiOperation(
        "GET", "/api/2.1/unity-catalog/current-metastore-assignment"
    ),
    "unity-catalog:ListCatalogs": ApiOperation(
        "GET", "/api/2.1/unity-catalog/catalogs"
    ),
    "unity-catalog:ListExternalLocations": ApiOperation(
        "GET", "/api/2.1/unity-catalog/external-locations"
    ),
    # Tokens. Whether a student can mint the credential this tool needs to
    # measure the student at all.
    "token:List": ApiOperation("GET", "/api/2.0/token/list"),
    "workspace-conf:Get": ApiOperation(
        "GET",
        "/api/2.0/workspace-conf",
        query={"keys": "enableTokensConfig"},
        note="Whether personal access tokens are enabled for the workspace.",
    ),
    # Account scope. A different host, and the plane Vocareum itself drives when
    # it creates workspaces and assigns users.
    "account:ListWorkspaces": ApiOperation(
        "GET",
        "/api/2.0/accounts/{account_id}/workspaces",
        scope=ACCOUNT_SCOPE,
        note="Vocareum creates one workspace per group of students through this.",
    ),
    "account:ListMetastores": ApiOperation(
        "GET",
        "/api/2.0/accounts/{account_id}/metastores",
        scope=ACCOUNT_SCOPE,
    ),
    "account:ListUsers": ApiOperation(
        "GET",
        "/api/2.0/accounts/{account_id}/scim/v2/Users",
        scope=ACCOUNT_SCOPE,
        query={"count": 1},
    ),
    "account:ListGroups": ApiOperation(
        "GET",
        "/api/2.0/accounts/{account_id}/scim/v2/Groups",
        scope=ACCOUNT_SCOPE,
        query={"count": 1},
    ),
}


def is_read_only(method: str) -> bool:
    """Report whether an HTTP method changes nothing.

    Everything other than a read is presumed to change something. Databricks has
    read-only POST endpoints, and treating one of those as mutating costs a
    ``--allow-mutating`` flag, while treating a real write as read-only costs a
    cluster nobody meant to start.
    """
    return method.upper() in {"GET", "HEAD"}


@dataclass(frozen=True)
class Probe:
    """One requested measurement, resolved against the catalog."""

    name: str
    operation: ApiOperation | None

    @property
    def method(self) -> str:
        """Return the HTTP method, or the empty string when unresolved."""
        return self.operation.method if self.operation else ""

    def is_safe(self) -> bool:
        """Report whether this probe creates nothing.

        An unresolved probe is safe because it will not be sent.
        """
        return self.operation is None or is_read_only(self.operation.method)


@dataclass
class ProbeResult:
    """The classified outcome of one probe."""

    name: str
    verdict: Verdict
    detail: str
    error_code: str = ""

    @property
    def status(self) -> str:
        """Return the docs/permissions.md tracker Status value for this result."""
        return TRACKER_STATUS.get(self.verdict, "Untested")

    def as_dict(self) -> dict[str, str]:
        """Return a JSON-ready mapping."""
        return {
            "operation": self.name,
            "verdict": str(self.verdict),
            "status": self.status,
            "errorCode": self.error_code,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class Config:
    """Everything the run needs, resolved from the environment."""

    host: str
    account_host: str
    account_id: str
    client_id: str
    client_secret: str
    lab_user_token: str
    expected_workspace_id: str
    source: str

    @property
    def measures_lab_user(self) -> bool:
        """Report whether this run describes the student rather than the tooling."""
        return self.source == "token"

    @property
    def principal_label(self) -> str:
        """Return a one-line description of the principal being measured."""
        if self.measures_lab_user:
            return "the lab user, through a personal access token"
        return f"the service principal {self.client_id}"


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def clip(text: str, limit: int = MAX_DETAIL_CHARS) -> str:
    """Collapse whitespace and trim text, marking any truncation."""
    collapsed = " ".join(str(text).split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def load_config(
    env_path: Path,
    source: str,
    host_override: str | None,
    workspace_id_override: str | None,
    *,
    need_account: bool,
) -> Config:
    """Build a :class:`Config` from real environment variables and ``.env``.

    Which variables are required depends on the source and on whether any probe
    is account-scoped, so the missing-variable message names what this particular
    run needs rather than everything the file documents.
    """
    environment = Environment.from_file(env_path)

    if source == "token":
        required: tuple[str, ...] = ("DATABRICKS_HOST", LAB_USER_TOKEN_VAR)
    elif need_account:
        required = DATABRICKS_ACCOUNT_VARS
    else:
        required = DATABRICKS_WORKSPACE_VARS

    try:
        environment.require(required)
    except ConfigError as error:
        raise ConfigError(
            f"{error}\nCopy .env.example to .env and fill in the values, "
            "or export them in your shell."
        ) from error

    raw_host = host_override or environment.get("DATABRICKS_HOST")
    raw_account_host = (
        environment.get("DATABRICKS_ACCOUNT_HOST") or DEFAULT_ACCOUNT_HOST
    )
    try:
        host = normalize_host(raw_host)
        account_host = normalize_host(raw_account_host)
    except DatabricksHostError as error:
        raise ConfigError(str(error)) from error

    return Config(
        host=host,
        account_host=account_host,
        account_id=environment.get("DATABRICKS_ACCOUNT_ID"),
        client_id=environment.get("DATABRICKS_CLIENT_ID"),
        client_secret=environment.get("DATABRICKS_CLIENT_SECRET"),
        lab_user_token=environment.get(LAB_USER_TOKEN_VAR),
        expected_workspace_id=(
            workspace_id_override or environment.get("DATABRICKS_WORKSPACE_ID")
        ),
        source=source,
    )


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def classify(error_code: str, message: str) -> Verdict:
    """Map a Databricks ``error_code`` and message onto a verdict.

    Takes the pair rather than an exception so the whole classifier is testable
    without a live workspace or a constructed SDK error. ``error_code`` is the
    primary key because the platform sets it on every structured failure; the
    message is read only where one code covers several causes that are fixed in
    different places.

    The order inside ``PERMISSION_DENIED`` is load bearing, for the same reason
    the order inside AWS's ``AccessDenied`` is. A workspace ACL, a Unity Catalog
    privilege, and a workspace entitlement all arrive under that one code, and
    only the message body says which. They are granted by three different
    admins in three different consoles, so collapsing them would turn an
    actionable result into a shrug.
    """
    code = error_code.strip().upper()
    lowered = message.lower()

    # The SCIM endpoints do not use the platform's error codes. They report
    # SCIM_<http status> instead, so SCIM_404 and RESOURCE_DOES_NOT_EXIST mean
    # the same thing arriving under different names. Mapped onto the same
    # verdicts as their platform equivalents below rather than given their own,
    # because the question a verdict answers is what to do next, and that does
    # not change with which API family refused.
    if code in {"PERMISSION_DENIED", "UNAUTHORIZED", "ACCESS_DENIED", "SCIM_403"}:
        # Checked before the generic denial because a metastore that was never
        # assigned refuses every Unity Catalog call in wording that reads like a
        # missing privilege, and no grant will fix it.
        #
        # The discriminator is that an assignment problem is described in terms
        # of assignment or existence, while a privilege problem is described as
        # a named privilege on a named object. Matching "assigned" rather than
        # "not assigned" is deliberate: the wording is "has no Unity Catalog
        # metastore assigned", where the negation sits several words away from
        # the participle it governs.
        if "metastore" in lowered and (
            "assigned" in lowered
            or "no metastore" in lowered
            or "does not exist" in lowered
        ):
            return Verdict.METASTORE_NOT_ASSIGNED
        if (
            "use catalog" in lowered
            or "use schema" in lowered
            or "on catalog" in lowered
            or "on schema" in lowered
            or "metastore" in lowered
            or "unity catalog" in lowered
        ):
            return Verdict.UNITY_CATALOG_DENIED
        if "entitlement" in lowered or "allow-cluster-create" in lowered:
            return Verdict.ENTITLEMENT_MISSING
        return Verdict.PERMISSION_DENIED

    if code in {"UNAUTHENTICATED", "INVALID_TOKEN", "EXPIRED_TOKEN", "SCIM_401"}:
        return Verdict.UNAUTHENTICATED

    if code == "METASTORE_DOES_NOT_EXIST":
        return Verdict.METASTORE_NOT_ASSIGNED

    # A workspace object the caller has no permission on is reported as absent
    # rather than refused, so this code cannot separate "not there" from "not
    # visible to you". Recorded as inconclusive rather than as allowed, which is
    # where the AWS classifier puts the same code.
    #
    # Unity Catalog names the absent thing in the code rather than sending one
    # generic absence. Measured against a live workspace: a missing
    # catalog answers CATALOG_DOES_NOT_EXIST, a schema SCHEMA_DOES_NOT_EXIST, a
    # table TABLE_DOES_NOT_EXIST, and a volume the generic
    # RESOURCE_DOES_NOT_EXIST. Without the three named codes a missing catalog
    # fell through to UNCLASSIFIED, which reads as "the call failed for an
    # unknown reason" rather than "the object is not there", and that is the
    # difference between a ship gate that names what to rebuild and one that
    # tells the reader to go look themselves.
    if code in {
        "RESOURCE_DOES_NOT_EXIST",
        "NOT_FOUND",
        "DOES_NOT_EXIST",
        "CATALOG_DOES_NOT_EXIST",
        "SCHEMA_DOES_NOT_EXIST",
        "TABLE_DOES_NOT_EXIST",
        "METASTORE_ASSIGNMENT_DOES_NOT_EXIST",
        "SCIM_404",
    }:
        return Verdict.NOT_FOUND_OR_HIDDEN

    # The path itself is wrong. Nothing about authorization was measured, and
    # this is worth telling apart from a missing resource because the fix is in
    # the catalog rather than in the workspace.
    if code in {"ENDPOINT_NOT_FOUND", "API_NOT_FOUND"}:
        return Verdict.UNKNOWN_ENDPOINT

    if code in {"FEATURE_DISABLED", "NOT_IMPLEMENTED", "TEMPORARILY_UNAVAILABLE"}:
        return Verdict.FEATURE_DISABLED

    # Request-shape validation is not evidence of authorization. Databricks
    # validates before it authorizes, so a rejected parameter measured against
    # an account admin says nothing at all.
    if code in {
        "INVALID_PARAMETER_VALUE",
        "MALFORMED_REQUEST",
        "BAD_REQUEST",
        "INVALID_STATE",
        "VALIDATION_ERROR",
        "SCIM_400",
    }:
        return Verdict.INCONCLUSIVE_BAD_INPUT

    if code in {"REQUEST_LIMIT_EXCEEDED", "TOO_MANY_REQUESTS", "RATE_LIMIT_EXCEEDED"}:
        return Verdict.THROTTLED

    if code in {"QUOTA_EXCEEDED", "RESOURCE_EXHAUSTED", "RESOURCE_LIMIT_EXCEEDED"}:
        return Verdict.QUOTA_EXCEEDED

    return Verdict.UNCLASSIFIED


def error_pair(error: DatabricksError) -> tuple[str, str]:
    """Return the ``error_code`` and message carried by an SDK error.

    The SDK lifts both onto the exception, but a proxy or a load balancer can
    fail a request before Databricks sees it, and then only the message exists.
    """
    code = str(getattr(error, "error_code", "") or "")
    return code, str(error)


# ---------------------------------------------------------------------------
# Clients and guards
# ---------------------------------------------------------------------------


def guard_credentials(config: Config) -> None:
    """Refuse to continue unless the chosen source carries a real credential.

    Blank is the case that matters. The Databricks SDK reads an empty string as
    "not supplied" and falls through its default provider chain to
    ``DATABRICKS_TOKEN``, then ``~/.databrickscfg``, then the CLI's cached OAuth
    login. A run that silently measures the operator's own admin login and
    prints it as a student's permissions is worse than no run at all.
    """
    if config.measures_lab_user:
        if not config.lab_user_token.strip():
            raise GuardError(
                f"Credential guard refused the run. {LAB_USER_TOKEN_VAR} is empty "
                "or absent.\nGenerate a personal access token from inside a live "
                "student session and export it. Without it the SDK would fall "
                "back to whatever credential the local environment holds, which "
                "is not the student."
            )
        print(f"  {LAB_USER_TOKEN_VAR} = {mask_secret(config.lab_user_token)}")
        print("  Credential guard passed. A lab-user token is present.")
        return

    missing = [
        name
        for name, value in (
            ("DATABRICKS_CLIENT_ID", config.client_id),
            ("DATABRICKS_CLIENT_SECRET", config.client_secret),
        )
        if not value.strip()
    ]
    if missing:
        raise GuardError(
            "Credential guard refused the run. Empty or absent: "
            + ", ".join(missing)
            + ".\nThe Databricks SDK would treat these as unsupplied, fall back "
            "through its default provider chain, and report on whatever "
            "credential the local environment holds. Nothing is reported."
        )
    print(f"  DATABRICKS_CLIENT_ID     = {config.client_id}")
    print(f"  DATABRICKS_CLIENT_SECRET = {mask_secret(config.client_secret)}")
    print("  Credential guard passed. Both OAuth fields carry a value.")


def build_workspace_client(config: Config) -> WorkspaceClient:
    """Build a workspace client for the chosen source.

    Every credential is passed explicitly and ``profile`` is left unset, so the
    client cannot reach ``~/.databrickscfg`` even when one exists. Raises
    :class:`GuardError` on failure, because a client that cannot be built is a
    run that cannot report, and letting the traceback out would exit with no
    verdict and no statement that a refusal happened.
    """
    try:
        if config.measures_lab_user:
            return WorkspaceClient(
                host=config.host,
                token=config.lab_user_token,
                auth_type="pat",
            )
        return WorkspaceClient(
            host=config.host,
            client_id=config.client_id,
            client_secret=config.client_secret,
            auth_type="oauth-m2m",
        )
    except (DatabricksError, ValueError) as error:
        raise GuardError(
            f"The Databricks SDK could not build a workspace client: {error}"
        ) from error


def build_account_client(config: Config) -> AccountClient:
    """Build an account client, which talks to a different host.

    A lab-user token is workspace-scoped, so it cannot reach the account API at
    all. That is refused here rather than reported as a denial, because it is a
    property of the credential type rather than a measurement of the principal.
    """
    if config.measures_lab_user:
        raise GuardError(
            "Account-scoped probes cannot run with --source token. A personal "
            "access token is scoped to one workspace, so the account API would "
            "refuse it for reasons that say nothing about what the lab user may "
            "do. Drop the account probes or use the service principal."
        )
    if not config.account_id.strip():
        raise GuardError(
            "Account-scoped probes need DATABRICKS_ACCOUNT_ID. It is the E2 "
            "account UUID from the account console."
        )
    try:
        return AccountClient(
            host=config.account_host,
            account_id=config.account_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
            auth_type="oauth-m2m",
        )
    except (DatabricksError, ValueError) as error:
        raise GuardError(
            f"The Databricks SDK could not build an account client: {error}"
        ) from error


def guard_host(client: WorkspaceClient, config: Config) -> None:
    """Confirm the client is pointed at the workspace the run was told to measure.

    The host comparison is local, so it catches a typo before any call goes out.
    The workspace ID comparison needs the SDK's own helper, which not every
    version carries, so its absence is reported rather than treated as a pass.
    """
    reached = normalize_host(str(client.config.host or ""))
    print(f"  Configured host = {config.host}")
    print(f"  Client host     = {reached}")
    if reached != config.host:
        raise GuardError(
            f"Host guard refused the run. Configured {config.host}, the client "
            f"resolved {reached}.\nThe SDK found a host somewhere other than "
            "where this run was pointed. Nothing is reported."
        )

    if not config.expected_workspace_id:
        print(
            "  Host guard passed on the host. DATABRICKS_WORKSPACE_ID is unset, "
            "so the numeric workspace ID was not compared."
        )
        return

    resolver = getattr(client, "get_workspace_id", None)
    if resolver is None:
        print(
            "  Host guard passed on the host. This SDK version does not expose "
            "get_workspace_id, so the numeric workspace ID was not compared."
        )
        return

    try:
        reached_id = str(resolver())
    except (DatabricksError, RequestException) as error:
        raise GuardError(
            f"Host guard could not read the workspace ID: {error}"
        ) from error

    print(f"  Workspace ID    = {reached_id}")
    if reached_id != config.expected_workspace_id.strip():
        raise GuardError(
            "Host guard refused the run. Expected workspace "
            f"{config.expected_workspace_id}, reached {reached_id}.\nThis host "
            "resolves to a different workspace than the one configured. Nothing "
            "is reported."
        )
    print(f"  Host guard passed. This is workspace {reached_id}.")


def guard_identity(client: WorkspaceClient, config: Config) -> str:
    """Read the caller's identity and confirm it matches the chosen source.

    Returns the identity. The service principal check is an equality test
    against the configured client ID, because a service principal's SCIM
    ``userName`` is its application ID. For a lab-user token there is nothing to
    compare against, so the identity is reported and the operator confirms it is
    the student they meant.
    """
    try:
        me = client.api_client.do("GET", ME_PATH)
    except (DatabricksError, RequestException) as error:
        raise GuardError(f"Could not read {ME_PATH}: {error}") from error

    if not isinstance(me, dict):
        raise GuardError(
            f"{ME_PATH} returned {type(me).__name__} rather than an object. "
            "Refusing to report against an identity that cannot be read."
        )

    user_name = str(me.get("userName", ""))
    display_name = str(me.get("displayName", ""))
    print(f"  userName    = {user_name or '(absent)'}")
    print(f"  displayName = {display_name or '(absent)'}")

    if config.measures_lab_user:
        print(
            "  Identity guard passed. Confirm the name above is the student this "
            "run is meant to describe."
        )
        return user_name

    if user_name != config.client_id:
        raise GuardError(
            f"Identity guard refused the run. Expected the service principal "
            f"{config.client_id}, reached {user_name or '(absent)'}.\nA service "
            "principal authenticates as its application ID, so a different "
            "userName means a different credential answered. Nothing is "
            "reported."
        )
    print("  Identity guard passed. This is the configured service principal.")
    return user_name


def banner(config: Config, identity: str) -> None:
    """State plainly which principal every result below describes."""
    section("WHAT THIS RUN MEASURES")
    print(f"Principal : {config.principal_label}")
    if identity:
        print(f"Identity  : {identity}")
    print(f"Workspace : {config.host}")
    if config.measures_lab_user:
        print(
            "\nThese results describe the lab user, which is the principal a\n"
            "student's notebook code runs as."
        )
        return
    print(
        "\nThese results describe the service principal, which Vocareum requires\n"
        "to hold Account admin. They say what the lifecycle scripts may do. They\n"
        "say nothing about what a student may do: for that, generate a personal\n"
        "access token inside a live student session and re-run with\n"
        "--source token."
    )


# ---------------------------------------------------------------------------
# Probing
# ---------------------------------------------------------------------------


def looks_empty(response: Any) -> bool:
    """Report whether a successful response carried no objects.

    A Databricks list response is an object holding one array, and an empty
    result is the array being absent rather than present and empty. Both shapes
    appear, so both are treated the same.
    """
    if response is None:
        return True
    if isinstance(response, dict):
        if not response:
            return True
        collections = [
            value for value in response.values() if isinstance(value, (list, dict))
        ]
        if collections and all(not value for value in collections):
            return True
    if isinstance(response, list):
        return not response
    return False


def run_probe(client: Any, probe: Probe, account_id: str) -> ProbeResult:
    """Run one probe and classify whatever comes back."""
    operation = probe.operation
    if operation is None:
        return ProbeResult(
            probe.name,
            Verdict.UNKNOWN_OPERATION,
            clip(
                f"{probe.name} is not in the built-in catalog. Known operations: "
                + ", ".join(sorted(CATALOG))
            ),
        )

    path = operation.path.replace("{account_id}", account_id)
    try:
        response = client.api_client.do(
            operation.method,
            path,
            query=operation.query or None,
            body=operation.body or None,
        )
    except DatabricksError as error:
        code, message = error_pair(error)
        return ProbeResult(
            probe.name, classify(code, message), clip(redact_text(message)), code
        )
    except RequestException as error:
        return ProbeResult(
            probe.name, Verdict.ENDPOINT_UNREACHABLE, clip(redact_text(str(error)))
        )

    if looks_empty(response):
        return ProbeResult(
            probe.name,
            Verdict.ALLOWED_EMPTY,
            clip(f"{operation.method} {path} succeeded and returned nothing"),
        )
    return ProbeResult(
        probe.name,
        Verdict.OK,
        clip(f"{operation.method} {path} succeeded"),
    )


def resolve_probe(name: str) -> Probe:
    """Resolve one operation name against the catalog."""
    return Probe(name=name, operation=CATALOG.get(name))


def load_probes_file(path: Path) -> list[Probe]:
    """Load a JSON list of probe definitions.

    An entry naming only an operation is resolved against the catalog. An entry
    carrying a method and path defines its own, which is how an operation the
    catalog does not know gets measured without editing this file.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConfigError(f"Could not read probes from {path}: {error}") from error
    if not isinstance(payload, list):
        raise ConfigError(f"{path} must hold a JSON list of probe objects.")

    known_fields = {"operation", "method", "path", "query", "body", "scope", "note"}
    probes: list[Probe] = []
    for entry in payload:
        if not isinstance(entry, dict) or "operation" not in entry:
            raise ConfigError(f"Every probe in {path} needs an 'operation' field.")
        unknown = sorted(set(entry) - known_fields)
        if unknown:
            raise ConfigError(
                f"Probe {entry['operation']!r} in {path} has unknown fields: "
                + ", ".join(unknown)
            )
        name = str(entry["operation"])
        if "path" not in entry:
            probes.append(resolve_probe(name))
            continue
        scope = str(entry.get("scope", WORKSPACE_SCOPE))
        if scope not in {WORKSPACE_SCOPE, ACCOUNT_SCOPE}:
            raise ConfigError(
                f"Probe {name!r} in {path} has scope {scope!r}. It must be "
                f"{WORKSPACE_SCOPE!r} or {ACCOUNT_SCOPE!r}."
            )
        probes.append(
            Probe(
                name=name,
                operation=ApiOperation(
                    method=str(entry.get("method", "GET")).upper(),
                    path=str(entry["path"]),
                    scope=scope,
                    query=entry.get("query", {}),
                    body=entry.get("body", {}),
                    note=str(entry.get("note", "")),
                ),
            )
        )
    return probes


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def print_catalog() -> None:
    """Print every operation the catalog knows, with its method and path."""
    section(f"CATALOG ({len(CATALOG)} operations)")
    width = max(len(name) for name in CATALOG)
    for name in sorted(CATALOG):
        operation = CATALOG[name]
        scope = "" if operation.scope == WORKSPACE_SCOPE else f" [{operation.scope}]"
        print(f"{name.ljust(width)}  {operation.method} {operation.path}{scope}")
        if operation.note:
            print(f"{' ' * width}  {operation.note}")


def report(results: list[ProbeResult], as_json: bool) -> None:
    """Print the probe results as a table, then one entry each, then JSON."""
    section("RESULTS")
    width = max((len(item.name) for item in results), default=10)
    print(f"{'Operation'.ljust(width)}  {'Status':20s}  Verdict")
    print(f"{'-' * width}  {'-' * 20}  {'-' * 24}")
    for item in results:
        print(f"{item.name.ljust(width)}  {item.status:20s}  {item.verdict}")

    print()
    for item in results:
        print(f"{item.name}")
        print(f"  verdict: {item.verdict}  ({item.status})")
        if item.error_code:
            print(f"  code:    {item.error_code}")
        if item.detail:
            print(f"  detail:  {item.detail}")
        action = NEXT_ACTION.get(item.verdict)
        if action:
            print(f"  next:    {action}")
        print()

    if as_json:
        section("JSON")
        print(json.dumps([item.as_dict() for item in results], indent=2))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Measure what a principal may do in the Databricks workspace behind "
            "a Vocareum lab."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--probe",
        action="append",
        default=[],
        metavar="SERVICE:OPERATION",
        help="A catalog operation to measure. Repeatable. See --catalog.",
    )
    parser.add_argument(
        "--probes-file",
        type=Path,
        help=(
            "JSON list of probe objects with operation, and optionally method, "
            "path, query, body, scope, note."
        ),
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Measure every read-only operation in the catalog.",
    )
    parser.add_argument(
        "--catalog",
        action="store_true",
        help="Print the built-in operation catalog and exit.",
    )
    parser.add_argument(
        "--source",
        choices=("service-principal", "token"),
        default="service-principal",
        help=(
            "Which principal to measure. service-principal uses the OAuth "
            "credentials from .env and describes the tooling. token uses "
            f"{LAB_USER_TOKEN_VAR} and describes the student. "
            "Default service-principal."
        ),
    )
    parser.add_argument(
        "--host",
        help="Workspace URL to measure. Defaults to DATABRICKS_HOST.",
    )
    parser.add_argument(
        "--expect-workspace-id",
        help=(
            "Numeric workspace ID the host must resolve to. Defaults to "
            "DATABRICKS_WORKSPACE_ID."
        ),
    )
    parser.add_argument(
        "--allow-mutating",
        action="store_true",
        help="Permit probes whose method is not a read.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repository_root() / ".env",
        help="Path to the .env file. Defaults to the repository root.",
    )
    parser.add_argument(
        "--json", action="store_true", help="Also print results as JSON."
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Check the classifier and the guards offline, then exit.",
    )
    return parser.parse_args(argv)


def collect_probes(args: argparse.Namespace) -> list[Probe]:
    """Assemble the probe list from every source the arguments name."""
    probes: list[Probe] = []
    if args.all:
        probes.extend(
            resolve_probe(name)
            for name in sorted(CATALOG)
            if is_read_only(CATALOG[name].method)
        )
    probes.extend(resolve_probe(name) for name in args.probe)
    if args.probes_file:
        probes.extend(load_probes_file(args.probes_file.expanduser()))
    if not probes:
        # scim:Me is the one operation every later probe depends on, so it is
        # the sensible default when nothing was named.
        probes.append(resolve_probe("scim:Me"))
    return probes


def main(argv: list[str] | None = None) -> int:
    """Run the guards, then the probes, and report."""
    args = parse_args(argv)

    if args.self_test:
        # Imported here rather than at module scope because selftest imports
        # this module for the classifier and the guards it checks.
        from .selftest import run_self_test

        return run_self_test()

    if args.catalog:
        print_catalog()
        return EXIT_OK

    try:
        probes = collect_probes(args)
        needs_account = any(
            probe.operation is not None and probe.operation.scope == ACCOUNT_SCOPE
            for probe in probes
        )
        config = load_config(
            args.env_file.expanduser(),
            args.source,
            args.host,
            args.expect_workspace_id,
            need_account=needs_account,
        )
    except ConfigError as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        return EXIT_CONFIG

    unsafe = [probe.name for probe in probes if not probe.is_safe()]
    if unsafe and not args.allow_mutating:
        print(
            "These probes would create or change resources: "
            + ", ".join(unsafe)
            + "\nRe-run with --allow-mutating if that is intended.",
            file=sys.stderr,
        )
        return EXIT_CONFIG

    try:
        section("GUARD 1: confirm the credential")
        guard_credentials(config)

        section("GUARD 2: confirm the workspace")
        workspace = build_workspace_client(config)
        guard_host(workspace, config)

        section("GUARD 3: confirm the identity")
        identity = guard_identity(workspace, config)

        account = build_account_client(config) if needs_account else None
    except (GuardError, DatabricksHostError) as error:
        print(f"\nREFUSED TO REPORT\n{error}", file=sys.stderr)
        return EXIT_GUARD

    banner(config, identity)

    section(f"PROBES ({len(probes)})")
    results: list[ProbeResult] = []
    for probe in probes:
        print(f"  {probe.name} ...")
        scope = probe.operation.scope if probe.operation else WORKSPACE_SCOPE
        client = account if scope == ACCOUNT_SCOPE else workspace
        results.append(run_probe(client, probe, config.account_id))

    report(results, args.json)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
