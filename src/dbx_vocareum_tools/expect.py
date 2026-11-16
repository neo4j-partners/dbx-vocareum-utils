"""Check that the objects a course's hooks were meant to build actually exist.

This exists because ``voccustomdata.txt`` may not surface. The Databricks
session record was measured to carry no ``voccustomdata`` (measured on one
part), so an ``error_code`` a hook writes can go nowhere. When the channel a hook
uses to say what it did is gone, what is left is the workspace itself.

So the question "did the lab come up" gets answered the other way round. Rather
than asking the hooks what they did, name what they were supposed to build and
go look. That turns a ship gate from "an operator remembers to check six things
in the console" into one command with an exit code, and it keeps working every
time the course is redeployed rather than only on the day somebody looked.

The manifest is the course's, not this package's. ``dbx-vocareum`` knows how to
ask whether a catalog named X exists; it must never know what X is for one
course, because the whole point of the split is that a
second course reuses this file unchanged. See ``load_manifest`` for the shape.

**Absence here is weaker evidence than it looks.** Databricks answers a caller
with no permission on an object by saying the object does not exist, which is
why :class:`Presence` says ``ABSENT_OR_HIDDEN`` rather than ``ABSENT``. Run as
the service principal, which holds account admin, hidden is unlikely and it is
still not measured, and a gate that printed "absent" would send a reader to
recreate an object that is already there.

Endpoints were measured against a live workspace rather than read off a doc
page. Both families answered:

- get-by-name, one GET that either returns the object or raises: catalogs,
  schemas, tables, volumes, and workspace paths.
- list-and-match, one GET returning a page that has to be searched: SQL
  warehouses, pipelines, and Genie spaces.

A library policy is the one kind that is more than a lookup. The policy existing
says nothing about whether a student can import anything, so the check follows
it onto the clusters that carry it and reads each one's library status. See
``_check_library_policy``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from requests.exceptions import RequestException

from .probe import Verdict, classify, error_pair
from .security import redact_text


class ManifestError(Exception):
    """The manifest could not be read, or names something uncheckable."""


class Presence(StrEnum):
    """What one lookup concluded about one named object.

    ``NOT_CHECKED`` is deliberately distinct from ``ABSENT_OR_HIDDEN``. A call
    that was throttled, denied, or never completed says something about the
    caller rather than about the object, and folding it into absence would fail
    a gate over a network blip and send the reader to rebuild a catalog that was
    never in question.
    """

    PRESENT = "present"
    ABSENT_OR_HIDDEN = "absent or hidden"
    NOT_CHECKED = "not checked"
    # The object is there and is not yet doing its job. Used by the library
    # policy only: a policy found by name whose libraries have not installed on
    # the clusters that carry it. Its own value rather than PRESENT, because a
    # lab whose first import fails is not a lab that came up. Its own value
    # rather than ABSENT_OR_HIDDEN, because that sends a reader to create a
    # policy that already exists.
    NOT_READY = "present, not ready"


@dataclass(frozen=True)
class Expectation:
    """One object a course says its hooks build."""

    kind: str
    name: str

    def __str__(self) -> str:
        return f"{self.kind} {self.name}"


@dataclass(frozen=True)
class Result:
    """What one expectation turned out to be, and why."""

    expectation: Expectation
    presence: Presence
    detail: str = ""

    @property
    def satisfied(self) -> bool:
        """Whether this expectation was met. Only ``PRESENT`` counts."""
        return self.presence is Presence.PRESENT


# ---------------------------------------------------------------------------
# The manifest
# ---------------------------------------------------------------------------

# Each key names a kind of object and takes a list of names. Plural, because the
# file reads as a list of things that should exist rather than as configuration.
# The value shape a name takes is the kind's own: a catalog is one word, a table
# is three joined by dots, a workspace path is a path.
MANIFEST_KEYS = {
    "catalogs": "catalog",
    "schemas": "schema",
    "tables": "table",
    "volumes": "volume",
    "warehouses": "warehouse",
    "pipelines": "pipeline",
    "genie_spaces": "genie space",
    "workspace_paths": "workspace path",
    "library_policies": "library policy",
}


WORKSPACE_ID_PLACEHOLDER = "${WORKSPACE_ID}"


def load_manifest(path: Path, workspace_id: str = "") -> list[Expectation]:
    """Read a course's manifest into a flat list of expectations.

    The file is JSON holding one object whose keys are drawn from
    :data:`MANIFEST_KEYS` and whose values are lists of names::

        {
          "catalogs": ["databricks-neo4j-workshop"],
          "warehouses": ["shared_warehouse"],
          "pipelines": ["Fleet Digital Twin ETL"]
        }

    An unknown key is an error rather than something to skip. A misspelled key
    that is quietly ignored is an expectation nobody checks, and the gate reports
    a pass it never measured, which is worse than having no gate.

    A name may hold ``${WORKSPACE_ID}``, which becomes ``workspace_id``. A course
    whose schemas are named for the workspace, because every workspace of a class
    shares one metastore, cannot write those names down in advance. With no
    workspace ID to put in, the manifest is refused: checking the literal text
    would report absence of an object nobody ever meant to create.
    """
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ManifestError(f"{path} could not be read: {error}") from error
    except json.JSONDecodeError as error:
        raise ManifestError(f"{path} is not valid JSON: {error}") from error

    if not isinstance(raw, dict):
        raise ManifestError(
            f"{path} must hold one JSON object keyed by kind, not "
            f"a {type(raw).__name__}."
        )

    unknown = sorted(set(raw) - set(MANIFEST_KEYS))
    if unknown:
        raise ManifestError(
            f"{path} names {', '.join(unknown)}, which this tool cannot check. "
            f"Known keys: {', '.join(sorted(MANIFEST_KEYS))}."
        )

    expectations: list[Expectation] = []
    for key, kind in MANIFEST_KEYS.items():
        names = raw.get(key, [])
        if not isinstance(names, list):
            raise ManifestError(f"{path}: {key} must be a list of names.")
        for name in names:
            if not isinstance(name, str) or not name.strip():
                raise ManifestError(f"{path}: {key} holds a name that is not text.")
            name = name.strip()
            if WORKSPACE_ID_PLACEHOLDER in name:
                if not workspace_id:
                    raise ManifestError(
                        f"{path}: {name} needs a workspace ID and none is known. "
                        "Set DATABRICKS_WORKSPACE_ID or let the workspace read "
                        "resolve it."
                    )
                name = name.replace(WORKSPACE_ID_PLACEHOLDER, workspace_id)
            expectations.append(Expectation(kind=kind, name=name))

    if not expectations:
        raise ManifestError(
            f"{path} names nothing to check. A manifest that expects nothing "
            "passes every time and says nothing about the lab."
        )
    return expectations


# ---------------------------------------------------------------------------
# Looking one object up
# ---------------------------------------------------------------------------

# Unity Catalog addresses these by full name in the path, so one template each
# is the whole implementation. Measured: a present object returns its
# record, and an absent one raises with a message naming what was looked for.
GET_BY_NAME_PATHS = {
    "catalog": "/api/2.1/unity-catalog/catalogs/{name}",
    "schema": "/api/2.1/unity-catalog/schemas/{name}",
    "table": "/api/2.1/unity-catalog/tables/{name}",
    "volume": "/api/2.1/unity-catalog/volumes/{name}",
}

# The kinds with no get-by-name call, which have to be listed and searched. The
# query keeps the page from being unbounded on a busy workspace; a course object
# missing from the first page reads as absent, which is why the page is large
# rather than default-sized.
LIST_AND_MATCH = {
    "warehouse": ("/api/2.0/sql/warehouses", {}),
    "pipeline": ("/api/2.0/pipelines", {"max_results": 100}),
    "genie space": ("/api/2.0/genie/spaces", {"page_size": 100}),
}

# Which fields in a listed record might carry the name a course wrote down.
# Several, because the three endpoints do not agree: a warehouse and a pipeline
# both answer to ``name`` and a Genie space is titled. Checked in this order.
NAME_FIELDS = ("name", "title", "display_name")

WORKSPACE_STATUS_PATH = "/api/2.0/workspace/get-status"

CLUSTER_POLICIES_PATH = "/api/2.0/policies/clusters/list"
CLUSTERS_PATH = "/api/2.0/clusters/list"
LIBRARY_STATUS_PATH = "/api/2.0/libraries/cluster-status"

# A cluster in one of these states is up or on its way up. The same set
# voclab.py uses, restated because the lab runtime is package data and cannot be
# imported from here.
LIVE_CLUSTER_STATES = frozenset({"RUNNING", "PENDING", "RESIZING", "RESTARTING"})


def check(client: WorkspaceClient, expectation: Expectation) -> Result:
    """Look one expectation up and say what came back."""
    if expectation.kind in GET_BY_NAME_PATHS:
        path = GET_BY_NAME_PATHS[expectation.kind].format(name=expectation.name)
        return _get_one(client, expectation, path)
    if expectation.kind == "workspace path":
        return _get_one(
            client,
            expectation,
            WORKSPACE_STATUS_PATH,
            query={"path": expectation.name},
        )
    if expectation.kind in LIST_AND_MATCH:
        return _list_and_match(client, expectation)
    if expectation.kind == "library policy":
        return _check_library_policy(client, expectation)
    return Result(
        expectation,
        Presence.NOT_CHECKED,
        f"no lookup is defined for a {expectation.kind}",
    )


def _get_one(
    client: WorkspaceClient,
    expectation: Expectation,
    path: str,
    query: dict[str, Any] | None = None,
) -> Result:
    """Run a get-by-name call and turn its outcome into a :class:`Result`."""
    try:
        client.api_client.do("GET", path, query=query)
    except DatabricksError as error:
        code, message = error_pair(error)
        if classify(code, message) is Verdict.NOT_FOUND_OR_HIDDEN:
            return Result(expectation, Presence.ABSENT_OR_HIDDEN, redact_text(message))
        return Result(expectation, Presence.NOT_CHECKED, redact_text(message))
    except RequestException as error:
        return Result(expectation, Presence.NOT_CHECKED, redact_text(str(error)))
    return Result(expectation, Presence.PRESENT)


def _list_and_match(client: WorkspaceClient, expectation: Expectation) -> Result:
    """List a kind and look for the expected name in the page that comes back.

    The listing is searched rather than indexed by a known key. Measured:
    ``/api/2.0/pipelines`` and ``/api/2.0/genie/spaces`` both answer
    an empty workspace with ``{}`` and omit the list key entirely, so the key's
    name could not be measured without creating the objects the gate exists to
    check for. Searching every list the payload holds gets the same answer
    without a write, and it survives the key being renamed.
    """
    path, query = LIST_AND_MATCH[expectation.kind]
    try:
        payload = client.api_client.do("GET", path, query=query or None)
    except DatabricksError as error:
        # No NOT_FOUND branch here. A listing that answers at all has told us
        # the truth about every name in it, and a listing that fails has told us
        # nothing, so there is no third outcome to draw out of the code.
        return Result(
            expectation, Presence.NOT_CHECKED, redact_text(error_pair(error)[1])
        )
    except RequestException as error:
        return Result(expectation, Presence.NOT_CHECKED, redact_text(str(error)))

    if not isinstance(payload, dict):
        return Result(
            expectation, Presence.NOT_CHECKED, "the listing was not a JSON object"
        )

    for value in payload.values():
        if not isinstance(value, list):
            continue
        for item in value:
            if not isinstance(item, dict):
                continue
            for field in NAME_FIELDS:
                if str(item.get(field, "")).strip() == expectation.name:
                    return Result(expectation, Presence.PRESENT)
    return Result(
        expectation,
        Presence.ABSENT_OR_HIDDEN,
        "no object of that kind in this workspace carries that name",
    )


class _Unreadable(Exception):
    """A listing that answered but could not be read to the end."""


def _library_key(library: Any) -> str:
    """A stable identity for a library specification, for comparing two lists."""
    return json.dumps(library, sort_keys=True)


def _library_label(library: Any) -> str:
    """Name a library the way ``VOC_COURSE_LIBRARIES`` would, for a detail line."""
    if isinstance(library, dict) and len(library) == 1:
        kind, value = next(iter(library.items()))
        if isinstance(value, dict):
            value = value.get("package") or value.get("coordinates")
        if isinstance(value, str) and value:
            return f"{kind}:{value}"
    return _library_key(library)


def _list_clusters(client: WorkspaceClient) -> list[dict[str, Any]]:
    """Every cluster in the workspace, following ``next_page_token``.

    Every page, because a class of students is one cluster each, and a page
    boundary that hid half of them would pass the gate on the half it read.
    """
    clusters: list[dict[str, Any]] = []
    query: dict[str, Any] | None = None
    seen: set[str] = set()
    while True:
        payload = client.api_client.do("GET", CLUSTERS_PATH, query=query)
        if not isinstance(payload, dict):
            raise _Unreadable("clusters/list was not a JSON object")
        clusters.extend(c for c in payload.get("clusters") or [] if isinstance(c, dict))
        token = payload.get("next_page_token")
        if not token:
            return clusters
        if token in seen:
            raise _Unreadable("clusters/list returned the same page token twice")
        seen.add(token)
        query = {"page_token": token}


def _cluster_problems(
    client: WorkspaceClient, cluster: dict[str, Any], libraries: list[Any]
) -> list[str]:
    """Say what is wrong with one cluster's libraries, or nothing if all installed.

    A cluster that is not RUNNING is reported as such and not read. Nothing has
    installed on a cluster that is still starting, so reading it would only say
    PENDING at greater cost.
    """
    name = cluster.get("cluster_name") or cluster.get("cluster_id") or "a cluster"
    state = cluster.get("state") or "UNKNOWN"
    if state != "RUNNING":
        return [f"{name} is {state}"]
    payload = client.api_client.do(
        "GET", LIBRARY_STATUS_PATH, query={"cluster_id": cluster.get("cluster_id")}
    )
    statuses = {
        _library_key(entry.get("library")): entry
        for entry in (payload or {}).get("library_statuses") or []
        if isinstance(entry, dict)
    }
    problems = []
    for library in libraries:
        entry = statuses.get(_library_key(library))
        if entry is None:
            problems.append(f"{name}: {_library_label(library)} not listed")
            continue
        status = entry.get("status") or "UNKNOWN"
        if status != "INSTALLED":
            messages = entry.get("messages") or []
            first = str(messages[0]).splitlines()[0] if messages else ""
            reason = f" ({first})" if first else ""
            problems.append(f"{name}: {_library_label(library)} {status}{reason}")
    return problems


def _check_library_policy(client: WorkspaceClient, expectation: Expectation) -> Result:
    """Follow a library policy onto the clusters that carry it.

    ``PRESENT`` only when every library the policy names is ``INSTALLED`` on
    every RUNNING cluster carrying it, and at least one live cluster does.
    ``NOT_READY`` covers a policy naming no libraries, no live cluster carrying
    the policy, a cluster still starting, and a library that is pending, failed,
    or not listed. Absence of the policy itself is ``ABSENT_OR_HIDDEN`` and a
    failed call is ``NOT_CHECKED``, as for every other kind.

    Matching a cluster to the policy reads ``policy_id`` off ``clusters/list``.
    Whether that listing reports ``policy_id`` is unmeasured. If it does not,
    this reads as no cluster carrying the policy, and the detail says so rather
    than calling the gap a fault in the lab.

    Measured 2026-11-16: the policy listing returns a ``libraries`` key when
    the policy has libraries and omits it when it has none, and a policy's
    libraries appear in ``libraries/cluster-status`` as PENDING and then
    INSTALLED.
    """
    try:
        payload = client.api_client.do("GET", CLUSTER_POLICIES_PATH)
        policy = next(
            (
                item
                for item in (payload or {}).get("policies") or []
                if isinstance(item, dict) and item.get("name") == expectation.name
            ),
            None,
        )
        if policy is None:
            return Result(
                expectation,
                Presence.ABSENT_OR_HIDDEN,
                "no cluster policy in this workspace carries that name",
            )
        libraries = policy.get("libraries") or []
        if not libraries:
            return Result(
                expectation, Presence.NOT_READY, "the policy names no libraries"
            )

        policy_id = policy.get("policy_id")
        carrying = [
            cluster
            for cluster in _list_clusters(client)
            if cluster.get("policy_id") == policy_id
            and cluster.get("state") in LIVE_CLUSTER_STATES
        ]
        if not carrying:
            return Result(
                expectation,
                Presence.NOT_READY,
                "no running or starting cluster carries this policy, so no "
                "student has its libraries. Run the gate while a student's "
                "session is live",
            )

        problems = [
            problem
            for cluster in carrying
            for problem in _cluster_problems(client, cluster, libraries)
        ]
    except DatabricksError as error:
        return Result(
            expectation, Presence.NOT_CHECKED, redact_text(error_pair(error)[1])
        )
    except (RequestException, _Unreadable) as error:
        return Result(expectation, Presence.NOT_CHECKED, redact_text(str(error)))

    if problems:
        return Result(expectation, Presence.NOT_READY, "; ".join(problems))
    return Result(
        expectation,
        Presence.PRESENT,
        f"{len(libraries)} libraries installed on {len(carrying)} cluster(s)",
    )


def check_all(client: WorkspaceClient, expectations: list[Expectation]) -> list[Result]:
    """Check every expectation, in the order the manifest gave them."""
    return [check(client, expectation) for expectation in expectations]
