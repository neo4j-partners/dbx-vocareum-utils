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
}


def load_manifest(path: Path) -> list[Expectation]:
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
            expectations.append(Expectation(kind=kind, name=name.strip()))

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


def check_all(client: WorkspaceClient, expectations: list[Expectation]) -> list[Result]:
    """Check every expectation, in the order the manifest gave them."""
    return [check(client, expectation) for expectation in expectations]
