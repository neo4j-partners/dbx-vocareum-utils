"""What ``expect`` concludes about an object it went looking for.

The gate this backs replaces a channel that may not exist. The Databricks
session record was measured to carry no ``voccustomdata`` (measured on one
part), so a hook cannot report its own failure and the only remaining evidence is
whether the objects it was supposed to build are there. That makes the
distinctions below load bearing rather than fussy: an object that is absent, an
object that could not be looked up, and a manifest that never ran are three
different answers, and only the first is a statement about the lab.

Offline. The workspace client is a stub whose ``api_client.do`` answers from a
table, so every branch is exercised without a live workspace.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from databricks.sdk.errors import DatabricksError

from dbx_vocareum_tools.expect import (
    Expectation,
    ManifestError,
    Presence,
    check,
    check_all,
    load_manifest,
)


class Stub:
    """A workspace client whose calls answer from a table, or raise.

    Keys are the request path. A value that is an exception is raised, anything
    else is returned as the payload, and an unlisted path raises the not-found
    error Databricks sends for an object that is absent, which is the common
    case and the one worth having as the default.
    """

    def __init__(self, answers: dict[str, Any]) -> None:
        self.answers = answers
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.api_client = self

    def do(self, method: str, path: str, query: dict[str, Any] | None = None) -> Any:
        self.calls.append((path, query))
        answer = self.answers.get(path, not_found(path))
        if isinstance(answer, Exception):
            raise answer
        return answer


def not_found(what: str) -> DatabricksError:
    """The error Databricks raises for an object that is not there."""
    return DatabricksError(f"{what} does not exist.", error_code="NOT_FOUND")


def denied() -> DatabricksError:
    """A failure that says something about the caller, not about the object."""
    return DatabricksError("Rate limit exceeded", error_code="REQUEST_LIMIT_EXCEEDED")


# ---------------------------------------------------------------------------
# The manifest
# ---------------------------------------------------------------------------


def write(tmp_path: Path, body: Any) -> Path:
    path = tmp_path / "expected.json"
    path.write_text(
        body if isinstance(body, str) else json.dumps(body), encoding="utf-8"
    )
    return path


def test_a_manifest_becomes_one_flat_list_of_expectations(tmp_path: Path) -> None:
    path = write(
        tmp_path,
        {
            "catalogs": ["workshop"],
            "warehouses": ["shared_warehouse"],
            "pipelines": ["Fleet Digital Twin ETL"],
        },
    )

    assert load_manifest(path) == [
        Expectation("catalog", "workshop"),
        Expectation("warehouse", "shared_warehouse"),
        Expectation("pipeline", "Fleet Digital Twin ETL"),
    ]


def test_an_unknown_key_is_refused_rather_than_skipped(tmp_path: Path) -> None:
    # The whole reason this is an error. A key nobody checks is an expectation
    # that always passes, and a gate reporting a pass it never measured is worse
    # than no gate, because it is believed.
    path = write(tmp_path, {"catalogs": ["workshop"], "clusters": ["mine"]})

    with pytest.raises(ManifestError, match="clusters"):
        load_manifest(path)


def test_a_manifest_that_expects_nothing_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="names nothing"):
        load_manifest(write(tmp_path, {"catalogs": []}))


def test_a_manifest_that_is_not_json_says_so(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="not valid JSON"):
        load_manifest(write(tmp_path, "{not json"))


def test_a_manifest_that_is_not_an_object_says_so(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="one JSON object"):
        load_manifest(write(tmp_path, ["workshop"]))


def test_a_missing_manifest_is_an_error_and_not_an_empty_one(tmp_path: Path) -> None:
    with pytest.raises(ManifestError, match="could not be read"):
        load_manifest(tmp_path / "nothing.json")


# ---------------------------------------------------------------------------
# Looking one object up
# ---------------------------------------------------------------------------


def test_a_catalog_that_exists_reads_as_present() -> None:
    client = Stub({"/api/2.1/unity-catalog/catalogs/workshop": {"name": "workshop"}})

    result = check(client, Expectation("catalog", "workshop"))

    assert result.presence is Presence.PRESENT
    assert result.satisfied


def test_a_catalog_that_does_not_exist_reads_as_absent_or_hidden() -> None:
    # Named absent-or-hidden rather than absent on purpose: Databricks answers a
    # caller with no permission on an object by saying it is not there, so the
    # two arrive identically and the name has to carry the doubt.
    result = check(Stub({}), Expectation("catalog", "workshop"))

    assert result.presence is Presence.ABSENT_OR_HIDDEN
    assert not result.satisfied


def test_a_call_that_fails_for_another_reason_is_not_called_absent() -> None:
    # A throttle says something about the caller. Filing it as absence would
    # send a reader to rebuild a catalog that was never in question.
    client = Stub({"/api/2.1/unity-catalog/catalogs/workshop": denied()})

    result = check(client, Expectation("catalog", "workshop"))

    assert result.presence is Presence.NOT_CHECKED
    assert not result.satisfied


def test_a_table_is_addressed_by_its_full_name() -> None:
    full = "workshop.aircraft.fleet_health"
    client = Stub({f"/api/2.1/unity-catalog/tables/{full}": {"name": "fleet_health"}})

    assert check(client, Expectation("table", full)).presence is Presence.PRESENT


def test_a_workspace_path_is_asked_for_as_a_query_parameter() -> None:
    # The one get-by-name kind whose name does not go in the path. Getting this
    # wrong would ask for the status of the endpoint itself and pass forever.
    client = Stub({"/api/2.0/workspace/get-status": {"object_type": "NOTEBOOK"}})

    result = check(client, Expectation("workspace path", "/Shared/workshop/etl"))

    assert result.presence is Presence.PRESENT
    assert client.calls == [
        ("/api/2.0/workspace/get-status", {"path": "/Shared/workshop/etl"})
    ]


# ---------------------------------------------------------------------------
# The kinds that have to be listed and searched
# ---------------------------------------------------------------------------


def test_a_warehouse_is_found_by_name_inside_the_listing() -> None:
    client = Stub(
        {
            "/api/2.0/sql/warehouses": {
                "warehouses": [
                    {"id": "a", "name": "Serverless Starter Warehouse"},
                    {"id": "b", "name": "shared_warehouse"},
                ]
            }
        }
    )

    assert (
        check(client, Expectation("warehouse", "shared_warehouse")).presence
        is Presence.PRESENT
    )


def test_a_genie_space_is_matched_on_its_title() -> None:
    # Measured: the three listed kinds do not agree on which field
    # carries the name, and an empty workspace omits the list key entirely, so
    # the payload is searched rather than indexed.
    client = Stub({"/api/2.0/genie/spaces": {"spaces": [{"title": "Fleet"}]}})

    assert (
        check(client, Expectation("genie space", "Fleet")).presence is Presence.PRESENT
    )


def test_an_empty_listing_reads_as_absent_rather_than_as_a_failure() -> None:
    # The measured shape of an empty workspace: {} with no list key at all.
    client = Stub({"/api/2.0/pipelines": {}})

    result = check(client, Expectation("pipeline", "Fleet Digital Twin ETL"))

    assert result.presence is Presence.ABSENT_OR_HIDDEN


def test_a_listing_that_cannot_be_read_is_not_called_absent() -> None:
    client = Stub({"/api/2.0/pipelines": denied()})

    assert (
        check(client, Expectation("pipeline", "anything")).presence
        is Presence.NOT_CHECKED
    )


def test_a_near_miss_on_the_name_does_not_count_as_a_match() -> None:
    # A course renaming its pipeline and forgetting the manifest should fail the
    # gate, which only happens if the comparison stays exact.
    client = Stub({"/api/2.0/pipelines": {"statuses": [{"name": "Fleet ETL v2"}]}})

    assert (
        check(client, Expectation("pipeline", "Fleet ETL")).presence
        is Presence.ABSENT_OR_HIDDEN
    )


def test_every_expectation_is_checked_and_the_order_is_kept() -> None:
    client = Stub({"/api/2.1/unity-catalog/catalogs/there": {"name": "there"}})
    expectations = [
        Expectation("catalog", "there"),
        Expectation("catalog", "gone"),
    ]

    results = check_all(client, expectations)

    assert [result.presence for result in results] == [
        Presence.PRESENT,
        Presence.ABSENT_OR_HIDDEN,
    ]
    assert [result.expectation for result in results] == expectations
