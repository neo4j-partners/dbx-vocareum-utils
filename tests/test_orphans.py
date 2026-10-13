"""Orphaned home-folder tests.

This is the only tool in the repository that deletes something a student could
still be using, so the tests are weighted the way ``test_session.py`` is: the
reads lightly, the refusals and the skips heavily. A wrong read prints a wrong
line. A wrong delete takes a running student's notebooks.

The client is faked rather than mocked at the HTTP layer, in the idiom
``tests/test_diagnose.py`` uses: one object that is its own ``api_client`` and
answers ``do`` from a table. It also records every call, because most of what
matters here is not the value a function returns but which calls it did and did
not make.
"""

from __future__ import annotations

from typing import Any

import pytest
from databricks.sdk.errors import DatabricksError

from dbx_vocareum_tools import orphans

LIVE_STUDENT = "labuser10000001_1785962713@vocareum.com"
GONE_STUDENT = "labuser10000001_1785900000@vocareum.com"
INSTRUCTOR = "instructor@example.com"

HOST = "https://dbc-33333333-3333.cloud.databricks.com"


class FakeClient:
    """A workspace client that answers ``/Users`` and SCIM from a table.

    ``folders`` is mutated by a successful delete, so the re-listing at the end
    of a run observes the same thing a real one would rather than replaying the
    listing the run started from.
    """

    def __init__(
        self,
        folders: list[str],
        live: list[str],
        fail_on: tuple[str, ...] = (),
    ) -> None:
        self.api_client = self
        self.folders = list(folders)
        self.live = list(live)
        self.fail_on = fail_on
        self.calls: list[tuple[str, str, Any]] = []

    def do(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append((method, path, body or query))
        if path == orphans.USERS_SCIM_PATH:
            return {"Resources": [{"userName": name} for name in self.live]}
        if path == orphans.LIST_PATH:
            return {"objects": [{"path": folder} for folder in self.folders]}
        if path == orphans.DELETE_PATH:
            target = (body or {})["path"]
            if target in self.fail_on:
                raise DatabricksError(
                    f"{target} could not be deleted.", error_code="IO_ERROR"
                )
            self.folders.remove(target)
            return {}
        raise AssertionError(f"unexpected call {method} {path}")

    @property
    def deleted(self) -> list[str]:
        return [
            call[2]["path"]
            for call in self.calls
            if call[1] == orphans.DELETE_PATH and call[2]
        ]


def workspace(
    folders: list[str] | None = None,
    live: list[str] | None = None,
    fail_on: tuple[str, ...] = (),
) -> FakeClient:
    """The measured shape: several lab folders against one live identity.

    Measured: one workspace held nine ``/Users/labuser*`` folders against
    exactly one live identity. Three is that situation
    with the arithmetic made readable.
    """
    if folders is None:
        folders = [
            f"/Users/{INSTRUCTOR}",
            f"/Users/{LIVE_STUDENT}",
            f"/Users/{GONE_STUDENT}",
        ]
    if live is None:
        live = [INSTRUCTOR, LIVE_STUDENT]
    return FakeClient(folders, live, fail_on)


def run(client: FakeClient, **overrides: Any) -> int:
    arguments: dict[str, Any] = {
        "prefix": orphans.DEFAULT_PREFIX,
        "host": HOST,
        "do_delete": False,
        "assume_yes": False,
    }
    arguments.update(overrides)
    return orphans.run(client, **arguments)


# --- the safety property, which is the point of the tool ----------------------


def test_a_folder_whose_identity_is_live_is_never_deleted(capsys) -> None:
    # The student may be sitting in a running session with unsaved work in
    # exactly this folder.
    client = workspace()
    assert run(client, do_delete=True, assume_yes=True) == orphans.EXIT_OK
    assert client.deleted == [f"/Users/{GONE_STUDENT}"]
    assert "SKIP" in capsys.readouterr().out


def test_the_identities_are_read_before_anything_is_deleted() -> None:
    # A delete issued before the listing that authorizes it is a delete made on
    # no evidence, whatever the code around it looks like.
    client = workspace()
    run(client, do_delete=True, assume_yes=True)
    paths = [call[1] for call in client.calls]
    assert paths.index(orphans.USERS_SCIM_PATH) < paths.index(orphans.DELETE_PATH)


def test_a_folder_outside_the_prefix_is_not_touched() -> None:
    # The instructor's own folder is under /Users and is not a lab identity.
    client = workspace()
    run(client, do_delete=True, assume_yes=True)
    assert f"/Users/{INSTRUCTOR}" not in client.deleted


def test_the_prefix_matches_the_folder_name_rather_than_the_whole_path() -> None:
    # A substring test would match a real person whose address happens to
    # contain the word, and this tool must never be why such a folder went away.
    folders = ["/Users/notalabuser@example.com", f"/Users/{GONE_STUDENT}"]
    classified = orphans.classify_folders(folders, set(), orphans.DEFAULT_PREFIX)
    assert [folder.path for folder in classified] == [f"/Users/{GONE_STUDENT}"]


# --- deleting, when it is asked for -------------------------------------------


def test_orphans_are_deleted_and_the_body_is_recursive() -> None:
    client = workspace(
        folders=[f"/Users/{GONE_STUDENT}", "/Users/labuser999_1@vocareum.com"],
        live=[INSTRUCTOR],
    )
    assert run(client, do_delete=True, assume_yes=True) == orphans.EXIT_OK
    deletes = [call for call in client.calls if call[1] == orphans.DELETE_PATH]
    assert len(deletes) == 2
    for _method, _path, body in deletes:
        assert body["recursive"] is True


def test_the_result_is_re_listed_rather_than_assumed() -> None:
    # The deletes were issued against a listing that is out of date by the time
    # they finish, so what the workspace holds is read again.
    client = workspace()
    run(client, do_delete=True, assume_yes=True)
    listings = [call for call in client.calls if call[1] == orphans.LIST_PATH]
    assert len(listings) == 2
    assert client.folders == [f"/Users/{INSTRUCTOR}", f"/Users/{LIVE_STUDENT}"]


def test_one_failed_folder_does_not_abort_the_rest(capsys) -> None:
    # A half-cleaned workspace with no report is worse than a fully reported
    # partial one.
    first = f"/Users/{GONE_STUDENT}"
    second = "/Users/labuser999_1@vocareum.com"
    client = workspace(folders=[first, second], live=[INSTRUCTOR], fail_on=(first,))
    exit_code = run(client, do_delete=True, assume_yes=True)
    assert client.deleted == [first, second]
    assert client.folders == [first]
    output = capsys.readouterr().out
    assert "FAIL" in output
    assert "DEL" in output
    # The failure is carried into the exit code, because a scripted run that
    # reports success after failing to delete is a lie nobody reads.
    assert exit_code == orphans.EXIT_API


# --- the default, which deletes nothing ---------------------------------------


def test_the_default_run_deletes_nothing(capsys) -> None:
    client = workspace()
    assert run(client) == orphans.EXIT_OK
    assert client.deleted == []
    output = capsys.readouterr().out
    assert orphans.ORPHAN in output
    assert orphans.LIVE in output
    assert "Nothing was deleted" in output


def test_a_workspace_with_no_lab_folders_reports_zero_and_deletes_nothing(
    capsys,
) -> None:
    client = workspace(folders=[f"/Users/{INSTRUCTOR}"], live=[INSTRUCTOR])
    assert run(client, do_delete=True, assume_yes=True) == orphans.EXIT_OK
    assert client.deleted == []
    assert "Nothing to do" in capsys.readouterr().out


def test_an_empty_users_listing_is_not_an_error() -> None:
    # A workspace whose /Users holds nothing answers with an empty body rather
    # than an empty objects array.
    client = FakeClient([], [])
    assert orphans.list_users_root(client) == []
    assert run(client, do_delete=True, assume_yes=True) == orphans.EXIT_OK


# --- the refusals -------------------------------------------------------------


def test_a_delete_with_nothing_attached_to_answer_is_refused() -> None:
    # confirm_delete reads stdin, and pytest gives it a stream that is not a
    # tty, which is the same case as a cron job or a CI run.
    orphan = orphans.HomeFolder(f"/Users/{GONE_STUDENT}", GONE_STUDENT, live=False)
    assert orphans.confirm_delete([orphan], HOST) is False


def test_a_declined_confirmation_deletes_nothing() -> None:
    client = workspace()
    assert run(client, do_delete=True) == orphans.EXIT_REFUSED
    assert client.deleted == []


def test_an_empty_prefix_is_refused_before_any_credential_is_read() -> None:
    # An empty prefix matches every folder under /Users, the instructor's
    # included.
    assert orphans.main(["--prefix", "  "]) == orphans.EXIT_CONFIG


# --- the CLI surface ----------------------------------------------------------


def test_reading_is_the_default_action() -> None:
    args = orphans.parse_args([])
    assert args.delete is False
    assert args.yes is False
    assert args.prefix == orphans.DEFAULT_PREFIX


def test_the_prefix_is_the_one_vocareum_names_its_principals_with() -> None:
    # Duplicated from diagnose rather than imported; the test is what keeps the
    # two copies honest.
    from dbx_vocareum_tools.diagnose import LAB_USER_PREFIX

    assert orphans.DEFAULT_PREFIX == LAB_USER_PREFIX


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        (f"/Users/{GONE_STUDENT}", GONE_STUDENT),
        ("/Users/", ""),
        ("labuser@example.com", "labuser@example.com"),
    ],
)
def test_the_owner_is_the_last_path_segment(path: str, expected: str) -> None:
    assert orphans.owner_of(path) == expected
