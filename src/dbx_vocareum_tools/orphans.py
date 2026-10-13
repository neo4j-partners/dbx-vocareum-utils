"""Report and reclaim the home folders Vocareum's deleted lab identities left.

Vocareum mints a Databricks principal per session and removes it when the
session ends. The ``/Users/<identity>`` folder Databricks created for that
principal is not removed with it: the folder is a workspace object, the identity
is an account object, and nothing in Vocareum's teardown joins the two. So the
folders accumulate, one per session, forever.

Measured: one workspace held nine ``/Users/labuser*`` folders against exactly
one live SCIM identity, so eight were orphans. Each held a copy of the course's
imported notebooks.

They cost nothing. That is worth saying plainly, because it is the reason this
is a seventh tool rather than a step inside ``lab_end.sh``: there is no bill
running and no student waiting. What they cost is legibility. An instructor
opening the workspace tree mid-class to find the one student who is stuck reads
a list of nine near-identical names, eight of which belong to nobody, and has to
guess. This tool is the answer to that, and reporting is its default because on
most days the report is the whole job.

Why the identity listing is read first, before anything is listed for deletion.
A folder is evidence that an identity once existed; it is not evidence about
whether it exists now. The only thing that answers that is
``GET /api/2.0/preview/scim/v2/Users``, and the answer has to be in hand before
a single delete is issued, because a live identity is a student who may be
sitting in a running session with unsaved work in exactly that folder. The
obvious alternative, deleting anything older than some age, was rejected on that
ground alone: a long lab is indistinguishable from an abandoned one by
timestamp, and it is only distinguishable by whether the principal still exists.

Why this passes ``probe.py``'s three guards, the same way ``pool.py`` does. A
tool that destroys objects has at least as much to lose from an unverified
connection as one that creates them. The SDK's provider chain will fall through
to ``DATABRICKS_TOKEN``, ``~/.databrickscfg``, or a cached CLI login without
saying so, and Vocareum creates a workspace per part, so an unguarded run can
delete real folders in a workspace no student of this course ever entered and
report success doing it.

Usage::

    uv run dbx-vocareum-orphans                 # read only, the default
    uv run dbx-vocareum-orphans --delete        # delete orphans, after a y/n
    uv run dbx-vocareum-orphans --delete --yes  # skip the y/n, for scripted runs

Exit codes:
    0  The operation ran.
    1  Configuration error.
    2  A guard refused, the API did, or a folder failed to delete.
    3  A confirmation was declined.
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from requests.exceptions import RequestException

from .config import ConfigError, repository_root
from .databricks import DatabricksHostError
from .output import rule, section
from .probe import (
    Config,
    GuardError,
    build_workspace_client,
    guard_credentials,
    guard_host,
    guard_identity,
    load_config,
)

EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_API = 2
EXIT_REFUSED = 3

USERS_ROOT = "/Users"
LIST_PATH = "/api/2.0/workspace/list"
DELETE_PATH = "/api/2.0/workspace/delete"

# The workspace-plane SCIM listing rather than the account-plane one. The
# question this tool asks is "may this folder go", the folder lives in the
# workspace, and an identity removed from the workspace is exactly the identity
# whose folder is now unreachable, whatever the account still holds.
USERS_SCIM_PATH = "/api/2.0/preview/scim/v2/Users"

# The prefix Vocareum gives the principals it generates per session. Duplicated
# from ``diagnose.LAB_USER_PREFIX`` rather than imported: importing it would tie
# a destructive tool's blast radius to a constant owned by a diagnostic, and the
# flag exists so a course that sees another prefix can say so without either
# file changing.
DEFAULT_PREFIX = "labuser"

LIVE = "LIVE"
ORPHAN = "ORPHAN"


@dataclass(frozen=True)
class HomeFolder:
    """One ``/Users`` folder and whether its owner is still an identity.

    Held as a record rather than resolved inline so the classification can be
    tested against a listing and an identity set with no client in the picture,
    and so the report and the delete pass are reading the same verdict rather
    than each deciding for itself what counts as an orphan.
    """

    path: str
    owner: str
    live: bool

    @property
    def label(self) -> str:
        return LIVE if self.live else ORPHAN


def owner_of(path: str) -> str:
    """Return the identity a ``/Users`` folder belongs to.

    Databricks names the home folder with the principal's ``userName``
    verbatim, so ``/Users/labuser10000001_1785962713@vocareum.com`` and the SCIM
    ``userName`` are the same string and comparing them needs no normalization.
    The split is on the last separator rather than on ``/Users/`` so that a path
    which is not under the root at all yields its own last segment and fails to
    match any live identity, which is the safe direction: an unmatched folder is
    reported, and reporting is not destructive.
    """
    return path.rsplit("/", 1)[-1]


def list_users_root(client: WorkspaceClient) -> list[str]:
    """Return every path directly under ``/Users``.

    A workspace whose ``/Users`` holds nothing answers with an empty body rather
    than with an empty ``objects`` array, which is why the key is read
    defensively instead of indexed.
    """
    payload = client.api_client.do("GET", LIST_PATH, query={"path": USERS_ROOT})
    if not isinstance(payload, dict):
        return []
    paths = []
    for entry in payload.get("objects") or []:
        if isinstance(entry, dict) and entry.get("path"):
            paths.append(str(entry["path"]))
    return sorted(paths)


def live_identities(client: WorkspaceClient) -> set[str]:
    """Return the ``userName`` of every identity the workspace still has.

    This is the only call whose failure must stop the run, so it is deliberately
    not wrapped: an empty set from a call that did not complete is
    indistinguishable from an empty set from a workspace with no users, and the
    second is a licence to delete everything.
    """
    payload = client.api_client.do("GET", USERS_SCIM_PATH)
    if not isinstance(payload, dict):
        return set()
    names = set()
    for resource in payload.get("Resources") or []:
        if isinstance(resource, dict) and resource.get("userName"):
            names.add(str(resource["userName"]))
    return names


def classify_folders(paths: list[str], live: set[str], prefix: str) -> list[HomeFolder]:
    """Pick the lab folders out of a ``/Users`` listing and mark each one.

    Matching is on the folder name starting with the prefix, not on the prefix
    appearing anywhere in the path. The two agree on every folder measured so
    far and disagree on the one that matters: a real person whose address
    happens to contain the word would be matched by a substring test, and this
    tool's job is to never be the reason a folder that belongs to somebody goes
    away.
    """
    folders = []
    for path in paths:
        owner = owner_of(path)
        if not owner.startswith(prefix):
            continue
        folders.append(HomeFolder(path=path, owner=owner, live=owner in live))
    return folders


def report_identities(live: set[str]) -> None:
    """Print the identity listing the whole run is judged against."""
    section("LIVE IDENTITIES")
    print(f"  {len(live)} identities exist in this workspace.")
    for name in sorted(live):
        print(f"    {name}")


def report_folders(folders: list[HomeFolder], prefix: str) -> None:
    """Print every matched folder with its verdict, and the counts."""
    section(f"HOME FOLDERS UNDER {USERS_ROOT} NAMED {prefix}*")
    if not folders:
        print(f"  No folder under {USERS_ROOT} is named {prefix}*. Nothing to do.")
        return
    for folder in folders:
        print(f"  {folder.label:7s} {folder.path}")
    rule()
    orphans = [folder for folder in folders if not folder.live]
    print(
        f"  {len(folders)} folders: {len(folders) - len(orphans)} live, "
        f"{len(orphans)} orphaned."
    )


def confirm_delete(orphans: list[HomeFolder], host: str) -> bool:
    """Ask for a y/n before deleting, naming the count and the workspace.

    The prompt names the host because the mistake this is guarding against is
    not "delete the wrong number of folders", which the report above has already
    made visible, but "delete the right folders in the wrong workspace".
    """
    print(f"About to delete {len(orphans)} orphaned home folders in {host}.")
    print(
        "Each delete is recursive and takes the notebooks inside the folder with\n"
        "it. Every one of these belongs to an identity that no longer exists, so\n"
        "nobody can open them, and nothing else in the workspace refers to them."
    )
    if not sys.stdin.isatty():
        print("Refusing: nothing is attached to answer, and --yes was not passed.")
        return False
    answer = input(f"Delete {len(orphans)} folders? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Not confirmed. Nothing was deleted.")
        return False
    return True


def delete_folder(client: WorkspaceClient, folder: HomeFolder) -> None:
    """Delete one home folder and everything in it."""
    client.api_client.do(
        "POST", DELETE_PATH, body={"path": folder.path, "recursive": True}
    )


def delete_orphans(
    client: WorkspaceClient, folders: list[HomeFolder]
) -> tuple[int, int]:
    """Delete every orphan, printing one line per folder either way.

    A failure prints and the loop continues rather than raising, because the
    alternative leaves a workspace half cleaned with no account of which half.
    A fully reported partial run can be re-run and read; an aborted one has to
    be reconstructed from the tree. The count of failures comes back so the exit
    code can be honest about it, since a scripted run that reports success after
    failing to delete is the same lie in a place nobody reads.
    """
    deleted = 0
    failed = 0
    for folder in folders:
        if folder.live:
            print(f"  SKIP   {folder.path} (identity is live)")
            continue
        try:
            delete_folder(client, folder)
        except (DatabricksError, RequestException) as error:
            failed += 1
            print(f"  FAIL   {folder.path}: {error}")
            continue
        deleted += 1
        print(f"  DEL    {folder.path}")
    return deleted, failed


def run(
    client: WorkspaceClient,
    prefix: str,
    host: str,
    do_delete: bool,
    assume_yes: bool,
) -> int:
    """Read, and delete only when asked to.

    The identity listing is read first in both modes, not only in the deleting
    one, so that a read-only run and the delete that follows it are answering
    the same question from the same evidence.
    """
    live = live_identities(client)
    report_identities(live)

    folders = classify_folders(list_users_root(client), live, prefix)
    report_folders(folders, prefix)

    orphans = [folder for folder in folders if not folder.live]
    if not do_delete:
        if orphans:
            print(
                "\n  Nothing was deleted. This tool only reads unless --delete is\n"
                "  passed."
            )
        return EXIT_OK

    if not orphans:
        print("\n  No orphans. Nothing to delete.")
        return EXIT_OK

    section("DELETE")
    if not assume_yes and not confirm_delete(orphans, host):
        return EXIT_REFUSED

    deleted, failed = delete_orphans(client, folders)
    rule()
    print(f"  Deleted {deleted}. Failed {failed}.")

    # Re-listed rather than subtracted from what was deleted, because the
    # deletes were issued against a listing that is now out of date and the
    # question worth answering is what the workspace holds, not what this run
    # believes it did.
    section("AFTER")
    remaining = classify_folders(list_users_root(client), live, prefix)
    if not remaining:
        print(f"  No folder under {USERS_ROOT} is named {prefix}* any more.")
    else:
        for folder in remaining:
            print(f"  {folder.label:7s} {folder.path}")
    return EXIT_API if failed else EXIT_OK


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report, and optionally delete, the Databricks home folders left "
            "behind by Vocareum lab identities that no longer exist."
        )
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="delete the orphans. Without this the tool only reads.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="skip the delete confirmation. For scripted runs.",
    )
    parser.add_argument(
        "--prefix",
        default=DEFAULT_PREFIX,
        help=(
            "which folder names count as lab identities. Default "
            f"{DEFAULT_PREFIX!r}, which is what Vocareum names its principals."
        ),
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=repository_root() / ".env",
        help="path to the .env file. Defaults to the repository root.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if not args.prefix.strip():
        print(
            "--prefix is empty, which would match every folder under /Users "
            "including the instructor's. Name a prefix.",
            file=sys.stderr,
        )
        return EXIT_CONFIG

    try:
        config: Config = load_config(
            args.env_file.expanduser(),
            "service-principal",
            None,
            None,
            need_account=False,
        )
    except ConfigError as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        return EXIT_CONFIG

    try:
        section("GUARD 1: confirm the credential")
        guard_credentials(config)

        section("GUARD 2: confirm the workspace")
        client = build_workspace_client(config)
        guard_host(client, config)

        section("GUARD 3: confirm the identity")
        guard_identity(client, config)
    except (GuardError, DatabricksHostError) as error:
        print(f"\nREFUSED\n{error}", file=sys.stderr)
        return EXIT_API

    try:
        return run(
            client,
            args.prefix.strip(),
            config.host,
            do_delete=args.delete,
            assume_yes=args.yes,
        )
    except (DatabricksError, RequestException) as error:
        print(f"\n{error}", file=sys.stderr)
        return EXIT_API


if __name__ == "__main__":
    raise SystemExit(main())
