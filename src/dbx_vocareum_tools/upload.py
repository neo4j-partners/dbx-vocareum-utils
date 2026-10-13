"""Upload the lifecycle scripts into a Vocareum part workspace and verify them.

Vocareum reads a Databricks lab's provisioning logic from ``/voc/scripts`` in the
instructor workspace: ``workspace_init.sh``, ``lab_setup.sh``, ``user_setup.sh``,
and ``lab_end.sh``, run in that order as a student's session opens and closes.
The usual way to put them there is Configure Workspace in the browser, which is
manual, silent about what it actually wrote, and impossible to run from a
pipeline.

The API can do it, though the v2 reference does not say how. All three documented
``files`` operations are ``GET``. The only write path for workspace content is
the part update, ``PUT .../parts/{partId}``, whose ``content`` block is
documented in a single line naming ``target``, ``url``/``zipcontent``, and
``reset`` with no example. The undocumented part that costs an afternoon to find
is that **``content`` must be a JSON array**. Passing the object it looks like it
should be produces ``HTTP 500`` with a zero-length body and no error text, which
reads as a server fault rather than as the request-shape problem it is.

This tool differs from its AWS counterpart in one way that matters. An AWS lab
deploys a single CloudFormation template, so one file is the whole deployment. A
Databricks lab deploys a set of scripts that call each other through
``VOC_CUSTOM_DATA``, so a partial upload leaves a lab that half works: the
student's environment is built by the version of ``user_setup.sh`` that landed
and torn down by a ``lab_end.sh`` that expects a different one. The default
source is therefore a directory, every file in it goes up in one archive, and
each is verified by hash before the run reports success.

What goes up is a course's ``lab/`` plus the lab runtime, and the runtime does
not come from the course. ``voclib.sh`` and ``voclab.py`` ship inside this
package and are injected into the archive here, so a course upgrades them by
bumping a dependency rather than by remembering to copy two files. A course that
keeps its own copy of either is refused rather than preferred, because a stale
``voclab.py`` uploads and verifies exactly as cleanly as a current one.

Four things make this worth a script rather than a curl invocation:

1. The write is asynchronous. It answers ``202`` with a ``transactionid``, and
   the files are not in place when the response arrives.
2. Neither the ``202`` nor the directory listing can tell you what content
   landed. The listing returns names only, with no size, hash, or modified time.
   Only downloading each file back and comparing hashes proves anything.
3. Getting ``target`` or the archive layout wrong fails the same silent way as
   getting the JSON shape wrong.
4. A shell script that arrives without its executable bit is a lab that fails at
   the first lifecycle hook, so the mode is set on the archive member rather than
   left to whatever the extractor defaults to.

This command is part of the repository's uv project::

    uv run dbx-vocareum-upload lab/
    uv run dbx-vocareum-upload lab/ --target scripts
    uv run dbx-vocareum-upload lab/user_setup.sh
    uv run dbx-vocareum-upload --dry-run

Configuration comes from a ``.env`` file in the repository root, or from real
environment variables, which always win. See ``.env.example``. Only the API key,
course, assignment, and part are needed. ``VOCAREUM_USER_ID`` is not, since a
workspace belongs to the part rather than to a student. ``--dry-run`` needs none
of them: which files go up, under which names, with which hashes, is answerable
without addressing a part at all.

Run ``shellcheck`` and ``bash -n`` on a script before uploading it. A script
that cannot parse uploads just as successfully as one that can, and the failure
then surfaces minutes later inside a student session, as a lab that will not
start.

Exit codes:
    0  Every file was uploaded and verified byte for byte.
    1  Configuration error.
    2  The Vocareum API calls could not complete.
    3  The upload was accepted but a file that came back does not match.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .client import (
    TERMINAL_SUCCESS_STATES,
    ApiError,
    download_bytes,
    find_transaction_id,
    request_json,
)
from .client import (
    poll_transaction as wait_for_transaction,
)
from .config import (
    VOCAREUM_PART_VARS,
    ConfigError,
    Environment,
    repository_root,
)
from .databricks import GRADE_SCRIPT, LIFECYCLE_SCRIPT_NAMES, SCRIPTS_TARGET
from .endpoints import DEFAULT_API_BASE, PartEndpoints
from .labruntime import COURSE_ENV_TEMPLATE, COURSE_OWNED_ELSEWHERE, runtime_files
from .output import section
from .security import redact_payload, redact_text

EXIT_OK = 0
EXIT_CONFIG = 1
EXIT_API = 2
EXIT_MISMATCH = 3

DEFAULT_SOURCE = "lab"
DEFAULT_TARGET = SCRIPTS_TARGET

# Deliberately faster than client.TRANSACTION_POLL_SECONDS. This poll waits on
# a workspace write rather than on a lab launch, and the run is interactive.
TRANSACTION_POLL_SECONDS = 3
TRANSACTION_TIMEOUT_SECONDS = 120
HTTP_TIMEOUT_SECONDS = 60

# Vocareum's transaction reports success before its files endpoint can address
# every path the write just created. Measured: on a deploy of a few dozen
# members, over half answered "does not exist" on the very next read, and all
# were addressable and byte-correct minutes later. A small flat deploy rarely
# shows the window.
#
# So verify waits rather than reporting a deployment it just wrote as missing.
# Same shape as the retry policy in voclab.py, for the same reason: a fixed
# sleep before verifying is either too short on a larger archive or wasted on
# every small one, and it leaves the transcript nothing to say when it was too
# short. The cost is that a genuinely absent file takes 14 seconds longer to be
# reported as absent, which is a good trade against exit 3 on a correct deploy.
VERIFY_SETTLE_BACKOFF = (2.0, 4.0, 8.0)

# Zip entries carry a modification time, so the same file zipped twice produces
# two different archives unless the timestamp is pinned. Pinning it makes an
# upload reproducible and lets two runs be compared. 1980-01-01 is the zero of
# the DOS timestamp the format uses, and is the only value guaranteed to encode.
ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)

# The whole valid set, and for once that is measured rather than collected.
# Passing a nested target is refused with the only error on this API that names
# its own field and enumerates its own domain:
#
#   Invalid target (private/probe_target) for content update.
#   Valid targets: [course, data, docs, scripts, startercode, private]
#
# It is still a warning rather than a refusal, because the list was measured on one
# course and a second course adding a folder should cost a warning rather than
# a blocked upload. Only "scripts" is written to by this tool.
KNOWN_TARGETS = frozenset(
    {"course", "data", "docs", "scripts", "startercode", "private"}
)

# The names Vocareum will actually execute. Anything else uploaded into
# /voc/scripts is inert: it is not an error, because a lifecycle script is
# allowed to source a shared helper, but a typo in one of these five names
# produces a lab that silently skips a hook, which is worth a warning.
EXECUTED_SCRIPT_NAMES = frozenset((*LIFECYCLE_SCRIPT_NAMES, GRADE_SCRIPT.name))

# Files and directories that are part of the checkout rather than part of the
# lab. ``course.env.example`` is the scaffold a deployer copies, not a file a
# hook reads: uploading it puts a second, all-commented course config beside the
# real one, which is the sort of thing somebody eventually edits instead.
#
# ``__pycache__`` is here because anything that imports the course's program
# between the dry run and the real one writes bytecode into ``lab/``. The
# bytecode is inert, it is stale the moment the source changes, and this API has
# no delete, so the only place to stop it is here.
SKIPPED_NAMES = frozenset(
    {".DS_Store", "README.md", "__pycache__", COURSE_ENV_TEMPLATE}
)

# The runtime this package injects goes only into the folder Vocareum executes
# from. A courseware upload into /voc/private has no use for voclib.sh, and
# putting it there would be two copies of a file whose whole point is that there
# is one.
RUNTIME_TARGET = SCRIPTS_TARGET


@dataclass(frozen=True)
class Config(PartEndpoints):
    """Resolved configuration for one upload."""

    api_key: str


@dataclass(frozen=True)
class Member:
    """One file on its way into the workspace."""

    source: Path
    arcname: str
    payload: bytes

    @property
    def executable(self) -> bool:
        """Report whether this member should arrive with its executable bit set.

        Decided from the content rather than from the local file mode, because
        the mode does not survive a checkout on every platform and a shebang
        does. A file Vocareum runs and cannot execute fails at the hook.
        """
        return self.payload.startswith(b"#!")

    @property
    def digest(self) -> str:
        """Return the SHA-256 hex digest of what will be sent."""
        return digest(self.payload)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


def load_config(env_path: Path) -> Config:
    """Build a :class:`Config` from real environment variables and ``.env``.

    Real environment variables take precedence over ``.env`` values so the
    script works unchanged in CI. Raises :class:`ConfigError` naming every
    missing required variable at once.
    """
    environment = Environment.from_file(env_path)
    try:
        environment.require(VOCAREUM_PART_VARS)
    except ConfigError as error:
        raise ConfigError(
            f"{error}\nCopy .env.example to .env and fill in the values, "
            "or export them in your shell."
        ) from error

    return Config(
        api_key=environment.get("VOCAREUM_API_KEY"),
        course_id=environment.get("VOCAREUM_COURSE_ID"),
        assignment_id=environment.get("VOCAREUM_ASSIGNMENT_ID"),
        part_id=environment.get("VOCAREUM_PART_ID"),
        api_base=(environment.get("VOCAREUM_API_BASE") or DEFAULT_API_BASE).rstrip("/"),
    )


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def api_json(method: str, url: str, api_key: str, body: Any = None) -> Any:
    """Call the shared API client with the upload timeout."""
    return request_json(method, url, api_key, body, timeout=HTTP_TIMEOUT_SECONDS)


def download(url: str) -> bytes:
    """Download with the upload timeout and safe URL diagnostics."""
    return download_bytes(url, timeout=HTTP_TIMEOUT_SECONDS)


# ---------------------------------------------------------------------------
# Payload
# ---------------------------------------------------------------------------


def digest(payload: bytes) -> str:
    """Return the SHA-256 hex digest of a payload."""
    return hashlib.sha256(payload).hexdigest()


def validate_path_segment(value: str, label: str) -> None:
    """Reject names that could escape the selected workspace directory.

    A slash is refused here, and for ``--target`` that is measured behaviour.
    A ``target`` of ``private/probe_target`` is answered with ``400`` and an
    error that enumerates the whole valid set, ``[course, data, docs, scripts,
    startercode, private]``. See ``docs/vocareum-api.md``. An archive *member*
    may carry a slash, which the same measurement established, so members go
    through
    :func:`validate_arcname` and each of their segments arrives here separately.

    Whitespace is refused rather than trimmed. A target of ``"   "`` escapes
    nothing, so the escape checks alone let it through, and it then addresses
    ``/voc/   `` which is not a directory Vocareum reads. Padding is refused for
    the reason it is dangerous: ``" scripts"`` and ``"scripts"`` look identical
    in a terminal and only one of them is a folder.
    """
    if (
        not value.strip()
        or value != value.strip()
        or value in {".", ".."}
        or "/" in value
        or "\\" in value
        or any(ord(character) < 32 for character in value)
    ):
        raise ConfigError(f"{label} must be one non-empty path segment, not {value!r}.")


def collect_directory(source: Path) -> list[Member]:
    """Read every file under ``source``, arcnamed relative to ``source`` itself.

    Subdirectories are allowed. Measured: under the array form of ``content``,
    a member named ``probe_dir/probe_nested_member.txt`` uploads, reaches
    transaction state ``success``, lists at exactly that path under the target,
    and downloads with the bytes that went up. The *object* form of ``content``
    returns a bodyless ``500`` on its own, which once made nesting look refused.
    ``docs/vocareum-api.md`` has the details.

    Allowing nesting matters because the obvious alternative was worse. The
    only other way to get a course's courseware up is to flatten it into one
    folder, or to leave it as a manual upload in the Vocareum UI. Flattening
    loses the directory the course's own program reads; a manual step in the
    middle of a deployment means no deployment is hash verified, which is the
    guarantee this whole tool exists to provide.

    Symlinked directories are followed, because a course whose courseware is
    generated elsewhere in its repository should be able to point at it once
    rather than keep a second copy that drifts. ``seen`` holds resolved paths so
    a link back to an ancestor stops rather than recurses forever.
    """
    members: list[Member] = []
    seen: set[Path] = set()

    def walk(directory: Path) -> None:
        resolved = directory.resolve()
        if resolved in seen:
            return
        seen.add(resolved)
        for item in sorted(directory.iterdir()):
            if item.name.startswith(".") or item.name in SKIPPED_NAMES:
                continue
            if item.is_dir():
                walk(item)
            elif item.is_file():
                members.append(read_member(item, item.relative_to(source).as_posix()))

    walk(source)
    return members


def collect_members(paths: list[str], name_override: str | None) -> list[Member]:
    """Expand the command line into the files to upload, sorted by name.

    A directory contributes every file under it, keeping its layout as the
    archive member names. Sorting makes the archive, and therefore the
    transcript, reproducible.
    """
    found: dict[str, Member] = {}

    for raw in paths:
        source = Path(raw).expanduser()
        if source.is_dir():
            collected = collect_directory(source)
            if not collected:
                raise ConfigError(f"{source} holds no files to upload.")
            for member in collected:
                found[member.arcname] = member
            continue

        if not source.is_file():
            raise ConfigError(f"No such file or directory: {source}")
        arcname = name_override or source.name
        found[arcname] = read_member(source, arcname)

    if name_override is not None and len(found) > 1:
        raise ConfigError(
            "--name renames a single file, but "
            f"{len(found)} were selected: {', '.join(sorted(found))}."
        )

    return [found[name] for name in sorted(found)]


def validate_arcname(value: str, label: str) -> None:
    """Reject an archive member name that could escape the target folder.

    Every segment goes through :func:`validate_path_segment`, so ``..``, an
    absolute name, a backslash, and padded whitespace are refused at any depth
    rather than only at the top. An empty segment is what a trailing or doubled
    slash produces, and it is refused for the same reason: it names a directory
    entry nobody can address.
    """
    for segment in value.split("/"):
        validate_path_segment(segment, label)


def read_member(source: Path, arcname: str) -> Member:
    """Read one file and reject the states that cannot be uploaded usefully."""
    validate_arcname(arcname, f"The archive name for {source}")
    try:
        payload = source.read_bytes()
    except OSError as error:
        raise ConfigError(f"Could not read {source}: {error}") from error
    if not payload:
        raise ConfigError(f"{source} is empty. Refusing to upload it.")
    return Member(source=source, arcname=arcname, payload=payload)


def build_zip(members: list[Member]) -> bytes:
    """Return a deterministic archive holding every member.

    Archive members are written into the folder named by ``target``, so a member
    named ``user_setup.sh`` uploaded with ``target: "scripts"`` lands at
    ``/voc/scripts/user_setup.sh``.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9
    ) as archive:
        for member in members:
            info = zipfile.ZipInfo(filename=member.arcname, date_time=ZIP_EPOCH)
            info.compress_type = zipfile.ZIP_DEFLATED
            mode = 0o755 if member.executable else 0o644
            info.external_attr = mode << 16
            archive.writestr(info, member.payload)
    return buffer.getvalue()


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


def find_download_url(payload: Any) -> str | None:
    """Pull the first ``download_url`` out of a files response, at any nesting."""
    if isinstance(payload, dict):
        url = payload.get("download_url")
        if isinstance(url, str) and url.strip():
            return url
        for value in payload.values():
            found = find_download_url(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_download_url(item)
            if found is not None:
                return found
    return None


def remote_digest(config: Config, remote_path: str) -> str | None:
    """Return the digest of the file at a workspace path, or ``None`` if absent.

    Two response shapes mean "not there yet". A path that has never held a file
    answers 404; a path whose directory exists but whose file does not answers
    400 with prose saying so. Both are an ordinary first upload. Any other 400
    still raises, because a malformed request looks the same from here and must
    not be read as an empty workspace.
    """
    try:
        payload = api_json(
            "GET", config.files_url({"filename": remote_path}), config.api_key
        )
    except ApiError as error:
        not_found = error.status_code == 404 or (
            error.status_code == 400 and "does not exist" in str(error).lower()
        )
        if not not_found:
            raise
        return None

    url = find_download_url(payload)
    if url is None:
        raise ApiError(
            f"The files response for {remote_path} succeeded but carried no "
            "download_url. Refusing to treat an unknown response shape as a "
            "missing file."
        )
    return digest(download(url))


def show_current_files(
    config: Config, target: str, members: list[Member]
) -> dict[str, str | None]:
    """Print the digest of each path as it stands now, and return them.

    Runs before the upload so the transcript records what was replaced, and so
    a run that changes nothing can say so rather than writing anyway.
    """
    section("STEP 1: Read what is there now")
    current: dict[str, str | None] = {}
    for member in members:
        remote_path = f"{target}/{member.arcname}"
        found = remote_digest(config, remote_path)
        current[member.arcname] = found
        print(f"  {remote_path}: {found or '(not present)'}")
    return current


def upload(config: Config, target: str, members: list[Member]) -> str | None:
    """Send the part update carrying the archive. Returns a transaction id.

    ``content`` is a list. That is the whole trick, and getting it wrong is
    answered with a bodyless 500 that names nothing.
    """
    section("STEP 2: Upload")
    archive = build_zip(members)
    encoded = base64.b64encode(archive).decode("ascii")
    print(f"PUT {config.part_url()}")
    print(f"target={target} members={len(members)}")
    for member in members:
        mode = "0755" if member.executable else "0644"
        print(f"  {member.arcname} ({len(member.payload)} bytes, mode {mode})")
    print(f"zip bytes={len(archive)} base64 chars={len(encoded)}")

    body = {"content": [{"target": target, "zipcontent": encoded}]}
    response = api_json("PUT", config.part_url(), config.api_key, body)
    print(f"Response: {json.dumps(redact_payload(response), sort_keys=True)}")

    return find_transaction_id(response)


def poll_transaction(config: Config, transaction_id: str) -> str:
    """Poll a transaction to a terminal state and return that state."""
    section("STEP 3: Wait for the write")
    url = config.transaction_url(transaction_id)
    print(f"GET {url}")

    result = wait_for_transaction(
        url,
        config.api_key,
        poll_seconds=TRANSACTION_POLL_SECONDS,
        timeout_seconds=TRANSACTION_TIMEOUT_SECONDS,
        on_state=lambda state: print(f"  state={state}"),
    )
    if result.timed_out:
        print(
            f"  still pending after {TRANSACTION_TIMEOUT_SECONDS}s, "
            "giving up on the poll"
        )
    if isinstance(result.payload, dict):
        message = str(result.payload.get("message", "(none)"))
        print(f"  message={redact_text(message)}")
    return result.state


def verify(
    config: Config,
    target: str,
    members: list[Member],
    backoff: tuple[float, ...] = VERIFY_SETTLE_BACKOFF,
    sleep: Callable[[float], None] = time.sleep,
) -> list[str]:
    """Download each file back and return the names whose digests disagree.

    A path that is not addressable yet is retried; a path that answers with the
    wrong bytes is not. The two look the same to :func:`remote_digest`, which
    returns ``None`` for both "never written" and "written but not yet
    addressable", and only a caller knows which it is looking at.
    :func:`show_current_files` reads the same paths before the upload, where
    ``None`` genuinely means absent and a retry would be 14 seconds of waiting
    for a file nobody has sent yet.

    Retrying a digest that came back and disagreed would be the dangerous half
    of this. That is a real mismatch, it is the reason exit ``3`` exists, and
    reading it again until it passes is how a bad deploy talks its way through
    the gate. Only ``None`` is ambiguous, so only ``None`` is retried.
    """
    section("STEP 4: Verify")
    mismatched: list[str] = []
    pending = list(members)

    for attempt, delay in enumerate((*backoff, None)):
        unsettled: list[Member] = []
        for member in pending:
            remote_path = f"{target}/{member.arcname}"
            actual = remote_digest(config, remote_path)
            if actual == member.digest:
                print(f"  {remote_path}: ok")
            elif actual is None:
                unsettled.append(member)
            else:
                print(f"  {remote_path}: MISMATCH")
                print(f"    expected {member.digest}")
                print(f"    actual   {actual}")
                mismatched.append(member.arcname)

        pending = unsettled
        if not pending or delay is None:
            break
        print(
            f"  {len(pending)} path(s) not addressable yet, waiting {delay:g}s "
            f"(attempt {attempt + 1} of {len(backoff)})"
        )
        sleep(delay)

    for member in pending:
        print(f"  {target}/{member.arcname}: MISSING")
        print(f"    expected {member.digest}")
        print("    actual   (nothing came back)")
        mismatched.append(member.arcname)

    return mismatched


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def warn_unknown_target(target: str) -> None:
    """Warn when ``--target`` names a folder nobody has seen on this course."""
    if target in KNOWN_TARGETS:
        return
    print(
        f"WARNING: --target {target!r} is not one of the folders seen on this "
        f"course ({', '.join(sorted(KNOWN_TARGETS))}). If it is wrong, the API "
        "answers with a bodyless HTTP 500 that names nothing."
    )


def warn_inert_members(target: str, members: list[Member]) -> None:
    """Warn about uploaded files Vocareum will never execute.

    A helper the lifecycle scripts source is a legitimate reason to be here, so
    this reports rather than refuses. It exists to catch the case it cannot tell
    apart from that one: a misspelled hook name, which produces a lab that skips
    a stage and says nothing about why.
    """
    if target != SCRIPTS_TARGET:
        return
    inert = [
        member.arcname
        for member in members
        if member.arcname not in EXECUTED_SCRIPT_NAMES
    ]
    if not inert:
        return
    print(
        f"NOTE: Vocareum does not execute {', '.join(inert)}. It runs only "
        f"{', '.join(sorted(EXECUTED_SCRIPT_NAMES))}. That is expected for a "
        "sourced helper, and a misspelled hook name looks exactly the same."
    )


def inject_runtime(members: list[Member]) -> list[Member]:
    """Add ``voclib.sh`` and ``voclab.py`` from the package to the archive.

    A course does not own these files. They are the same for every course, they
    are upgraded by bumping a dependency, and a course that kept its own copy
    would be running whatever version it was scaffolded with for as long as
    nobody noticed. Injecting them here is what makes "one owner per file" true
    at deployment rather than only in a design document.

    A local file of either name is refused rather than preferred or merged. The
    failure it prevents is the silent one: a stale ``voclab.py`` in a course's
    ``lab/`` uploads perfectly, verifies by hash, and provisions with last
    year's runtime, and nothing in the transcript says which copy went up.
    """
    clashing = sorted(
        member.arcname for member in members if member.arcname in COURSE_OWNED_ELSEWHERE
    )
    if clashing:
        raise ConfigError(
            f"{', '.join(clashing)} ships inside dbx-vocareum-tools and is added "
            "to every upload from there. Delete the local copy: keeping one is "
            "how a course ends up deploying a runtime nobody has looked at since "
            "it was scaffolded."
        )

    injected = [
        read_member(path, path.name)
        for path in runtime_files()
        # A course may ship a file the runtime also names only by being wrong
        # about it, and that is caught above. This filter is for the reverse: a
        # runtime file the package holds but this target does not want.
        if path.name not in {member.arcname for member in members}
    ]
    return sorted(members + injected, key=lambda member: member.arcname)


def prepare(args: argparse.Namespace) -> list[Member]:
    """Check the target, collect the files, and warn about what is going up.

    Every step here is local. Nothing in it addresses a course, an assignment, or
    a part, which is what lets the dry run happen before those are known.
    """
    validate_path_segment(args.target, "--target")
    members = collect_members(args.paths, args.name)
    if args.target == RUNTIME_TARGET:
        members = inject_runtime(members)
    warn_unknown_target(args.target)
    warn_inert_members(args.target, members)
    return members


def describe_members(target: str, members: list[Member]) -> None:
    """Print the destination and every file that would be written to it."""
    print(f"Destination       : /voc/{target}/")
    print(f"Files             : {len(members)}")
    for member in members:
        print(f"  {member.source} -> {member.arcname}  {member.digest}")


def run_dry_run(args: argparse.Namespace) -> int:
    """Print what would be uploaded, reading no configuration at all.

    Separated from the real run because the four Vocareum variables are not
    needed to answer what a dry run answers: which files, under which names, with
    which hashes. Requiring them would fail the one command that is useful before
    a part exists to upload to.
    """
    members = prepare(args)
    describe_members(args.target, members)
    section("RESULT")
    print("--dry-run was passed. Nothing was uploaded, and no configuration")
    print("was read. Drop --dry-run to write these files.")
    return EXIT_OK


def run_upload(config: Config, args: argparse.Namespace) -> int:
    """Run every step in order and return the process exit code."""
    members = prepare(args)

    print(f"Vocareum API base : {config.api_base}")
    print(f"Course/Assignment : {config.course_id}/{config.assignment_id}")
    print(f"Part              : {config.part_id}")
    describe_members(args.target, members)

    current = show_current_files(config, args.target, members)
    unchanged = all(current[member.arcname] == member.digest for member in members)
    if unchanged and not args.force:
        section("RESULT")
        print("Every file up there is already byte-identical to the local one.")
        print("Nothing to do. Pass --force to write them anyway.")
        return EXIT_OK

    transaction_id = upload(config, args.target, members)
    if transaction_id is None:
        section("RESULT")
        print("No transaction ID came back, so the asynchronous write is unproven.")
        return EXIT_API

    state = poll_transaction(config, transaction_id)
    if state.lower() not in TERMINAL_SUCCESS_STATES:
        section("RESULT")
        print(f"The write ended in state {state!r} rather than success.")
        return EXIT_API

    if args.no_verify:
        section("RESULT")
        print("--no-verify was passed. The write was accepted and not checked.")
        return EXIT_OK

    mismatched = verify(config, args.target, members)
    if mismatched:
        section("RESULT")
        print(
            f"The upload was accepted but {', '.join(mismatched)} came back "
            "different from what was sent.\n"
            "Do not start a lab against this. The scripts call each other through "
            "VOC_CUSTOM_DATA, so a partial write builds a student environment with "
            "one version and tears it down with another.\n"
            "Check that --target names the right folder and that nothing else "
            "wrote to the workspace at the same time."
        )
        return EXIT_MISMATCH

    section("RESULT")
    replaced = {name: value for name, value in current.items() if value is not None}
    print(f"/voc/{args.target}/ now matches the local files byte for byte.")
    if replaced:
        print(f"It replaced {len(replaced)} existing file(s):")
        for name, value in sorted(replaced.items()):
            print(f"  {name} was {value}")
    print("The next Start Lab will use them. Vocareum caches nothing.")
    return EXIT_OK


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Upload the Databricks lab lifecycle scripts into a Vocareum part "
            "workspace and verify each one by hash."
        ),
        epilog=(
            "Configuration is read from .env in the repository root. Real "
            "environment variables take precedence. See .env.example."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=[DEFAULT_SOURCE],
        metavar="PATH",
        help=(
            "Files, or one directory of files, to upload. A directory "
            "contributes every file under it, keeping its layout as the "
            f"archive member names. Default: {DEFAULT_SOURCE}"
        ),
    )
    parser.add_argument(
        "--target",
        default=DEFAULT_TARGET,
        help=(
            "Workspace folder under /voc to write into. Default: "
            f"{DEFAULT_TARGET}, which is where Vocareum reads the lifecycle "
            "scripts from."
        ),
    )
    parser.add_argument(
        "--name",
        default=None,
        help=(
            "Filename to write on the Vocareum side. Renames a single file, so "
            "it cannot be combined with a directory or several paths."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print what would be uploaded and stop before writing anything. "
            "Reads no configuration, so it works before a part is known."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Upload even when every remote file already matches. Useful to prove "
            "the write path still works without changing what is deployed."
        ),
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help=(
            "Skip the download-and-compare step. Only useful when a file is too "
            "large to fetch back, since the API cannot otherwise confirm what "
            "it stored."
        ),
    )
    parser.add_argument(
        "--env-file",
        default=None,
        metavar="PATH",
        help="Override the default .env location (repository root).",
    )
    args = parser.parse_args(argv)
    if not args.paths:
        args.paths = [DEFAULT_SOURCE]
    return args


def main(argv: list[str] | None = None) -> int:
    """Entry point. See the module docstring for exit code meanings."""
    args = parse_args(argv)

    default_env = repository_root() / ".env"
    env_path = Path(args.env_file).expanduser() if args.env_file else default_env

    try:
        if args.dry_run:
            return run_dry_run(args)
        config = load_config(env_path)
        return run_upload(config, args)
    except ConfigError as error:
        print("Configuration error.")
        print(error)
        return EXIT_CONFIG
    except ApiError as error:
        section("RESULT")
        print(f"The upload could not complete: {error}")
        return EXIT_API


if __name__ == "__main__":
    raise SystemExit(main())
