#!/usr/bin/env python3
"""Cluster provisioning for the Vocareum Databricks lifecycle hooks.

This is the Python counterpart of ``voclib.sh``: it is uploaded to
``/voc/scripts`` beside the four hooks, Vocareum never executes it directly, and
the hooks invoke it through ``voc_voclab``. It exists because the Databricks
calls a hook has to make are not calls worth writing in shell, and because the
obvious alternative was worse.

Why the standard library and no SDK. The reference integration this replaces
begins every hook with ``pip3 install "databricks-sdk==0.38.0"``, which puts a
PyPI round trip on the path a student waits on, once per hook invocation, and
fails outright on any host with a PEP 668 managed Python. The handful of calls
below are bearer-token JSON requests against the workspace API, so
``urllib.request`` covers them. The SDK earns its place in ``pyproject.toml``
for the OAuth machine-to-machine exchange against the *account* API; there is no
exchange to avoid writing here, because ``VOC_DB_API_TOKEN`` is already a bearer
token. The one thing genuinely given up is the SDK's retry policy, which is why
``call`` below has its own and is careful about which failures it may retry.

Why the credential is relied on. ``VOC_DB_API_TOKEN`` was measured inside a
running Vocareum hook: it authenticates and carries the service principal, which
``docs/permissions.md`` records and nothing else may. It is the only Vocareum
variable needed to provision a cluster. It is still checked rather than assumed
on every run, because one observation on one part is not a guarantee about the
next, and when it is absent this program says so through ``MISSING_API_TOKEN``
rather than failing obscurely.

The output contract, which is the part that makes failures visible. Everything
this program says about what it is doing goes to **stderr**, so it interleaves
with ``voc_log`` in whatever console is watching. Its return value is
``key=value`` lines on **stdout**, one per line, keys drawn from ``[a-z0-9_]``.
On failure it emits ``error_code`` and ``message`` and exits non-zero, and the
shell routes both into ``voc_fail``. That is the whole point: the reference lets
a Python traceback exit non-zero with nothing written to ``voccustomdata.txt``,
which produces exactly the lab that failed for no stated reason that
``voc_fail`` exists to prevent. ``key=value`` rather than JSON so the shell side
needs no JSON parser.

Nothing here waits on a cluster. ``clusters/create`` and ``libraries/install``
both return as soon as the request is accepted and the cluster warms
server-side, so a hook makes a handful of short requests and exits. Pre-warming
from ``lab_setup.sh`` is worth doing anyway: resolving the Neo4j Spark connector
from Maven and installing ten pypi packages is several minutes of cluster
time, and that is time a student should not be sitting through.

Targeted at Python 3.9 so that whatever interpreter the hook host carries is
very likely to run it. ``from __future__ import annotations`` is what allows the
modern annotations below on an older runtime.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request

EXIT_OK = 0
EXIT_FAILED = 1

USER_AGENT = "dbx-vocareum-tools/voclab.py"
HTTP_TIMEOUT_SECONDS = 60

# Four attempts, three waits between them. Long enough to ride out a rate limit
# and short enough that a hook does not appear to hang while a student waits.
RETRY_BACKOFF_SECONDS = (2.0, 4.0, 8.0)

# ---------------------------------------------------------------------------
# Course values
#
# Everything below that a course could reasonably want different is read from an
# environment variable at call time, and the constant beside it is only what a
# course that says nothing gets. ``voclib.sh`` sources ``course.env`` out of
# /voc/scripts before any hook runs, which is where those variables come from.
#
# The split is not "everything configurable". It is: a value whose right answer
# depends on the course is read from the course, and a value whose right answer
# is the same for any Databricks lab keeps a default here. So the runtime, the
# library list, the catalog and the notebooks are the course's, and the single
# node shape, the retry policy and the autotermination ceiling are not.
#
# Read at call time rather than at import, because an argparse default is
# evaluated when the parser is built and every other environment read in this
# program happens when the subcommand runs. Two reading moments in one program
# is a bug waiting for a test that sets an environment variable late.
# ---------------------------------------------------------------------------

# What a course gets when it names no runtime. Deliberately not the ML build:
# an ML runtime is several minutes slower to boot and a course that needs one
# should have to say so, where a course that quietly got one would never find
# out it was paying for it. Whether a workspace offers whichever version is
# asked for is measured on the create path by ``preflight_cluster_settings``, so
# a workspace that does not reaches the session record as
# UNAVAILABLE_SPARK_VERSION with the versions it does offer.
DEFAULT_SPARK_VERSION = "17.3.x-scala2.13"
SPARK_VERSION_VAR = "VOC_COURSE_SPARK_VERSION"

# i3.xlarge, 30.5 GB and 4 cores, and the reason is worth recording because the
# obvious reason is wrong.
#
# The Vocareum workspace's "Create compute" screen offers five node types and
# nothing else: i3.xlarge, i3.2xlarge, i3.4xlarge, i3en.xlarge, g4dn.xlarge. That
# looks like a workspace limit and is not one. Measured:
# clusters/list-node-types returns hundreds of types including m5.large, and
# the five in the dropdown are character-for-character the node_type_id allowlist
# of the built-in **Personal Compute** policy, in that order, with i3.xlarge as
# its defaultValue. The screen was filtered by the policy the UI had selected.
#
# Nothing here sets policy_id, so that allowlist never applies to these clusters
# and a wider choice is available than the screenshot suggested. i3.xlarge is
# kept anyway: it is what the Personal Compute default would give a student who
# made their own cluster, so the provisioned cluster and a hand-made one behave
# alike, and 4 cores with 30.5 GB is ample for labs that push the work to Aura.
#
# What a workspace actually offers is read live on the create path rather than
# assumed from any of this; see ``preflight_cluster_settings``.
DEFAULT_NODE_TYPE_ID = "i3.xlarge"
NODE_TYPE_VAR = "VOC_COURSE_NODE_TYPE"

# The catalog a student's cluster opens in. No default, because a catalog name a
# course did not ask for is a catalog that does not exist, and a cluster whose
# initial catalog is missing fails at the first cell rather than at create.
CATALOG_VAR = "VOC_COURSE_CATALOG"

# The instance pool a cluster takes a booted box from, when the workspace holds
# one. Found by name rather than by ID, for the same reason a cluster is: an
# instance_pool_id means nothing outside the one workspace that minted it, and
# Vocareum creates a workspace per part, so an ID written down here would be
# wrong for every part but one. A name is something an instructor can create
# ahead of a class and these hooks can look for.
#
# VOC_POOL_NAME overrides it, so one workspace can host two courses without them
# fighting over the same pool. Absent, the default below is what is looked for.
#
# No pool is not an error. ``resolve_pool`` falls back to DEFAULT_NODE_TYPE_ID
# and records which case it was, because a student's session is the wrong place
# to enforce instructor setup: the class runs either way and the record is what
# keeps the mistake from staying invisible. docs/pre-warm.md is the account of
# what a pool buys, which is the 10 minutes a cluster spends waiting on AWS and
# not the 6 its libraries take.
DEFAULT_POOL_NAME = "workshop-pool"
POOL_NAME_VAR = "VOC_POOL_NAME"

# What instance-pools/list calls a pool that can hand out a box. A pool in any
# other state is treated as absent rather than waited on, because the alternative
# is a student's provisioning blocking on an object an instructor may have
# deleted.
ACTIVE_POOL_STATE = "ACTIVE"

# At most this many options in a preflight message. Both messages end up in
# voccustomdata.txt as a single field, and a workspace offers something like
# sixty runtimes, so the whole list would be a wall of text nobody reads. Ten is
# enough to recognize the family and pick a replacement from.
PREFLIGHT_OPTION_LIMIT = 10

# The last line of defense against an unbounded bill, not a tidiness setting.
# A student cluster was measured still RUNNING after Vocareum's session record
# read Terminated. So lab_end.sh cannot be assumed to run, nor to reach the
# cluster when it does, and this number bounds the bill on its own.
#
# 90 rather than 60, because 60 does not clear a scheduled break. A cluster and
# its libraries take about 16 minutes to come back, and idle minutes on a
# cluster nobody returns to are cheaper than a student waiting that long again.
DEFAULT_AUTOTERMINATION_MINUTES = 90
AUTOTERMINATION_VAR = "VOC_COURSE_AUTOTERMINATION_MINUTES"

# spark.master carries the number of local task retries, not a core count, so it
# is 4 regardless of the node type. The three settings together are what
# Databricks reads as a single-node cluster; changing one without the others
# produces a cluster that waits forever for workers.
SINGLE_NODE_SPARK_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*, 4]",
}

# What the labs import, named by the course rather than by this file.
#
# The format is one entry per line or per comma, each ``kind:value``, because
# the alternative is JSON in a shell variable and the file this is read from is
# sourced by bash. ``pypi:neo4j==6.2.0`` and
# ``maven:org.neo4j:neo4j-connector-apache-spark_2.13:5.4.3_for_spark_3`` are
# the two the workshop uses; the split is on the first colon only, which is what
# lets a Maven coordinate keep its own.
#
# No default. A course that installs nothing is a real course, and inventing a
# library list for one would put somebody else's dependency on their cluster.
LIBRARIES_VAR = "VOC_COURSE_LIBRARIES"

# How each ``kind`` becomes the library specification the API wants. Five of the
# six documented kinds take a single string, which is what makes a one-line
# format possible at all; ``requirements`` and the pypi ``repo`` variant are
# left out because neither can be said in one field and neither has been needed.
LIBRARY_KINDS = {
    "pypi": lambda value: {"pypi": {"package": value}},
    "maven": lambda value: {"maven": {"coordinates": value}},
    "cran": lambda value: {"cran": {"package": value}},
    "jar": lambda value: {"jar": value},
    "whl": lambda value: {"whl": value},
}

# A cluster in one of these states is on its way up or already up, so asking for
# it to start again is at best wasted and at worst an error.
LIVE_CLUSTER_STATES = frozenset({"RUNNING", "PENDING", "RESIZING", "RESTARTING"})

# A library in one of these states will not arrive without being asked for
# again. Every other status means it is installed or on its way, so re-requesting
# it would be noise.
UNSATISFIED_LIBRARY_STATUSES = frozenset({"FAILED", "UNINSTALL_ON_RESTART"})

# Databricks caps a cluster name at 100 characters and accepts a narrow
# character set. A Vocareum lab identity looks like student1_200000@vocareum.com,
# so the local part is already within both limits, but a name is what this
# program uses as its idempotency key and a silently truncated key is a second
# cluster.
CLUSTER_NAME_MAX_LENGTH = 100
UNSAFE_NAME_CHARACTERS = re.compile(r"[^A-Za-z0-9_-]+")

# Keys the shell side will accept. voc_voclab rejects anything else rather than
# building a variable name out of it.
VALID_OUTPUT_KEY = re.compile(r"^[a-z][a-z0-9_]*$")

# The notebooks a student is handed, and where their copies go.
#
# They travel in /voc/scripts beside this file rather than in /voc/startercode,
# and that is not where they were first put. Measured in Student View: a
# notebook uploaded to /voc/startercode never reached the student's Databricks
# workspace folder /Users/<lab identity>. startercode reaches the Vocareum
# environment beside Databricks, not the Databricks workspace, so nothing
# Vocareum does on its own puts a notebook in front of a student here.
# ``notebooks_import`` below is what does, and the files have to be somewhere a
# hook can read them, which is the folder the hooks themselves are uploaded to.
# Vocareum executes only the four hooks plus grade.sh out of that folder, so an
# .ipynb sitting there is inert.
#
# A list, comma or whitespace separated, of file names and directory names. A
# name is searched for in the same three places ``voc_find`` searches; a
# directory contributes every notebook under it, at any depth, keeping the
# relative path so a course can hand a student a folder of labs rather than a
# heap of files. Empty is a valid course: it means nothing is imported and
# ``notebook-import`` reports that rather than failing, because a course whose
# content reaches the student some other way is not a broken course.
NOTEBOOKS_VAR = "VOC_COURSE_NOTEBOOKS"
NOTEBOOK_USER_FOLDER = "/Users"

# What ``workspace/import`` is told a file is. JUPYTER for a notebook, which
# carries its own language in its metadata, and SOURCE for the plain-text forms,
# which do not and so have to be told. Anything else under a named directory is
# skipped rather than guessed at: importing a CSV as a notebook succeeds and
# produces an object nobody can open.
#
# ``.py`` is AUTO, and it is the one entry here that declines to say what the
# file is. A ``.py`` module imported as SOURCE fails with
# ``NotebookImportException: Unable to import module ... The following file
# appears to be a notebook``. SOURCE with a language always produces a NOTEBOOK,
# and Python's import machinery refuses one. AUTO reads the path first and the
# content second:
#
#     .py, no ``# Databricks notebook source`` first line  -> FILE
#     .py carrying that line                               -> NOTEBOOK
#     any content at a path with no extension              -> FILE
#
# So AUTO alone is not the fix, and it is not safe alone either. The extension
# has to survive onto the workspace path as well, which is what
# NOTEBOOK_KEEP_EXTENSIONS is for, and the two are one change: AUTO on a
# stripped path makes a FILE out of every notebook in every course. A course
# that wants a ``.py`` delivered as a notebook says so in the file, with the
# first line Databricks' own export already writes.
#
# The other plain-text extensions stay SOURCE. Nothing imports a ``.sql`` or a
# ``.scala``, so a notebook is what a course naming one is asking for, and
# moving them to AUTO would silently change what they arrive as.
NOTEBOOK_FORMATS = {
    ".ipynb": ("JUPYTER", None),
    ".py": ("AUTO", None),
    ".sql": ("SOURCE", "SQL"),
    ".scala": ("SOURCE", "SCALA"),
    ".r": ("SOURCE", "R"),
}

# The extensions that stay on the student's workspace path rather than being
# stripped off it by ``notebook_workspace_path``.
#
# Only ``.py``, because it is the only extension AUTO needs in order to tell a
# module from a notebook. Everything else is stripped, for the cosmetic reason
# that function's docstring gives, and nothing here changes that.
#
# Where the extension survives is decided by Databricks and differs by format.
# Measured:
#
#     JUPYTER, any path        -> stored as written, extension and all
#     AUTO deciding FILE       -> stored as written, extension and all
#     AUTO deciding NOTEBOOK   -> extension stripped by Databricks
#
# Keeping ``.py`` is safe because a headerless module takes the second line: the
# object lands at exactly the path this module built, so the ``get-status``
# check that makes the import idempotent asks about a path that exists, and
# ``user_setup.sh`` leaves a returning student's work alone.
#
# That check is also why this constant is a list of one rather than a rule.
# Under the third line the path written and the path checked would differ, the
# check would 404 on every run, and every start after a ``stop`` would re-import
# over the student's work. Today no entry in NOTEBOOK_FORMATS can reach that
# line with an extension kept, and this constant is what keeps it that way.
NOTEBOOK_KEEP_EXTENSIONS = (".py",)

# The SQL warehouse the course's provisioning runs its statements against.
#
# It exists because the Statement Execution API takes a warehouse_id and there
# is no other way to run SQL from a hook. The comments and grants that make a
# Genie space answer questions correctly rather than plausibly are SQL, so a
# course that drops the warehouse to save a call drops those with it.
#
# Named, not defaulted, for the same reason the catalog is: this object is
# billable, and creating one nobody asked for is a bill nobody is expecting.
# ``auto_stop_mins`` is stated rather than left to the API's default and is
# recorded in voccustomdata.txt with everything else, because an instructor who
# leaves one running after a class is the failure mode worth being able to read.
WAREHOUSE_NAME_VAR = "VOC_COURSE_WAREHOUSE_NAME"
WAREHOUSE_SIZE_VAR = "VOC_COURSE_WAREHOUSE_SIZE"
WAREHOUSE_AUTO_STOP_VAR = "VOC_COURSE_WAREHOUSE_AUTO_STOP_MINUTES"

DEFAULT_WAREHOUSE_SIZE = "2X-Small"
DEFAULT_WAREHOUSE_AUTO_STOP_MINUTES = 10

# Serverless, so the first statement after an auto-stop waits seconds rather
# than the several minutes a classic warehouse takes to come back. A workshop
# alternates between talking and running, which is exactly the shape that makes
# a classic warehouse either expensive or slow.
WAREHOUSE_TYPE = "PRO"

# A warehouse in one of these states can take a statement, or is on its way to
# being able to. Any other state means it has to be asked to start.
LIVE_WAREHOUSE_STATES = frozenset({"RUNNING", "STARTING"})

# The sizes the SQL warehouses API accepts, in order. Checked here rather than
# left to the API because a typo in a course.env reaches Databricks as a 400
# naming the field and not the file, and the course value is the thing worth
# naming when the answer is "2x-small is not 2X-Small".
WAREHOUSE_SIZES = (
    "2X-Small",
    "X-Small",
    "Small",
    "Medium",
    "Large",
    "X-Large",
    "2X-Large",
    "3X-Large",
    "4X-Large",
    "5X-Large",
)

# The warehouse Databricks creates by itself, which no course asked for and
# every workspace has.
#
# Measured: a Vocareum-created workspace arrives holding one serverless PRO
# warehouse named "Serverless Starter Warehouse" at cluster_size Small,
# auto_stop_mins 10, created by the same service principal that created the
# workspace. Databricks documents it as
# "a small SQL warehouse called Starter Warehouse" and states no size, so the
# size is a measurement rather than a documented constant and may move.
#
# Small is four times the compute of the 2X-Small this repo asks for everywhere
# else, and a student who picks it out of the compute dropdown, or a dashboard
# that defaults to it, bills at that rate for as long as it runs. Nothing in a
# course names this object, so nothing in a course was going to notice.
#
# Two names because the workspace is not the only place it appears: Databricks'
# own documentation calls it "Starter Warehouse" and the workspace calls it
# "Serverless Starter Warehouse". Both are tried, in this order, so the longer
# and more specific one wins before the substring-shaped one gets a chance.
STARTER_WAREHOUSE_NAMES = ("Serverless Starter Warehouse", "Starter Warehouse")

STARTER_WAREHOUSE_NAME_VAR = "VOC_COURSE_STARTER_WAREHOUSE_NAME"
STARTER_WAREHOUSE_SIZE_VAR = "VOC_COURSE_STARTER_WAREHOUSE_SIZE"

# The three places a hook's companion file may be, in the order voc_find in
# voclib.sh tries them, and for the same reason: Vocareum guarantees neither the
# working directory a hook runs in nor the form of $0. A real session proved $0
# can carry no directory at all, so a path built from it alone is not enough.
NOTEBOOK_SEARCH_DIRECTORIES = (
    os.path.dirname(os.path.abspath(__file__)),
    "/voc/scripts",
    ".",
)

# What Databricks says when the path already holds an object. Both the
# get-status probe and the import itself can produce it, and in both cases it
# means the state this command was asked to reach is already the state.
ALREADY_EXISTS_ERROR_CODE = "RESOURCE_ALREADY_EXISTS"
NOT_FOUND_ERROR_CODE = "RESOURCE_DOES_NOT_EXIST"


class VoclabError(Exception):
    """A failure with an ``error_code`` a human can act on.

    ``fields`` carries anything already established when the failure happened.
    A permissions grant that fails after the cluster was created is the case
    that matters: the cluster exists and its name has to reach the record, or
    the next run has no way to know it is there.
    """

    def __init__(self, code: str, message: str, fields: dict | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.fields = dict(fields or {})


class ApiError(Exception):
    """One Databricks request that did not succeed, named by what it asked for.

    Raised by ``Workspace.call`` and turned into a ``VoclabError`` by the caller,
    because which ``error_code`` a failed request deserves depends on what the
    request was for and not on how it failed.
    """

    def __init__(self, method: str, path: str, detail: str) -> None:
        super().__init__(f"{method} {path} failed: {detail}")


def api_detail(status: str, body: str) -> str:
    """Reduce a Databricks error body to one line worth putting in a record.

    A Databricks failure carries a machine-readable ``error_code`` beside its
    prose ``message``, which is worth keeping both halves of. A body that is not
    JSON at all is a bodyless 500 or an HTML error page, and for those the status
    line is the only information there is.
    """
    try:
        payload = json.loads(body)
    except ValueError:
        payload = None
    if isinstance(payload, dict):
        code = payload.get("error_code")
        message = payload.get("message") or payload.get("error")
        if code and message:
            return f"HTTP {status} {code}: {message}"
        if message:
            return f"HTTP {status}: {message}"
    if body.strip():
        return f"HTTP {status}: {body.strip()[:400]}"
    return f"HTTP {status} with no body"


class Workspace:
    """The Databricks workspace API, over a bearer token and nothing else.

    ``opener`` is injectable so the tests can drive every branch below without a
    workspace. It defaults to ``urllib.request.urlopen`` and is called with the
    same signature.
    """

    def __init__(self, host: str, token: str, opener=None) -> None:
        self.host = host
        self.token = token
        self._opener = opener or urllib.request.urlopen

    def call(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        idempotent: bool = False,
    ) -> dict:
        """Make one request, retrying only the failures it is safe to retry.

        ``idempotent`` is the whole reason this is not a generic retry helper.
        A 429 means the request was refused before it ran, so retrying it is
        safe whatever it was. A 500 or a dropped connection means the request may
        well have been carried out, and retrying ``clusters/create`` on that
        basis is how one student ends up with two clusters. So a 5xx is retried
        for a read or an idempotent write and surfaced immediately for a create.
        """
        url = f"{self.host}{path}"
        payload = None if body is None else json.dumps(body).encode("utf-8")
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

        attempts = len(RETRY_BACKOFF_SECONDS) + 1
        for attempt in range(attempts):
            request = urllib.request.Request(
                url, data=payload, headers=headers, method=method
            )
            try:
                with self._opener(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                    text = response.read().decode("utf-8")
                return json.loads(text) if text.strip() else {}
            except urllib.error.HTTPError as error:
                detail = api_detail(str(error.code), read_error_body(error))
                retryable = error.code == 429 or (error.code >= 500 and idempotent)
            except urllib.error.URLError as error:
                detail = f"could not reach {self.host}: {error.reason}"
                retryable = idempotent
            except (ValueError, OSError) as error:
                detail = f"unreadable response: {error}"
                retryable = idempotent

            last = attempt == attempts - 1
            if not retryable or last:
                raise ApiError(method, path, detail)
            wait = RETRY_BACKOFF_SECONDS[attempt]
            log(f"{method} {path}: {detail}; retrying in {wait:.0f}s")
            time.sleep(wait)

        raise AssertionError("unreachable")


def read_error_body(error: urllib.error.HTTPError) -> str:
    """Read an error body without letting the read become the reported failure."""
    # Broad on purpose. A body that cannot be read is not the failure worth
    # reporting; the status code above it is, and losing that to a second
    # exception raised while explaining the first would be the worse outcome.
    try:
        return error.read().decode("utf-8", "replace")
    except Exception:
        return ""


def log(message: str) -> None:
    """Say something to whoever is watching, on the channel the shell logs to.

    stderr, because stdout is the return channel. A print to stdout here would
    be read by ``voc_voclab`` as a malformed field.
    """
    print(f"voclab.py: {message}", file=sys.stderr)


def single_line(value: str) -> str:
    """Collapse a value onto one line, because the output contract is line-based.

    A Databricks error message runs to several lines often enough that this is
    not a theoretical concern: without it, the second line of a message becomes
    a field the shell cannot parse and reports as unparsable output.
    """
    return " ".join(str(value).split())


def emit(fields: dict) -> None:
    """Write the return value. Insertion order, so a run is reproducible."""
    for key, value in fields.items():
        if not VALID_OUTPUT_KEY.match(key):
            raise AssertionError(f"invalid output key: {key!r}")
        print(f"{key}={single_line(value)}")


def course_value(name: str, default: str = "") -> str:
    """Read one course value, treating blank as absent.

    Blank is absent because ``course.env`` is a shell file a human edits, and a
    key left as ``VOC_COURSE_CATALOG=`` is a key that has not been filled in.
    Reading that as "the catalog is the empty string" produces a cluster whose
    initial catalog is unset in a way nothing reports.
    """
    return (os.environ.get(name) or "").strip() or default


def course_int(name: str, default: int) -> int:
    """Read a course value that has to be a number, or fail saying which.

    Raises rather than falling back to the default. A course that wrote
    ``VOC_COURSE_AUTOTERMINATION_MINUTES=ninety`` has an opinion about the
    value, and quietly substituting 90 for it means the opinion never arrives
    and nobody is told.
    """
    raw = course_value(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise VoclabError(
            "BAD_COURSE_VALUE",
            f"{name} is {raw!r}, which is not a whole number of minutes.",
        ) from None


def parse_libraries(raw: str) -> list:
    """Turn the course's library list into what ``libraries/install`` wants.

    Separators are newline and comma, so the value reads naturally in a
    ``course.env`` either way. The kind is split off at the **first** colon only,
    which is the whole reason this is not a generic split: a Maven coordinate is
    three colon-separated fields and would otherwise lose two of them.

    An unrecognized kind fails rather than being skipped. A silently dropped
    library is a cluster that comes up fine and a lab that fails at an import
    several minutes later, which is the most expensive place for this to go
    wrong.
    """
    entries = [item.strip() for item in raw.replace(",", "\n").split("\n")]
    libraries = []
    for entry in entries:
        if not entry:
            continue
        kind, _, value = entry.partition(":")
        kind = kind.strip().lower()
        value = value.strip()
        if kind not in LIBRARY_KINDS or not value:
            raise VoclabError(
                "BAD_COURSE_VALUE",
                single_line(
                    f"{LIBRARIES_VAR} holds {entry!r}, which is not "
                    f"kind:value with kind one of "
                    f"{', '.join(sorted(LIBRARY_KINDS))}."
                ),
            )
        libraries.append(LIBRARY_KINDS[kind](value))
    return libraries


def course_libraries() -> list:
    """The libraries this course asks for, or none."""
    return parse_libraries(course_value(LIBRARIES_VAR))


def course_notebook_entries(raw: str) -> list:
    """Split the course's notebook list into names, dropping the empties."""
    return [item.strip() for item in raw.replace(",", "\n").split("\n") if item.strip()]


def cluster_name_for(user_email: str) -> str:
    """Name a student's cluster after the local part of their lab identity.

    The name is the idempotency key for everything below, which is what lets
    ``lab_setup.sh`` pre-warm a cluster and ``user_setup.sh`` guarantee one
    without the second call undoing or duplicating the first. It matches the
    reference integration's scheme so that a workspace holding clusters from
    either can be read the same way.
    """
    local = user_email.split("@")[0]
    safe = UNSAFE_NAME_CHARACTERS.sub("_", local).strip("_")
    if not safe:
        raise VoclabError(
            "BAD_USER_EMAIL",
            f"No usable cluster name could be built from {user_email!r}.",
        )
    return safe[:CLUSTER_NAME_MAX_LENGTH]


def parse_resource_tags(raw: str) -> dict:
    """Read ``VOC_RESOURCE_TAGS`` into Databricks custom tags.

    Vocareum documents this variable as "tags to put on anything billable this
    script creates" and does not document its format, so this accepts the two
    plausible ones: a JSON object, or a comma-separated list of ``key=value``
    pairs. An unrecognized format is dropped with a warning rather than guessed
    at, because Databricks rejects a tag value containing a comma and a rejected
    tag fails the whole ``clusters/create`` call. Dropping the tags costs
    billing attribution; guessing costs the cluster.
    """
    text = (raw or "").strip()
    if not text:
        return {}

    if text.startswith("{"):
        try:
            payload = json.loads(text)
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            return {str(key): single_line(value) for key, value in payload.items()}
        log(
            f"VOC_RESOURCE_TAGS is not a JSON object, so no tags were applied: {text!r}"
        )
        return {}

    tags = {}
    for item in text.split(","):
        if "=" not in item:
            log(
                "VOC_RESOURCE_TAGS holds a fragment that is not key=value, so "
                f"no tags were applied: {item!r}"
            )
            return {}
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            log("VOC_RESOURCE_TAGS holds an empty tag key, so no tags were applied")
            return {}
        tags[key] = single_line(value)
    return tags


def cluster_settings(
    cluster_name: str,
    user_email: str,
    node_type_id: str,
    spark_version: str,
    autotermination_minutes: int,
    catalog: str | None = None,
    tags: dict | None = None,
    instance_pool_id: str | None = None,
) -> dict:
    """Build the ``clusters/create`` body for one student's cluster.

    Single node, single user. Single node because every lab in this workshop
    reads from Aura or writes to it rather than shuffling across workers, so a
    worker would idle while being billed. Single user because ``SINGLE_USER``
    access mode is what allows the Neo4j Maven connector to be installed
    directly: on a ``USER_ISOLATION`` cluster the same jar has to be added to
    the metastore's artifact allowlist first, which is a shared-state
    prerequisite a per-student hook has no business creating.

    A pooled body is a different body rather than the same body with one key
    swapped, and that is measured rather than assumed. Sending the full body
    below alongside ``instance_pool_id`` answers ``400
    INVALID_PARAMETER_VALUE``, naming each offending field in turn: first
    ``enable_elastic_disk``, then ``aws_attributes.zone_id``, then
    ``node_type_id``. All three are pool-owned, so all three come out when a
    pool is named. docs/pre-warm.md holds the table of what was sent and what
    came back.

    The one that is not merely a shape difference is ``first_on_demand``. It
    keeps the driver off the spot market, and on a single-node cluster the
    driver is the whole cluster, so dropping it without replacing it hands every
    student a cluster a spot reclaim can kill mid-lab. Its job moves to the pool,
    which ``dbx-vocareum-pool --create`` creates ``ON_DEMAND`` for that reason. A
    pool made by hand with defaults is ``SPOT``, which is why the pool is created
    by a tool rather than through the console.
    """
    custom_tags = dict(tags or {})
    if "ResourceClass" in custom_tags:
        log("VOC_RESOURCE_TAGS carries ResourceClass, which is reserved here")
    # Ours last. ResourceClass is not decoration: Databricks reads it alongside
    # num_workers to bill and schedule the cluster as a single node.
    custom_tags["ResourceClass"] = "SingleNode"

    spark_conf = dict(SINGLE_NODE_SPARK_CONF)
    if catalog:
        spark_conf["spark.databricks.sql.initial.catalog.name"] = catalog

    settings = {
        "cluster_name": cluster_name,
        "spark_version": spark_version,
        "num_workers": 0,
        "autotermination_minutes": autotermination_minutes,
        "data_security_mode": "SINGLE_USER",
        "single_user_name": user_email,
        "spark_conf": spark_conf,
        "custom_tags": custom_tags,
        "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
    }

    if instance_pool_id:
        settings["instance_pool_id"] = instance_pool_id
        return settings

    settings["node_type_id"] = node_type_id
    settings["enable_elastic_disk"] = True
    # zone_id auto lets Databricks place the node in whichever availability
    # zone has capacity, which matters on a workshop morning when thirty
    # clusters are requested at once. first_on_demand 1 keeps the driver off
    # the spot market, and on a single-node cluster the driver is the whole
    # cluster. A pool gives the first of these up: it accepts auto and resolves
    # it once, at creation, to one concrete zone.
    settings["aws_attributes"] = {"zone_id": "auto", "first_on_demand": 1}
    return settings


def library_key(library: dict) -> str:
    """A stable identity for a library specification, for comparing two lists."""
    return json.dumps(library, sort_keys=True)


def missing_libraries(statuses: list, wanted) -> list:
    """Work out which libraries still have to be asked for.

    This is what makes ``cluster-ensure`` cheap on the second call. A library
    already installed or on its way is left alone; one that failed is asked for
    again, because a failed install does not retry on its own.
    """
    satisfied = set()
    for entry in statuses or []:
        library = entry.get("library")
        if not isinstance(library, dict):
            continue
        if entry.get("status") in UNSATISFIED_LIBRARY_STATUSES:
            continue
        satisfied.add(library_key(library))
    return [item for item in wanted if library_key(item) not in satisfied]


def find_cluster(workspace: Workspace, cluster_name: str) -> dict:
    """Find a cluster by name, or return ``None``.

    The list API does not return permanently deleted clusters, so a name that
    was reclaimed by ``cluster-remove`` reads as absent and is created afresh.
    Two clusters can share a name; the first match wins, as in the reference,
    and a duplicate is a thing to notice in the workspace rather than something
    this program can resolve.
    """
    try:
        payload = workspace.call("GET", "/api/2.0/clusters/list", idempotent=True)
    except ApiError as error:
        raise VoclabError("CLUSTER_LIST_FAILED", single_line(str(error))) from error
    for cluster in payload.get("clusters") or []:
        if cluster.get("cluster_name") == cluster_name:
            return cluster
    return None


def find_pool(workspace: Workspace, pool_name: str) -> dict:
    """Find an instance pool by name, or return ``None``.

    The same shape as ``find_cluster`` and for the same reason: the name is the
    join key, because an ``instance_pool_id`` is meaningless outside the
    workspace that issued it and this program runs against a workspace Vocareum
    created. Two pools sharing a name is a thing to notice in the workspace
    rather than something this can resolve, so the first match wins.
    """
    try:
        payload = workspace.call("GET", "/api/2.0/instance-pools/list", idempotent=True)
    except ApiError as error:
        raise VoclabError("POOL_LIST_FAILED", single_line(str(error))) from error
    for pool in payload.get("instance_pools") or []:
        if pool.get("instance_pool_name") == pool_name:
            return pool
    return None


def pool_preload_verdict(pool: dict, spark_version: str) -> str:
    """Whether the pool preloaded the runtime this cluster is asking for.

    ``hit`` or ``miss``, and a miss is recorded rather than raised. Measured: a
    pool preloading only ``17.3.x-cpu-ml-scala2.13`` accepted a cluster asking
    for ``17.3.x-scala2.13``, which reached ``PENDING`` carrying the version it
    asked for and warned about nothing. So a miss is not a failure, it is a pool
    that is paying for idle boxes and saving a student nothing, and the only way
    anyone ever finds out is if something writes it down.
    """
    preloaded = pool.get("preloaded_spark_versions") or []
    return "hit" if spark_version in preloaded else "miss"


def resolve_pool(
    workspace: Workspace, pool_name: str, spark_version: str, fields: dict
) -> str:
    """Decide which pool this cluster comes off, and record which case it was.

    Returns an ``instance_pool_id`` or ``None``. It does not raise, which is the
    whole point: a pool is an optimization an instructor sets up before a class,
    and every way it can be missing has to end in a cluster rather than in a
    student looking at a failed lab. So an absent pool, an inactive one, and a
    listing that could not be read all fall back to ``node_type_id``.

    ``pool`` reaches ``voccustomdata.txt`` naming the case: the pool's own name
    when one served, and ``absent``, ``inactive`` or ``unreadable`` when none
    did. Without it, a class that silently ran unpooled is indistinguishable
    from one that ran pooled and saved nothing.
    """
    try:
        pool = find_pool(workspace, pool_name)
    except VoclabError as error:
        fields["pool"] = "unreadable"
        log(f"could not list instance pools, so a node type is named: {error.message}")
        return None

    if pool is None:
        fields["pool"] = "absent"
        log(f"no instance pool named {pool_name}, so the cluster names a node type")
        return None

    state = pool.get("state") or "UNKNOWN"
    pool_id = pool.get("instance_pool_id")
    if state != ACTIVE_POOL_STATE or not pool_id:
        fields["pool"] = "inactive"
        log(f"instance pool {pool_name} reads {state}, so a node type is named")
        return None

    fields["pool"] = pool_name
    fields["pool_preload"] = pool_preload_verdict(pool, spark_version)
    if fields["pool_preload"] == "miss":
        log(
            f"instance pool {pool_name} did not preload {spark_version}, so the "
            "box is ready and the runtime still has to be downloaded"
        )
    log(f"instance pool {pool_name} ({pool_id}) will supply the node")
    return pool_id


def install_libraries(
    workspace: Workspace, cluster_id: str, wanted, fields: dict
) -> int:
    """Ask for whatever libraries are not already accounted for.

    ``fields`` is passed in so that a failure here still carries the cluster
    that was created, which is the difference between a next run that finds its
    cluster and one that cannot tell whether it has one.

    A course that asks for nothing costs nothing. Without the early return the
    status read below still happens, which is a round trip in front of a student
    that cannot change any answer.
    """
    if not wanted:
        log("this course names no libraries, so none were requested")
        return 0

    try:
        status = workspace.call(
            "GET",
            f"/api/2.0/libraries/cluster-status?cluster_id={cluster_id}",
            idempotent=True,
        )
    except ApiError as error:
        raise VoclabError(
            "LIBRARY_STATUS_FAILED", single_line(str(error)), fields
        ) from error

    outstanding = missing_libraries(status.get("library_statuses"), wanted)
    if not outstanding:
        log("every library is already installed or on its way")
        return 0

    try:
        workspace.call(
            "POST",
            "/api/2.0/libraries/install",
            {"cluster_id": cluster_id, "libraries": outstanding},
        )
    except ApiError as error:
        raise VoclabError(
            "LIBRARY_INSTALL_FAILED", single_line(str(error)), fields
        ) from error
    log(f"requested {len(outstanding)} of {len(wanted)} libraries")
    return len(outstanding)


def grant_restart(
    workspace: Workspace, cluster_id: str, user_email: str, fields: dict
) -> None:
    """Let the student restart their own cluster, and nothing more.

    ``CAN_RESTART`` rather than ``CAN_MANAGE`` because a student needs to bring
    a terminated cluster back and does not need to resize or reconfigure it. The
    token this runs under keeps ``IS_OWNER`` implicitly.

    This is the call most likely to fail on a pre-warm, with the
    ``Principal not found in account`` error ``docs/permissions.md`` describes:
    Vocareum is the account-wide SAML identity provider, and the student
    identity may not have propagated when ``lab_setup.sh`` runs. That is an
    ordering problem rather than a privilege one, and it recovers by itself,
    because ``user_setup.sh`` runs ``cluster-ensure`` again and reaches this same
    call with the identity in place.
    """
    body = {
        "access_control_list": [
            {"user_name": user_email, "permission_level": "CAN_RESTART"}
        ]
    }
    try:
        workspace.call(
            "PUT",
            f"/api/2.0/permissions/clusters/{cluster_id}",
            body,
            idempotent=True,
        )
    except ApiError as error:
        raise VoclabError(
            "PERMISSIONS_FAILED", single_line(str(error)), fields
        ) from error


def available_keys(
    workspace: Workspace, path: str, container: str, field: str
) -> list | None:
    """Read one list of what the workspace offers, or ``None`` if it cannot be.

    ``None`` rather than an exception, and this is the one place in this program
    where a failed request is not fatal. A workspace that refuses to list its
    node types would otherwise be an unprovisionable lab, and the check being
    performed here is only ever a courtesy: ``clusters/create`` is the authority
    on whether a setting is accepted, so a preflight that cannot run should get
    out of the way of it. An unexpected payload shape is treated the same as a
    failed request, because a shape this does not recognize is a shape it cannot
    draw a conclusion from.
    """
    try:
        payload = workspace.call("GET", path, idempotent=True)
    except ApiError as error:
        log(
            f"could not read {path}, so clusters/create will decide: "
            f"{single_line(str(error))}"
        )
        return None

    entries = payload.get(container)
    if not isinstance(entries, list):
        log(f"{path} returned no {container} list, so clusters/create will decide")
        return None

    keys = [
        entry.get(field)
        for entry in entries
        if isinstance(entry, dict) and entry.get(field)
    ]
    if not keys:
        log(f"{path} named no {field} values, so clusters/create will decide")
        return None
    return keys


def describe_options(candidates: list) -> str:
    """List a bounded sample of what is on offer, and say what was left out.

    The count matters as much as the sample: "and 48 more" tells whoever reads
    the session record that the workspace is fully stocked and the pinned value
    is simply wrong, which reads very differently from a workspace offering four
    runtimes in total.
    """
    shown = list(candidates)[:PREFLIGHT_OPTION_LIMIT]
    remaining = len(candidates) - len(shown)
    text = ", ".join(shown)
    if remaining > 0:
        text = f"{text}, and {remaining} more"
    return text


def spark_version_family(key: str) -> tuple:
    """Reduce a runtime key to the traits a substitute would have to share.

    A key looks like ``17.3.x-cpu-ml-scala2.13``. What makes one runtime a
    plausible replacement for another is not its version number but whether it
    carries the ML libraries, whether it is a GPU build, and which Scala it was
    built against, because the Neo4j Maven coordinate is pinned to a Scala
    version. So the family is those three traits and the number is deliberately
    dropped: it is what lets the failure message show six relevant runtimes
    instead of sixty mostly irrelevant ones.
    """
    parts = key.lower().split("-")
    scala = re.search(r"scala\d+\.\d+", key.lower())
    return ("ml" in parts, "gpu" in parts, scala.group(0) if scala else "")


def preflight_cluster_settings(
    workspace: Workspace,
    node_type_id: str | None,
    spark_version: str,
    fields: dict,
) -> None:
    """Check the two create settings a workspace is most likely not to offer.

    Both are pinned constants, and a workspace that does not offer one of them
    used to surface as a raw API message under ``CLUSTER_CREATE_FAILED``, which
    named the problem in Databricks' words rather than in terms of what to do
    next. Asking first costs two reads and buys a message that names the options.

    A mismatch fails rather than substituting the nearest thing on offer. A
    course tested on one runtime must not quietly run on another: the Maven
    connector coordinate, the Scala version and ``sentence_transformers`` all
    have to agree, and a substitution that breaks Lab 3 halfway through is worse
    for a class than a lab that refuses to start and says why.

    ``node_type_id`` is ``None`` when a pool is supplying the node, and then the
    node type check is skipped rather than run against a value the create body
    will not carry. The pool owns the node type: the cluster inherits it, and
    measured, a pooled cluster reads back the pool's ``i3.xlarge`` without ever
    having named one.

    Called only from the create branch of ``cluster_ensure``. These settings are
    read only when a cluster is being created, so a student arriving at a
    pre-warmed cluster should not wait on two requests that cannot change the
    outcome.

    ``fields`` is carried into both failures for the same reason every other
    failure in this module carries it: a record saying a runtime is unavailable
    without naming whose provisioning stopped leaves a reader guessing which
    student to go and look at.
    """
    versions = available_keys(
        workspace, "/api/2.0/clusters/spark-versions", "versions", "key"
    )
    if versions is not None and spark_version not in versions:
        family = spark_version_family(spark_version)
        relevant = [key for key in versions if spark_version_family(key) == family]
        raise VoclabError(
            "UNAVAILABLE_SPARK_VERSION",
            single_line(
                f"The workspace does not offer Spark version {spark_version}, so "
                f"no cluster was created. Comparable versions it does offer: "
                f"{describe_options(relevant or versions)}."
            ),
            fields,
        )

    if node_type_id is None:
        log("a pool supplies the node, so no node type is checked")
        return

    node_types = available_keys(
        workspace, "/api/2.0/clusters/list-node-types", "node_types", "node_type_id"
    )
    if node_types is not None and node_type_id not in node_types:
        raise VoclabError(
            "UNAVAILABLE_NODE_TYPE",
            single_line(
                f"The workspace does not offer node type {node_type_id}, so no "
                f"cluster was created. Node types it does offer: "
                f"{describe_options(node_types)}."
            ),
            fields,
        )


def cluster_ensure(
    workspace: Workspace,
    user_email: str,
    node_type_id: str,
    spark_version: str,
    autotermination_minutes: int,
    catalog: str | None = None,
    tags: dict | None = None,
    pool_name: str = DEFAULT_POOL_NAME,
    libraries=None,
) -> dict:
    """Make sure the student has a cluster, warming or creating as needed.

    Safe to call more than once, which is the property the whole design rests
    on: ``lab_setup.sh`` calls it to pre-warm and ``user_setup.sh`` calls it to
    guarantee, and a student who arrives at a lab that was never pre-warmed is
    served by exactly the same path.

    The pool is looked for on the create branch only, beside the preflight and
    for the same reason: a cluster that already exists came off whatever the
    pool situation was when it was created, and re-deciding that for a student
    who is only waiting for their cluster to start would be a round trip that
    cannot change anything. So a record carrying no ``pool`` field is a record of
    a run that found its cluster already there.
    """
    cluster_name = cluster_name_for(user_email)
    existing = find_cluster(workspace, cluster_name)
    fields = {"cluster_name": cluster_name}

    if existing is None:
        instance_pool_id = resolve_pool(workspace, pool_name, spark_version, fields)
        # Only here, and only immediately before the create call, because these
        # two settings are read only by clusters/create.
        preflight_cluster_settings(
            workspace,
            None if instance_pool_id else node_type_id,
            spark_version,
            fields,
        )
        settings = cluster_settings(
            cluster_name=cluster_name,
            user_email=user_email,
            node_type_id=node_type_id,
            spark_version=spark_version,
            autotermination_minutes=autotermination_minutes,
            catalog=catalog,
            tags=tags,
            instance_pool_id=instance_pool_id,
        )
        try:
            created = workspace.call("POST", "/api/2.0/clusters/create", settings)
        except ApiError as error:
            raise VoclabError(
                "CLUSTER_CREATE_FAILED", single_line(str(error)), fields
            ) from error
        cluster_id = created.get("cluster_id")
        if not cluster_id:
            raise VoclabError(
                "CLUSTER_CREATE_FAILED",
                "clusters/create returned no cluster_id.",
                fields,
            )
        action = "created"
        state = "PENDING"
        log(f"created cluster {cluster_name} ({cluster_id})")
    else:
        cluster_id = existing.get("cluster_id")
        state = existing.get("state") or "UNKNOWN"
        fields["cluster_id"] = cluster_id
        if state in LIVE_CLUSTER_STATES:
            action = "running"
            log(f"cluster {cluster_name} ({cluster_id}) is already {state}")
        else:
            try:
                workspace.call(
                    "POST",
                    "/api/2.0/clusters/start",
                    {"cluster_id": cluster_id},
                    idempotent=True,
                )
            except ApiError as error:
                raise VoclabError(
                    "CLUSTER_START_FAILED", single_line(str(error)), fields
                ) from error
            log(f"started cluster {cluster_name} ({cluster_id}), which was {state}")
            action = "started"
            state = "PENDING"

    fields["cluster_id"] = cluster_id
    fields["cluster_state"] = state
    fields["cluster_action"] = action

    wanted = course_libraries() if libraries is None else list(libraries)
    requested = install_libraries(workspace, cluster_id, wanted, fields)
    fields["libraries_requested"] = str(requested)

    grant_restart(workspace, cluster_id, user_email, fields)
    fields["granted_to"] = user_email
    return fields


def cluster_release(workspace: Workspace, user_email: str, permanent: bool) -> dict:
    """Reclaim a student's cluster at the end of a session.

    Two depths, because ``VOC_END_LAB_BEHAVIOR`` distinguishes two endings. On
    ``stop`` the cluster is terminated and its configuration kept, and on
    ``terminate`` it is removed outright.

    The difference is smaller than it looks. The next ``cluster-ensure`` cannot
    find the terminated cluster by name and start it. ``cluster_name_for``
    builds the name from the local part of the lab identity, Vocareum mints that
    identity per session, and its local part carries the session's start epoch,
    so the next session looks up a name this one never used. Every ``stop``
    therefore leaves a TERMINATED cluster that nothing will ever start again,
    and the configuration kept is kept for no reader.

    What ``stop`` still buys is the thing worth having: the billing ends. It is
    kept shallower than ``terminate`` because a session Vocareum describes as
    resumable is not a session to delete objects out from under, and because a
    terminated cluster left behind is at least a record of which run created it.
    Whether a per-session name is the right idempotency key at all is the open
    question underneath this, and it is deliberately left open rather than
    answered by a docstring.

    Note the API's naming, which is the opposite way round from what it reads
    like: ``clusters/delete`` terminates and keeps the configuration, and
    ``clusters/permanent-delete`` is the one that removes it.
    """
    cluster_name = cluster_name_for(user_email)
    existing = find_cluster(workspace, cluster_name)
    fields = {"cluster_name": cluster_name}

    if existing is None:
        # Not a failure. Nothing to reclaim is the state this was asked to
        # reach, and a lab_end that ran twice should be quiet the second time.
        fields["cluster_action"] = "absent"
        log(f"no cluster named {cluster_name}, so there is nothing to reclaim")
        return fields

    cluster_id = existing.get("cluster_id")
    fields["cluster_id"] = cluster_id
    path = (
        "/api/2.0/clusters/permanent-delete"
        if permanent
        else "/api/2.0/clusters/delete"
    )
    code = "CLUSTER_REMOVE_FAILED" if permanent else "CLUSTER_STOP_FAILED"
    try:
        workspace.call("POST", path, {"cluster_id": cluster_id}, idempotent=True)
    except ApiError as error:
        raise VoclabError(code, single_line(str(error)), fields) from error

    fields["cluster_action"] = "removed" if permanent else "terminated"
    log(f"{fields['cluster_action']} cluster {cluster_name} ({cluster_id})")
    return fields


def list_warehouses(workspace: Workspace) -> list:
    """Every SQL warehouse the caller can see, or fail naming the list call.

    Split out from ``find_warehouse`` because two callers now look through the
    same list for different names, and a second copy of this call would be a
    second place for ``WAREHOUSE_LIST_FAILED`` to be spelled differently.
    """
    try:
        payload = workspace.call("GET", "/api/2.0/sql/warehouses", idempotent=True)
    except ApiError as error:
        raise VoclabError("WAREHOUSE_LIST_FAILED", single_line(str(error))) from error
    return payload.get("warehouses") or []


def find_warehouse(workspace: Workspace, warehouse_name: str) -> dict:
    """Find a SQL warehouse by name, or return ``None``.

    Third function of this shape, after ``find_cluster`` and ``find_pool``, and
    for the third time the reason is that the name is the only join key that
    survives. Vocareum creates a workspace per part and a ``warehouse_id`` is
    minted by one workspace, so an ID written into a course's config would be
    wrong everywhere but where it was recorded.
    """
    for warehouse in list_warehouses(workspace):
        if warehouse.get("name") == warehouse_name:
            return warehouse
    return None


def grant_warehouse_use(
    workspace: Workspace, warehouse_id: str, group_name: str, fields: dict
) -> None:
    """Let the lab group run statements on the warehouse, and nothing more.

    ``CAN_USE`` rather than ``CAN_MANAGE``: running a query is what a student
    and a Genie space need, and neither needs to resize the warehouse or stop
    it. To the group rather than to the user, which is the rule everywhere in
    these hooks, because Vocareum tears the group down at the end of a session
    and a direct user grant outlives it.
    """
    body = {
        "access_control_list": [
            {"group_name": group_name, "permission_level": "CAN_USE"}
        ]
    }
    try:
        workspace.call(
            "PUT",
            f"/api/2.0/permissions/warehouses/{warehouse_id}",
            body,
            idempotent=True,
        )
    except ApiError as error:
        raise VoclabError(
            "WAREHOUSE_PERMISSIONS_FAILED", single_line(str(error)), fields
        ) from error


def warehouse_ensure(
    workspace: Workspace,
    warehouse_name: str,
    size: str,
    auto_stop_minutes: int,
    group_name: str = "",
) -> dict:
    """Make sure the course has a SQL warehouse, creating or starting as needed.

    The same shape as ``cluster_ensure`` and safe to call as often: the name is
    the idempotency key, and a run that finds the warehouse already there starts
    it if it has stopped and otherwise does nothing.

    Unlike a cluster this object is not per student. It exists because the
    Statement Execution API takes a ``warehouse_id`` and there is no other way
    to run SQL from a hook, and the SQL in question is the ``COMMENT ON`` and
    ``GRANT`` statements that make a Genie space answer questions correctly
    rather than plausibly. One warehouse serves the whole workspace, so this
    belongs in ``workspace_init.sh`` beside the catalog rather than in
    ``user_setup.sh`` beside the cluster.

    It is billable while running, which is why ``auto_stop_mins`` is stated
    rather than left to the API's default and why every value reaches
    ``voccustomdata.txt``. Nothing here ever deletes it: like the instance pool
    it is an instructor-owned object, and ``docs/pre-warm.md`` is where a class
    is told to go looking for one left running.
    """
    fields = {
        "warehouse_name": warehouse_name,
        "warehouse_size": size,
        "warehouse_auto_stop_minutes": str(auto_stop_minutes),
    }
    existing = find_warehouse(workspace, warehouse_name)

    if existing is None:
        # Serverless is stated by the pair of fields, not by one. warehouse_type
        # PRO is what makes serverless available at all, and
        # enable_serverless_compute is what selects it; sending either alone
        # produces a classic warehouse that succeeds and bills differently.
        body = {
            "name": warehouse_name,
            "cluster_size": size,
            "auto_stop_mins": auto_stop_minutes,
            "warehouse_type": WAREHOUSE_TYPE,
            "enable_serverless_compute": True,
            # A workshop's statements are short and serial, so a second cluster
            # would be an idle bill rather than a queue that clears faster.
            "max_num_clusters": 1,
        }
        try:
            created = workspace.call("POST", "/api/2.0/sql/warehouses", body)
        except ApiError as error:
            raise VoclabError(
                "WAREHOUSE_CREATE_FAILED", single_line(str(error)), fields
            ) from error
        warehouse_id = created.get("id")
        if not warehouse_id:
            raise VoclabError(
                "WAREHOUSE_CREATE_FAILED",
                "sql/warehouses returned no id.",
                fields,
            )
        action = "created"
        state = "STARTING"
        log(f"created warehouse {warehouse_name} ({warehouse_id})")
    else:
        warehouse_id = existing.get("id")
        state = existing.get("state") or "UNKNOWN"
        fields["warehouse_id"] = warehouse_id
        if state in LIVE_WAREHOUSE_STATES:
            action = "running"
            log(f"warehouse {warehouse_name} ({warehouse_id}) is already {state}")
        else:
            try:
                workspace.call(
                    "POST",
                    f"/api/2.0/sql/warehouses/{warehouse_id}/start",
                    {},
                    idempotent=True,
                )
            except ApiError as error:
                raise VoclabError(
                    "WAREHOUSE_START_FAILED", single_line(str(error)), fields
                ) from error
            log(f"started warehouse {warehouse_name} ({warehouse_id}), was {state}")
            action = "started"
            state = "STARTING"

    fields["warehouse_id"] = warehouse_id
    fields["warehouse_state"] = state
    fields["warehouse_action"] = action

    # No group is not a failure. workspace_init.sh runs before any student
    # exists and therefore before Vocareum has minted a lab group, and the
    # warehouse it creates there is still the warehouse the course needs. The
    # case is recorded rather than raised so that a later run without a grant is
    # distinguishable from a grant that was never attempted.
    if group_name:
        grant_warehouse_use(workspace, warehouse_id, group_name, fields)
        fields["warehouse_granted_to"] = group_name
    else:
        fields["warehouse_granted_to"] = "none"
        log("no lab group named, so the warehouse was left with its owner alone")
    return fields


def find_starter_warehouse(workspace: Workspace, names) -> dict:
    """The Databricks-created starter warehouse, or ``None``.

    Tries each candidate name in order against the whole list rather than
    walking the list once and testing membership, so that the order in
    ``names`` decides the winner rather than the order Databricks returns
    warehouses in. A workspace holding both spellings is not a case anyone has
    seen, but the tie would otherwise be broken by the API's ordering, which is
    not documented and not something to depend on.
    """
    warehouses = list_warehouses(workspace)
    for name in names:
        for warehouse in warehouses:
            if warehouse.get("name") == name:
                return warehouse
    return None


def starter_warehouse_resize(
    workspace: Workspace, size: str, names=STARTER_WAREHOUSE_NAMES
) -> dict:
    """Put the Databricks-created starter warehouse on the size a course wants.

    This is the one object here the course does not own and cannot decline.
    Databricks creates it with the workspace, grants ``users`` ``CAN_USE`` on
    it, and sizes it at Small, and Vocareum creates a workspace per part, so
    every class gets a fresh one and a fix made by hand in the UI lasts exactly
    one class. That is why this is a hook step rather than a runbook line.

    Measured: the starter warehouse was ``Small`` and carried a ``users``
    ``CAN_USE`` grant, beside the course's own warehouse at ``2X-Small``. Small
    is four times the compute per hour, and the two sit next to each other in
    the same dropdown with nothing to tell a student which one the course meant.

    Resized rather than deleted. Deleting is the cheaper thought and the worse
    one: a deleted warehouse is restorable only by asking Databricks within 14
    days, a dashboard or a saved query pinned to it breaks with an error naming
    a missing warehouse rather than a policy, and the object comes back with
    the next workspace anyway. A size is reversible in one call.

    Absent is not a failure. A workspace without one is a workspace where
    Databricks changed its mind about creating it, and failing
    ``workspace_init.sh`` over an object no course asked for would take a class
    down to save nothing. The case is recorded instead, which is also how a
    later reader learns the name moved.

    ``POST /edit`` is documented as an incremental edit: fields that are not
    sent keep their current values. So this sends ``cluster_size`` alone rather
    than reading the warehouse and writing it back, which would make this
    function the owner of every other field on an object it does not own.
    """
    if size not in WAREHOUSE_SIZES:
        raise VoclabError(
            "INVALID_WAREHOUSE_SIZE",
            single_line(
                f"{size!r} is not a SQL warehouse size. {STARTER_WAREHOUSE_SIZE_VAR} "
                f"must be one of {', '.join(WAREHOUSE_SIZES)}, and the spelling is "
                "case sensitive."
            ),
            {"starter_warehouse_size": size},
        )

    fields = {"starter_warehouse_size": size}
    existing = find_starter_warehouse(workspace, names)

    if existing is None:
        fields["starter_warehouse_name"] = "none"
        fields["starter_warehouse_action"] = "absent"
        log(
            "no starter warehouse is in this workspace under "
            f"{' or '.join(names)}, so none was resized"
        )
        return fields

    warehouse_id = existing.get("id") or ""
    before = existing.get("cluster_size") or "UNKNOWN"
    fields["starter_warehouse_name"] = existing.get("name") or "unknown"
    fields["starter_warehouse_id"] = warehouse_id
    fields["starter_warehouse_size_before"] = before
    fields["starter_warehouse_state"] = existing.get("state") or "UNKNOWN"

    if before == size:
        fields["starter_warehouse_action"] = "unchanged"
        log(f"starter warehouse {fields['starter_warehouse_name']} is already {size}")
        return fields

    try:
        workspace.call(
            "POST",
            f"/api/2.0/sql/warehouses/{warehouse_id}/edit",
            {"cluster_size": size},
        )
    except ApiError as error:
        raise VoclabError(
            "STARTER_WAREHOUSE_EDIT_FAILED", single_line(str(error)), fields
        ) from error

    fields["starter_warehouse_action"] = "resized"
    log(
        f"resized starter warehouse {fields['starter_warehouse_name']} "
        f"({warehouse_id}) from {before} to {size}"
    )
    return fields


def names_error_code(error: ApiError, code: str) -> bool:
    """Whether a failed request carried this Databricks ``error_code``.

    ``api_detail`` keeps the machine-readable code beside the prose message
    precisely so a caller can ask this without a second exception type, and the
    code is what to match on: the prose beside it is not stable enough to
    branch on and the status code alone does not distinguish two 400s.

    One caveat, which matters everywhere else in this repository and does not
    matter here. Databricks hides rather than refuses, so
    ``RESOURCE_DOES_NOT_EXIST`` normally proves only that the caller cannot see
    anything at that path, not that the path is free. The token a hook runs
    under created the folder it is asking about, so for this one caller the two
    readings coincide.
    """
    return code in str(error)


def notebook_source_path(notebook_file: str) -> str:
    """Find one notebook or one directory of them, uploaded beside the hooks.

    Every relative name is searched for, not resolved from one path, because
    Vocareum guarantees neither the working directory a hook runs in nor the
    form of ``$0``, and this program is reached through a hook. Same three
    candidates in the same order as ``voc_find`` in ``voclib.sh``.

    The search used to apply only to a name with no directory in it, and a name
    like ``Lab_3/01_intro.ipynb`` was handed straight to the filesystem. That
    resolved it against the working directory, which is the one thing the
    comment above says cannot be relied on, so a course naming its notebooks by
    path worked from a laptop and failed inside Vocareum for a reason nothing
    printed. The discriminator is now absolute versus relative rather than
    directory versus bare: an absolute path names a place on this machine and is
    taken as given, which is what keeps the command runnable from a laptop, and
    everything else is looked for beside the hooks. ``.`` is still the third
    candidate, so a relative path that did resolve from the working directory
    still does.

    Returns the path whether it is a file or a directory; deciding which is the
    caller's job, because the two are different in what they contribute and not
    in how they are found.
    """
    if os.path.isabs(notebook_file):
        candidates = [notebook_file]
    else:
        candidates = [
            os.path.join(directory, notebook_file)
            for directory in NOTEBOOK_SEARCH_DIRECTORIES
        ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    raise VoclabError(
        "MISSING_NOTEBOOK",
        single_line(
            f"{notebook_file} is not at any of {', '.join(candidates)}, so this "
            f"course names content it did not ship. {NOTEBOOKS_VAR} in "
            f"course.env is what names it, and `dbx-vocareum-upload lab/` is "
            f"what puts it there."
        ),
    )


def notebook_relative_paths(root: str) -> list:
    """Every importable file under a directory, relative to it, sorted.

    Sorted so two runs against the same content produce the same record. A file
    whose extension is not one Databricks can be told the format of is skipped:
    a course directory holds data files beside its notebooks often enough that
    refusing the whole import over one CSV would be the wrong answer, and
    importing that CSV as a notebook succeeds and produces an object nobody can
    open.
    """
    found = []
    for directory, _, names in os.walk(root):
        for name in names:
            if os.path.splitext(name)[1].lower() not in NOTEBOOK_FORMATS:
                continue
            full = os.path.join(directory, name)
            found.append(os.path.relpath(full, root))
    return sorted(found)


def notebook_relative_name(entry: str, source: str) -> str:
    """What the student's copy of one named file is called.

    The name the course wrote, kept whole. A course naming
    ``Lab_3_Semantic_Search/01_intro.ipynb`` hands the student that folder and
    that notebook inside it, which is the same structure a named *directory*
    already produces and the reason the two can now be mixed in one list.

    It used to be the basename, and that made naming files instead of a
    directory a lossy choice: three labs each with an ``01_`` notebook arrived
    as three files in one flat folder with no way to tell which lab they came
    from. Naming individual files is what lets a course exclude one notebook
    from a folder it otherwise ships whole, so making that choice cost the
    student their folder structure was the wrong trade.

    An absolute path is the exception and keeps its basename. It names a
    location on the machine the command is running on, which is the laptop case,
    and a student folder called ``/Users/.../home/someone/checkout/...`` is not
    a thing anybody wants.
    """
    if os.path.isabs(entry):
        return os.path.basename(source)
    relative = os.path.normpath(entry).replace("\\", "/")
    # normpath collapses `a/../b` on its own. What it cannot do is stop a name
    # that climbs out of the directory the hooks were uploaded to, and such a
    # name would reach workspace/import as a student path with `..` in it. The
    # course's own config is not hostile input, but a typo here produces a
    # notebook at a path nobody can explain, so it is refused where it is
    # written rather than debugged where it lands.
    if relative == ".." or relative.startswith("../"):
        raise VoclabError(
            "MISSING_NOTEBOOK",
            single_line(
                f"{entry} climbs out of the directory the hooks were uploaded "
                f"to, so it cannot name a place in the student's folder. "
                f"{NOTEBOOKS_VAR} names content relative to /voc/scripts."
            ),
        )
    return relative


def notebook_sources(entries) -> list:
    """Expand the course's notebook list into (source, relative name) pairs.

    A file contributes itself under the name the course wrote, see
    :func:`notebook_relative_name`. A directory contributes everything
    importable under it, keeping the path relative to the directory, so a course
    that ships ``labs/Lab_1/setup.ipynb`` hands the student ``Lab_1/setup``
    rather than a folder's worth of files with no structure.

    A directory holding nothing importable fails rather than being skipped. A
    course that named it meant to ship something, and an empty result here is
    the one case where saying nothing produces a student with no content and a
    record that reports success.
    """
    pairs = []
    for entry in entries:
        source = notebook_source_path(entry)
        if os.path.isdir(source):
            relatives = notebook_relative_paths(source)
            if not relatives:
                raise VoclabError(
                    "MISSING_NOTEBOOK",
                    single_line(
                        f"{source} holds nothing importable, so the course named "
                        f"a directory that ships no content. Importable "
                        f"extensions: {', '.join(sorted(NOTEBOOK_FORMATS))}."
                    ),
                )
            pairs.extend(
                (os.path.join(source, relative), relative) for relative in relatives
            )
            continue
        pairs.append((source, notebook_relative_name(entry, source)))
    return pairs


def notebook_workspace_path(user_email: str, relative_name: str) -> str:
    """Name the student's copy.

    The extension comes off, unless it is in :data:`NOTEBOOK_KEEP_EXTENSIONS`. A
    Databricks notebook object does not carry one, and importing to a path that
    ends in ``.ipynb`` produces a notebook whose displayed name is
    ``lab_01.ipynb``, which reads to a student as a stray file rather than as
    the lab they were sent to. That constant carries the harder half of the
    reason: what happens when a kept extension is one Databricks strips for
    itself.

    Backslashes are folded to forward slashes. ``os.path.relpath`` uses the
    host's separator and a Databricks workspace path only ever uses ``/``, and
    while no hook runs on Windows this command is also run from a laptop.
    """
    name = relative_name.replace("\\", "/")
    stem, extension = os.path.splitext(name)
    kept = name if extension.lower() in NOTEBOOK_KEEP_EXTENSIONS else stem
    return f"{NOTEBOOK_USER_FOLDER}/{user_email}/{kept}"


def notebook_exists(workspace: Workspace, path: str) -> bool:
    """Whether the student already has something at this path."""
    query = urllib.parse.urlencode({"path": path})
    try:
        workspace.call("GET", f"/api/2.0/workspace/get-status?{query}", idempotent=True)
    except ApiError as error:
        if names_error_code(error, NOT_FOUND_ERROR_CODE):
            return False
        raise VoclabError("NOTEBOOK_STATUS_FAILED", single_line(str(error))) from error
    return True


def notebook_import_one(
    workspace: Workspace, source: str, path: str, fields: dict
) -> str:
    """Import one file, and report whether it was written or already there.

    Skipped when the path is taken, never overwritten. ``user_setup.sh`` runs
    again on every start, including the one after a ``stop``, and a ``stop``
    means the student's work is preserved for the session they are coming back
    to. Overwriting would hand them a blank copy of the notebook they spent the
    first session filling in, which is the same class of mistake ``lab_end.sh``
    reads ``$VOC_END_LAB_BEHAVIOR`` to avoid.

    ``/Users/<email>`` is not created here. Databricks creates it when the
    identity is added. Any folder below it is created by
    ``notebook_folders_ensure`` before this runs, because ``workspace/import``
    does not create missing parents.

    ``fields`` carries whatever the run has established so far, so a failure on
    the fourth notebook of six reaches the record still naming the three that
    landed.
    """
    if notebook_exists(workspace, path):
        log(f"{path} already exists, so the student's copy was left alone")
        return "kept"

    try:
        with open(source, "rb") as handle:
            content = base64.b64encode(handle.read()).decode("ascii")
    except OSError as error:
        raise VoclabError(
            "MISSING_NOTEBOOK",
            single_line(f"{source} could not be read: {error}"),
            fields,
        ) from error

    # With format JUPYTER the language comes out of the notebook's own metadata,
    # and stating it would be a second source for something the file answers. A
    # SOURCE import has no such metadata, so there the language is the only way
    # Databricks can know what it was handed. AUTO is told nothing on purpose:
    # reading the file is the whole point of asking for it.
    notebook_format, language = NOTEBOOK_FORMATS[os.path.splitext(source)[1].lower()]
    body = {
        "path": path,
        "format": notebook_format,
        "content": content,
        "overwrite": False,
    }
    if language:
        body["language"] = language

    try:
        # Retryable, unlike clusters/create, and ``overwrite`` false is the
        # whole reason. A retry that lands after a 5xx which had in fact
        # succeeded cannot produce a second copy; it produces
        # RESOURCE_ALREADY_EXISTS, which the branch below reads as the state
        # this was asked to reach.
        workspace.call("POST", "/api/2.0/workspace/import", body, idempotent=True)
    except ApiError as error:
        if names_error_code(error, ALREADY_EXISTS_ERROR_CODE):
            log(f"{path} appeared between the check and the write, so it stands")
            return "kept"
        raise VoclabError(
            "NOTEBOOK_IMPORT_FAILED", single_line(str(error)), fields
        ) from error

    log(f"imported {source} to {path}")
    return "imported"


def notebook_parent_folders(home: str, paths: list[str]) -> list[str]:
    """Name the folders that have to exist before any of ``paths`` can be written.

    ``home`` itself is excluded. Databricks creates ``/Users/<email>`` when it
    provisions the identity, so asking for it again is a call that buys nothing,
    and creating it when it is absent would manufacture a home directory for a
    user who does not exist rather than failing where the problem is.

    Only the immediate parent of each path is returned, because ``mkdirs``
    creates the whole chain, so a course shipping ``a/b/c.ipynb`` needs one call
    and not two. Order is the course's and duplicates are dropped, so a run's
    calls read in the order the notebooks were named.
    """
    folders: list[str] = []
    for path in paths:
        folder = path.rsplit("/", 1)[0]
        if folder == home or folder in folders:
            continue
        folders.append(folder)
    return folders


def notebook_folders_ensure(
    workspace: Workspace, folders: list[str], fields: dict
) -> None:
    """Create every folder the import is about to write into.

    **``POST /api/2.0/workspace/import`` does not create missing parents.**
    Measured, and it is the bug this function exists for. Top-level files never
    meet it, because their parent is the student's home folder and Databricks
    makes that itself. A directory tree fails with
    ``HTTP 404 RESOURCE_DOES_NOT_EXIST: The parent folder ... does not exist``.

    Before the loop rather than inside it, deliberately. A folder that cannot be
    created is a problem with the path or the caller, not with one notebook, and
    finding out first means the run either writes everything or writes nothing.
    Discovering it on file 2 leaves a student a half-imported workspace, which is
    the state that is hardest to tell from a working one later.

    ``mkdirs`` answers an existing folder with ``200`` and an empty body, so a
    resumed session pays two no-op calls rather than needing a check first.
    """
    for folder in folders:
        try:
            workspace.call(
                "POST", "/api/2.0/workspace/mkdirs", {"path": folder}, idempotent=True
            )
        except ApiError as error:
            raise VoclabError(
                "NOTEBOOK_FOLDER_FAILED",
                single_line(f"{folder} could not be created: {error}"),
                fields,
            ) from error
        log(f"folder {folder} is there")


def notebooks_import(workspace: Workspace, user_email: str, entries) -> dict:
    """Put the course's content into the student's workspace folder, once.

    Why this command exists at all. ``/voc/startercode`` was expected to reach
    the student's Databricks workspace and does not; the ``NOTEBOOKS_VAR``
    comment above says why.
    Nothing else puts a notebook in front of a student, so a hook has to.

    A course that names nothing is reported, not failed. Only ``user_setup.sh``
    is a required hook and a course whose content reaches the student some other
    way is a real course, so the absence has to be distinguishable in the record
    from an import that went wrong: ``notebook_action=none`` and
    ``notebook_count=0`` say which.

    ``notebook_path`` carries the first path imported rather than all of them,
    because it is what ``user_setup.sh`` builds the landing URL from and a
    student can only land on one page. Ordering is the course's: the first entry
    of the first name in ``VOC_COURSE_NOTEBOOKS``, so a course chooses where a
    student starts by putting it first.
    """
    fields = {}
    pairs = notebook_sources(entries)
    if not pairs:
        fields["notebook_count"] = "0"
        fields["notebook_action"] = "none"
        log("this course names no notebooks, so nothing was imported")
        return fields

    targets = [
        (source, notebook_workspace_path(user_email, relative))
        for source, relative in pairs
    ]
    fields["notebook_path"] = targets[0][1]
    fields["notebook_source"] = targets[0][0]

    # Every folder first, then every file. notebook_folders_ensure records why
    # the order matters and what it cost to find out.
    home = f"{NOTEBOOK_USER_FOLDER}/{user_email}"
    notebook_folders_ensure(
        workspace, notebook_parent_folders(home, [path for _, path in targets]), fields
    )

    actions = []
    for source, path in targets:
        actions.append(notebook_import_one(workspace, source, path, fields))
        fields["notebook_count"] = str(len(actions))

    fields["notebook_count"] = str(len(pairs))
    # One word for the whole run, because voccustomdata.txt has one field for
    # it. "imported" when anything was written, since that is the case a reader
    # is checking for; "kept" only when every path was already taken, which is
    # what a resumed session looks like.
    fields["notebook_action"] = "imported" if "imported" in actions else "kept"
    return fields


def normalize_host(value: str) -> str:
    """Reduce a workspace URL to scheme and host.

    ``VOC_DB_WORKSPACE_URL`` arrives carrying the ``?o=`` workspace parameter
    already, so appending an API path to it directly produces a URL with a query
    string in the middle. This is the third implementation of the same
    reduction, beside ``voc_origin`` in ``voclib.sh`` and ``normalize_host`` in
    ``src/dbx_vocareum_tools/databricks.py``, and it is duplicated rather than
    shared because this file has to stand alone in ``/voc/scripts``.
    """
    text = (value or "").strip()
    for separator in ("?", "#"):
        text = text.split(separator, 1)[0]
    return text.rstrip("/")


def build_workspace(args: argparse.Namespace) -> Workspace:
    """Resolve the credential, preferring what Vocareum hands the hook.

    ``VOC_DB_API_TOKEN`` first because that is the credential the lab actually
    runs under. ``DATABRICKS_HOST`` and ``DATABRICKS_TOKEN`` are honored second
    so that an operator can exercise this program from a laptop against a real
    workspace, which is the only way to test it end to end outside a live
    session.
    """
    host = normalize_host(
        args.host
        or os.environ.get("VOC_DB_WORKSPACE_URL")
        or os.environ.get("DATABRICKS_HOST")
        or ""
    )
    if not host:
        raise VoclabError(
            "MISSING_WORKSPACE_URL",
            "Neither VOC_DB_WORKSPACE_URL nor DATABRICKS_HOST is set, so there "
            "is no workspace to call.",
        )

    token = (
        args.token
        or os.environ.get("VOC_DB_API_TOKEN")
        or os.environ.get("DATABRICKS_TOKEN")
        or ""
    ).strip()
    if not token:
        raise VoclabError(
            "MISSING_API_TOKEN",
            "VOC_DB_API_TOKEN is not set, so this hook holds no Databricks "
            "credential and cannot provision anything. voc_detect_credential "
            "in voclib.sh records what the hook could reach.",
        )
    return Workspace(host, token)


def resolve_pool_name(args: argparse.Namespace) -> str:
    """Decide which pool name to look for.

    ``--pool-name`` first so a run can be aimed by hand, then ``VOC_POOL_NAME``
    which is how a course sets it, then the default. Read here rather than as an
    argparse default because the environment is read at call time everywhere
    else in this program and a default evaluated when the parser is built reads
    the environment at a different moment than the rest.
    """
    named = (args.pool_name or os.environ.get(POOL_NAME_VAR) or "").strip()
    return named or DEFAULT_POOL_NAME


def resolve_course_value(given, name: str, default: str = "") -> str:
    """Command line first, then the course, then whatever default there is.

    The order is the same one ``resolve_pool_name`` uses and it is the order
    that makes a hand-run reproduction possible: an operator debugging a course
    can aim one subcommand somewhere else without editing the ``course.env``
    that the failing run read.

    A blank flag counts as absent, matching ``course_value``. ``--catalog ''``
    on a command line means the same thing as an unfilled key in a file.
    """
    return (given or "").strip() or course_value(name, default)


def resolve_course_int(given, name: str, default: int) -> int:
    """The integer form of ``resolve_course_value``.

    ``given`` is already an ``int`` when argparse parsed one, so only the
    absent case falls through to the course. ``None`` rather than a number is
    what argparse leaves here, which is why the test is against ``None`` and
    not against truth: ``--auto-stop-minutes 0`` is a real answer.
    """
    if given is not None:
        return given
    return course_int(name, default)


def resolve_warehouse_name(args: argparse.Namespace) -> str:
    """Name the warehouse, or refuse to guess.

    No default anywhere. A warehouse is billable and this is the one object
    here that a course can go without entirely, so inventing a name for a
    course that never asked for one creates a bill nobody is expecting and
    nobody goes looking for.
    """
    name = resolve_course_value(args.warehouse_name, WAREHOUSE_NAME_VAR)
    if not name:
        raise VoclabError(
            "MISSING_WAREHOUSE_NAME",
            single_line(
                f"Neither --warehouse-name nor {WAREHOUSE_NAME_VAR} names a "
                "warehouse, and a billable object is not created under a name "
                "this program invented."
            ),
        )
    return name


def resolve_user(args: argparse.Namespace) -> str:
    """Decide whose cluster this is about."""
    user = (args.user or os.environ.get("VOC_DB_USER_EMAIL") or "").strip()
    if not user:
        raise VoclabError(
            "MISSING_USER_EMAIL",
            "Neither --user nor VOC_DB_USER_EMAIL names a student, and a "
            "per-student cluster cannot be named without one.",
        )
    return user


class ContractParser(argparse.ArgumentParser):
    """An argument parser that fails through the output contract.

    ``argparse`` answers a bad command line by printing usage to stderr and
    exiting 2, which is a non-zero exit with nothing on stdout: the shell would
    have no ``error_code`` to hand ``voc_fail``, and the lab would fail for no
    stated reason. The hooks carry a fallback code for exactly this, but a
    program that can name its own failure should.
    """

    def error(self, message: str) -> None:  # type: ignore[override]
        emit({"error_code": "BAD_ARGUMENTS", "message": message})
        log(f"FAILED BAD_ARGUMENTS: {message}")
        self.exit(EXIT_FAILED)


def build_parser() -> argparse.ArgumentParser:
    """Define the subcommands the hooks call.

    The three shared options are attached to each subcommand rather than to the
    top-level parser, so that ``cluster-ensure --user x`` works. Attaching them
    to both is the arrangement to avoid: a subparser writes its own defaults into
    the namespace, so a value given before the subcommand would be overwritten
    with ``None`` after it.

    No course value is an argparse default. An argparse default is evaluated
    when the parser is built, and every other environment read in this program
    happens when the subcommand runs; two reading moments in one program is a
    bug waiting for a test that sets a variable late. So every option below
    defaults to ``None`` and ``run`` resolves it against the course.
    """
    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--host",
        help="Workspace URL. Defaults to VOC_DB_WORKSPACE_URL.",
    )
    shared.add_argument(
        "--token",
        help="Workspace API token. Defaults to VOC_DB_API_TOKEN.",
    )
    shared.add_argument(
        "--user",
        help="The student's lab identity. Defaults to VOC_DB_USER_EMAIL.",
    )

    parser = ContractParser(
        prog="voclab.py",
        description=(
            "Provision and reclaim per-student Databricks clusters for the "
            "Vocareum lifecycle hooks."
        ),
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    ensure = subcommands.add_parser(
        "cluster-ensure",
        parents=[shared],
        help="Create the student's cluster, or start it if it is stopped.",
    )
    ensure.add_argument(
        "--node-type",
        help=(
            f"Node shape for the single node. Defaults to {NODE_TYPE_VAR}, "
            f"then to {DEFAULT_NODE_TYPE_ID!r}."
        ),
    )
    ensure.add_argument(
        "--spark-version",
        help=(
            f"Databricks runtime. Defaults to {SPARK_VERSION_VAR}, then to "
            f"{DEFAULT_SPARK_VERSION!r}, which is not an ML runtime."
        ),
    )
    ensure.add_argument(
        "--autotermination-minutes",
        type=int,
        help=(
            f"Idle minutes before the cluster stops. Defaults to "
            f"{AUTOTERMINATION_VAR}, then to {DEFAULT_AUTOTERMINATION_MINUTES}."
        ),
    )
    ensure.add_argument(
        "--catalog",
        help=(
            "Set as the cluster's initial catalog. Defaults to "
            f"{CATALOG_VAR}, and is omitted when neither names one."
        ),
    )
    ensure.add_argument(
        "--libraries",
        help=(
            f"Libraries to install, defaulting to {LIBRARIES_VAR}. One "
            "kind:value per line or per comma, kind one of "
            f"{', '.join(sorted(LIBRARY_KINDS))}. Empty installs nothing."
        ),
    )
    ensure.add_argument(
        "--pool-name",
        help=(
            f"Instance pool to take the node from. Defaults to {POOL_NAME_VAR}, "
            f"then to {DEFAULT_POOL_NAME!r}. No pool of that name means a node "
            "type is named instead, which is the unpooled behavior."
        ),
    )

    notebook = subcommands.add_parser(
        "notebook-import",
        parents=[shared],
        help="Copy the course's notebooks into the student's workspace folder.",
    )
    notebook.add_argument(
        "--notebooks",
        help=(
            f"What to import, defaulting to {NOTEBOOKS_VAR}. Names of files or "
            "of directories, comma or whitespace separated. A bare name is "
            "looked for beside this script, then in /voc/scripts, then in the "
            "working directory. A directory contributes every notebook under "
            "it. Naming nothing imports nothing, which is not a failure."
        ),
    )

    warehouse = subcommands.add_parser(
        "warehouse-ensure",
        parents=[shared],
        help="Create the course's SQL warehouse, or start it if it is stopped.",
    )
    warehouse.add_argument(
        "--warehouse-name",
        help=(
            f"Name, which is the idempotency key. Defaults to "
            f"{WAREHOUSE_NAME_VAR}. There is no default beyond that."
        ),
    )
    warehouse.add_argument(
        "--warehouse-size",
        help=(
            f"Cluster size. Defaults to {WAREHOUSE_SIZE_VAR}, then to "
            f"{DEFAULT_WAREHOUSE_SIZE!r}."
        ),
    )
    warehouse.add_argument(
        "--auto-stop-minutes",
        type=int,
        help=(
            f"Idle minutes before the warehouse stops. Defaults to "
            f"{WAREHOUSE_AUTO_STOP_VAR}, then to "
            f"{DEFAULT_WAREHOUSE_AUTO_STOP_MINUTES}."
        ),
    )

    starter = subcommands.add_parser(
        "starter-warehouse",
        parents=[shared],
        help="Resize the warehouse Databricks created with the workspace.",
    )
    starter.add_argument(
        "--warehouse-size",
        help=(
            f"Size to put it on. Defaults to {STARTER_WAREHOUSE_SIZE_VAR}. "
            "Naming no size leaves the warehouse alone, which is not a failure."
        ),
    )
    starter.add_argument(
        "--warehouse-name",
        help=(
            "Name to look for, for a workspace where Databricks has renamed it. "
            f"Defaults to {STARTER_WAREHOUSE_NAME_VAR}, then to each of "
            f"{', '.join(STARTER_WAREHOUSE_NAMES)} in turn."
        ),
    )

    subcommands.add_parser(
        "cluster-stop",
        parents=[shared],
        help="Terminate the cluster but keep its configuration, for a stop.",
    )
    subcommands.add_parser(
        "cluster-remove",
        parents=[shared],
        help="Permanently delete the cluster, for a terminate.",
    )
    return parser


def run(args: argparse.Namespace) -> dict:
    """Dispatch one subcommand and return the fields it established.

    ``warehouse-ensure`` and ``starter-warehouse`` are handled before the
    student is resolved, and that is the only reason they are not read with the
    rest. Both are one object for the whole workspace, so both are called from
    ``workspace_init.sh``, which runs before any student exists and therefore
    with no ``VOC_DB_USER_EMAIL`` in its environment. Asking for one there would
    fail the hook over a value nothing was going to use.
    """
    workspace = build_workspace(args)

    if args.command == "starter-warehouse":
        # No size named means leave it alone, and say so. Unlike every other
        # object here this one exists whether or not the course wants it, so
        # the choice is between editing something nobody mentioned and doing
        # nothing; a course that never set the key gets the second. The field
        # is still written, because a hook that records nothing on the quiet
        # path leaves a reader unable to tell it from a hook that never ran.
        size = resolve_course_value(args.warehouse_size, STARTER_WAREHOUSE_SIZE_VAR)
        if not size:
            log(
                f"{STARTER_WAREHOUSE_SIZE_VAR} names no size, so the starter "
                "warehouse was left as Databricks made it"
            )
            return {
                "starter_warehouse_action": "skipped",
                "starter_warehouse_size": "none",
                "starter_warehouse_name": "none",
            }
        named = resolve_course_value(args.warehouse_name, STARTER_WAREHOUSE_NAME_VAR)
        return starter_warehouse_resize(
            workspace,
            size=size,
            names=(named,) if named else STARTER_WAREHOUSE_NAMES,
        )

    if args.command == "warehouse-ensure":
        return warehouse_ensure(
            workspace,
            warehouse_name=resolve_warehouse_name(args),
            size=resolve_course_value(
                args.warehouse_size, WAREHOUSE_SIZE_VAR, DEFAULT_WAREHOUSE_SIZE
            ),
            auto_stop_minutes=resolve_course_int(
                args.auto_stop_minutes,
                WAREHOUSE_AUTO_STOP_VAR,
                DEFAULT_WAREHOUSE_AUTO_STOP_MINUTES,
            ),
            group_name=os.environ.get("VOC_DB_GROUP_NAME", "").strip(),
        )

    user_email = resolve_user(args)

    if args.command == "cluster-ensure":
        return cluster_ensure(
            workspace,
            user_email=user_email,
            node_type_id=resolve_course_value(
                args.node_type, NODE_TYPE_VAR, DEFAULT_NODE_TYPE_ID
            ),
            spark_version=resolve_course_value(
                args.spark_version, SPARK_VERSION_VAR, DEFAULT_SPARK_VERSION
            ),
            autotermination_minutes=resolve_course_int(
                args.autotermination_minutes,
                AUTOTERMINATION_VAR,
                DEFAULT_AUTOTERMINATION_MINUTES,
            ),
            catalog=resolve_course_value(args.catalog, CATALOG_VAR) or None,
            tags=parse_resource_tags(os.environ.get("VOC_RESOURCE_TAGS", "")),
            pool_name=resolve_pool_name(args),
            libraries=parse_libraries(
                resolve_course_value(args.libraries, LIBRARIES_VAR)
            ),
        )
    if args.command == "notebook-import":
        return notebooks_import(
            workspace,
            user_email,
            course_notebook_entries(
                resolve_course_value(args.notebooks, NOTEBOOKS_VAR)
            ),
        )
    if args.command == "cluster-stop":
        return cluster_release(workspace, user_email, permanent=False)
    if args.command == "cluster-remove":
        return cluster_release(workspace, user_email, permanent=True)
    raise VoclabError("BAD_ARGUMENTS", f"Unknown command {args.command!r}.")


def main(argv=None) -> int:
    """Run one subcommand, and make sure every ending says something.

    The broad ``except`` is the point of this function rather than a lapse in
    it. An unhandled traceback here would exit non-zero having written nothing
    to stdout, and the shell would have no ``error_code`` to hand ``voc_fail``,
    which is the exact failure mode this program was written to remove. So a
    traceback goes to stderr for whoever is watching the console, and
    ``UNEXPECTED_ERROR`` goes to stdout for the session record.
    """
    args = build_parser().parse_args(argv)
    try:
        emit(run(args))
    except VoclabError as error:
        fields = dict(error.fields)
        fields["error_code"] = error.code
        fields["message"] = error.message
        emit(fields)
        log(f"FAILED {error.code}: {error.message}")
        return EXIT_FAILED
    # Broad on purpose; see the docstring above for why.
    except Exception as error:
        traceback.print_exc()
        emit(
            {
                "error_code": "UNEXPECTED_ERROR",
                "message": f"{type(error).__name__}: {error}",
            }
        )
        return EXIT_FAILED
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
