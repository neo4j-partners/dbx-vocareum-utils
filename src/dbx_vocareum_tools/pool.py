"""Create, read and delete the instance pool a workshop's clusters come off.

An instance pool keeps booted EC2 boxes on standby, so a cluster that names one
takes a ready box instead of waiting for AWS. Measured: a student waits about
16 minutes for a usable cluster, 10 minutes for the box and another 6 for the
course's libraries to install. A pool attacks the
first number and does nothing at all to the second. ``docs/pre-warm.md`` is the
account of that measurement and of why the obvious alternative, warming a
cluster from ``lab_setup.sh``, cannot work.

Why this is a tool rather than a hook. A pool needs no student identity, which
makes ``workspace_init.sh`` the right place for it architecturally, and that
hook has never been observed running, which makes it the wrong place to trust.
A pool created here is one an instructor can check before a class starts.

Why the pool is created by a tool rather than through the console, which is the
part worth not skipping. A pool created with defaults reports ``availability:
SPOT`` with ``spot_bid_price_percent`` 100, and a cluster off it comes back with
``first_on_demand`` 0. The cluster body in ``labruntime/voclab.py`` sets
``first_on_demand`` 1 deliberately, to keep the driver off the spot market, and
on a single-node cluster the driver is the entire cluster. Pooling moves that
decision from the cluster to the pool, so a pool made by hand with defaults
hands every student a cluster a spot reclaim can kill mid-lab. ``--create``
sends ``ON_DEMAND`` for that reason.

The other default worth overriding is ``max_capacity``, which is absent unless
asked for, and absent means no ceiling. It is the difference between one mistake
costing an afternoon and one mistake costing an unbounded amount, so it is
always sent.

Three things this shares with ``dbx-vocareum-probe`` rather than reimplementing:
the OAuth machine-to-machine exchange, the credential guard, and the workspace
guard. The workspace guard is the one that matters here. A pool lives in exactly
one workspace, Vocareum creates a workspace per part, and a pool created against
the wrong host bills in a place no student ever enters while reporting success.

Usage::

    uv run dbx-vocareum-pool                                 # status, the default
    uv run dbx-vocareum-pool --create --min-idle 5
    uv run dbx-vocareum-pool --delete --yes

Exit codes:
    0  The operation ran.
    1  Configuration error.
    2  A guard refused, or the API did.
    3  A confirmation was declined.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from requests.exceptions import RequestException

from .config import ConfigError, Environment, repository_root
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

LIST_PATH = "/api/2.0/instance-pools/list"
GET_PATH = "/api/2.0/instance-pools/get"
CREATE_PATH = "/api/2.0/instance-pools/create"
DELETE_PATH = "/api/2.0/instance-pools/delete"

# These four have to agree with ``labruntime/voclab.py``, which cannot import
# them: that file is uploaded to /voc/scripts and runs there with the standard
# library and nothing else. The name is the join key between the two halves, so a
# mismatch here is a pool that exists and that no cluster ever finds. The node
# type and the runtime are duplicated for the same reason and carry their
# reasoning there, in ``DEFAULT_NODE_TYPE_ID`` and ``DEFAULT_SPARK_VERSION``.
#
# The runtime is the one to watch, because a mismatch here does not fail. A pool
# preloading a runtime no cluster asks for accepts the cluster anyway, reports
# ACTIVE, and saves the student nothing: it hands out a booted box that then
# downloads the runtime it was supposed to already have. ``pool_preload_verdict``
# in voclab.py is what writes that case down. A course that sets
# VOC_COURSE_SPARK_VERSION has to pass the same value to ``--spark-version``
# here, and the default below is the one a course that says nothing gets.
DEFAULT_POOL_NAME = "workshop-pool"
POOL_NAME_VAR = "VOC_POOL_NAME"
DEFAULT_NODE_TYPE_ID = "i3.xlarge"
DEFAULT_SPARK_VERSION = "17.3.x-scala2.13"

# Idle boxes are real AWS money from create to delete, so the default is none of
# them: a pool created without --min-idle costs nothing and still preloads the
# runtime for boxes it launches on demand. An instructor raises it before a
# class and the runbook in docs/pre-warm.md says so.
DEFAULT_MIN_IDLE = 0

# Measured accepted and read back. 25 is a class of 20 with room
# for the instructor and a restart, and its job is to be a ceiling rather than a
# capacity plan.
DEFAULT_MAX_CAPACITY = 25

# How long a box waits, after the cluster using it goes away, before it is
# returned to AWS. The platform default is 60. 30 is chosen because the thing
# being bought is the next student's 10 minutes, and a box that sits idle for an
# hour after a lab ends is buying nobody anything.
DEFAULT_IDLE_MINUTES = 30

ACTIVE_STATE = "ACTIVE"

# What a status read shows, in the order it is worth reading: what the pool is,
# then what it costs, then whether it will actually save anyone time.
STATUS_FIELDS = (
    "instance_pool_id",
    "state",
    "node_type_id",
    "min_idle_instances",
    "max_capacity",
    "idle_instance_autotermination_minutes",
    "preloaded_spark_versions",
    "aws_attributes",
)

# The counts a pool keeps, which are the answer to "is anything running". Read
# from instance-pools/get rather than from the listing, which does not carry
# them.
STAT_FIELDS = (
    "idle_count",
    "used_count",
    "pending_idle_count",
    "pending_used_count",
)


def pool_name_from(environment_value: str, override: str | None) -> str:
    """Decide which pool this run is about.

    The same order ``resolve_pool_name`` in ``labruntime/voclab.py`` uses, because the
    tool and the hook have to look for the same name or the pool serves nobody.
    """
    named = (override or environment_value or "").strip()
    return named or DEFAULT_POOL_NAME


def find_pool(client: WorkspaceClient, pool_name: str) -> dict[str, Any] | None:
    """Return the pool with this name, or ``None``.

    By name rather than by ID for the reason the hooks do the same: an
    ``instance_pool_id`` means nothing outside the workspace that issued it, and
    the workspace here is one Vocareum created for a part.
    """
    payload = client.api_client.do("GET", LIST_PATH)
    if not isinstance(payload, dict):
        return None
    for pool in payload.get("instance_pools") or []:
        if isinstance(pool, dict) and pool.get("instance_pool_name") == pool_name:
            return pool
    return None


def read_pool(client: WorkspaceClient, pool_id: str) -> dict[str, Any]:
    """Read one pool in full, which is where the idle and used counts live."""
    payload = client.api_client.do("GET", GET_PATH, query={"instance_pool_id": pool_id})
    return payload if isinstance(payload, dict) else {}


def create_body(
    pool_name: str,
    node_type_id: str,
    spark_version: str,
    min_idle: int,
    max_capacity: int,
    idle_minutes: int,
) -> dict[str, Any]:
    """Build the ``instance-pools/create`` body.

    Every field here is sent rather than left to its default, and two of them
    are the reason this function exists. ``aws_attributes`` because the default
    is ``SPOT`` and a single-node cluster off a spot box is a lab that can die
    halfway through. ``max_capacity`` because the default is no ceiling.

    ``preloaded_spark_versions`` is what a pool can do for a runtime, and it is
    also the field a cluster can silently disagree with: measured, a cluster
    asking for a version the pool did not preload is accepted, boots, and saves
    nothing. Keeping this in step with the hooks' pinned version is the whole
    job, which is why both default to the same constant.
    """
    return {
        "instance_pool_name": pool_name,
        "node_type_id": node_type_id,
        "min_idle_instances": min_idle,
        "max_capacity": max_capacity,
        "idle_instance_autotermination_minutes": idle_minutes,
        "preloaded_spark_versions": [spark_version],
        "aws_attributes": {"availability": "ON_DEMAND"},
    }


def report_pool(pool: dict[str, Any], spark_version: str | None) -> None:
    """Print what the pool is, what it costs, and whether it will save time.

    ``spark_version`` is None when the caller did not name one, and that is a
    different question from naming one that does not match. This tool runs on a
    laptop against a workspace; the runtime the hooks actually ask for lives in
    the course's ``course.env``, in the course's own repository, which nothing
    here can see. A bare status read that substituted this package's default
    would report a correct ML-runtime pool as not preloading the default, and
    the remedy it printed would be to delete a working pool. So an unnamed
    version buys silence on the verdict rather than a verdict against a guess.
    """
    for name in STATUS_FIELDS:
        value = pool.get(name)
        if value is None:
            print(f"  {name:38s} not present")
            continue
        print(f"  {name:38s} {value}")

    stats = pool.get("stats")
    if isinstance(stats, dict):
        rule()
        for name in STAT_FIELDS:
            print(f"  {name:38s} {stats.get(name, 'not present')}")

    rule()
    preloaded = pool.get("preloaded_spark_versions") or []
    if spark_version is None:
        print(f"  This pool preloaded {preloaded or 'nothing'}.")
        print(
            "  Whether that is what the hooks ask for is not decided here. Pass\n"
            "  --spark-version with the course's VOC_COURSE_SPARK_VERSION to have\n"
            "  it checked."
        )
        return
    if spark_version in preloaded:
        print(f"  This pool preloaded {spark_version}, which is what the hooks ask")
        print("  for. A cluster off it skips the runtime download.")
        return
    print(f"  This pool did NOT preload {spark_version}.")
    print(
        "  A cluster asking for it is still accepted and still boots, and it\n"
        "  waits for the runtime while the pool bills for idle boxes. Nothing\n"
        "  in Databricks reports this, so it is reported here. Delete the pool\n"
        "  and create it again with --spark-version matching the hooks."
    )


def confirm_delete(pool_name: str, host: str) -> bool:
    """Ask for a y/n before deleting the pool.

    The prompt names the pool and the host, so the answer is still about a
    specific pool rather than a generic yes. Deleting a pool does not delete
    the clusters attached to it; it takes away the boxes they would restart
    onto, so a class in progress starts failing to start clusters.
    """
    print(f"About to delete instance pool {pool_name!r} in {host}.")
    print(
        "Clusters already running are unaffected. Clusters that try to start\n"
        "afterwards will not find the pool, and the hooks fall back to naming a\n"
        "node type, so a class in progress keeps working and stops being warm."
    )
    if not sys.stdin.isatty():
        print("Refusing: nothing is attached to answer, and --yes was not passed.")
        return False
    answer = input(f"Delete {pool_name}? [y/N]: ").strip().lower()
    if answer not in ("y", "yes"):
        print("Not confirmed. Nothing was deleted.")
        return False
    return True


def show_status(
    client: WorkspaceClient, pool_name: str, spark_version: str | None
) -> int:
    """Report whether the pool exists and what it is doing."""
    section(f"POOL {pool_name}")
    pool = find_pool(client, pool_name)
    if pool is None:
        print(f"  No instance pool named {pool_name!r} in this workspace.")
        print(
            "\n  That is not an error. The lifecycle hooks fall back to naming a\n"
            "  node type and record pool=absent, so the class runs unpooled and\n"
            "  a student waits the full time for a box."
        )
        return EXIT_OK

    pool_id = str(pool.get("instance_pool_id") or "")
    full = read_pool(client, pool_id) if pool_id else pool
    report_pool(full or pool, spark_version)
    state = str(full.get("state") or pool.get("state") or "")
    if state != ACTIVE_STATE:
        print(
            f"\n  This pool reads {state!r} rather than {ACTIVE_STATE}. The hooks\n"
            "  treat anything else as absent and record pool=inactive."
        )
    return EXIT_OK


def create_pool(
    client: WorkspaceClient, pool_name: str, args: argparse.Namespace
) -> int:
    """Create the pool, or report the one that is already there.

    Existing is not an error and is not overwritten. Two pools with one name is
    the state that breaks the name-as-join-key arrangement the hooks rely on, and
    an update in place would silently change what a running class is sitting on.
    """
    section(f"CREATE POOL {pool_name}")
    existing = find_pool(client, pool_name)
    if existing is not None:
        print(f"  A pool named {pool_name!r} already exists. Nothing was created.")
        print(f"  instance_pool_id {existing.get('instance_pool_id')}")
        print(
            "\n  Creating a second pool with the same name would break the way\n"
            "  the hooks find it, which is by name. Delete this one first if it\n"
            "  is configured wrongly."
        )
        return EXIT_OK

    body = create_body(
        pool_name=pool_name,
        node_type_id=args.node_type,
        # The default lives here rather than on the flag, because the flag is
        # read by the status path too and a default there is a guess presented
        # as the course's answer. Creating has to preload something, so a course
        # that says nothing gets the package's runtime and the printed body says
        # which one went up.
        spark_version=args.spark_version or DEFAULT_SPARK_VERSION,
        min_idle=args.min_idle,
        max_capacity=args.max_capacity,
        idle_minutes=args.idle_minutes,
    )
    for key, value in body.items():
        print(f"  {key:38s} {value}")
    rule()
    response = client.api_client.do("POST", CREATE_PATH, body=body)
    pool_id = ""
    if isinstance(response, dict):
        pool_id = str(response.get("instance_pool_id") or "")
    print(f"  Created. instance_pool_id {pool_id or 'not returned'}")
    if args.min_idle > 0:
        print(
            f"\n  {args.min_idle} idle boxes start now and bill until this pool is\n"
            "  deleted. Nothing deletes it on its own."
        )
    return EXIT_OK


def delete_pool(
    client: WorkspaceClient, pool_name: str, host: str, assume_yes: bool
) -> int:
    """Delete the pool, after saying which one and where."""
    section(f"DELETE POOL {pool_name}")
    pool = find_pool(client, pool_name)
    if pool is None:
        print(f"  No pool named {pool_name!r} in this workspace. Nothing to delete.")
        return EXIT_OK

    pool_id = str(pool.get("instance_pool_id") or "")
    print(f"  instance_pool_id {pool_id}")
    rule()
    if not assume_yes and not confirm_delete(pool_name, host):
        return EXIT_REFUSED

    client.api_client.do("POST", DELETE_PATH, body={"instance_pool_id": pool_id})
    print("  Deleted. Idle boxes stop billing as they are returned to AWS.")
    return EXIT_OK


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read, create or delete the instance pool the workshop's clusters "
            "take their nodes from."
        )
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="create the pool. Costs EC2 from this moment when --min-idle is set.",
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="delete the pool. Without this and --create, the tool only reads.",
    )
    parser.add_argument(
        "--pool-name",
        help=(
            f"the pool to act on. Defaults to {POOL_NAME_VAR}, then to "
            f"{DEFAULT_POOL_NAME!r}, which is what the hooks look for."
        ),
    )
    parser.add_argument(
        "--min-idle",
        type=int,
        default=DEFAULT_MIN_IDLE,
        help=(
            f"boxes kept booted and waiting. Default {DEFAULT_MIN_IDLE}, which "
            "launches nothing and costs nothing."
        ),
    )
    parser.add_argument(
        "--max-capacity",
        type=int,
        default=DEFAULT_MAX_CAPACITY,
        help=(
            f"ceiling on boxes, idle and in use. Default {DEFAULT_MAX_CAPACITY}. "
            "The platform default is no ceiling at all."
        ),
    )
    parser.add_argument(
        "--idle-minutes",
        type=int,
        default=DEFAULT_IDLE_MINUTES,
        help=(
            "how long a released box waits before going back to AWS. Default "
            f"{DEFAULT_IDLE_MINUTES}; the platform default is 60."
        ),
    )
    parser.add_argument(
        "--node-type",
        default=DEFAULT_NODE_TYPE_ID,
        help=f"node type the pool boots. Default {DEFAULT_NODE_TYPE_ID}.",
    )
    parser.add_argument(
        "--spark-version",
        default=None,
        help=(
            "runtime to preload. Must match what the hooks ask for or the pool "
            f"saves nothing. Creating without it preloads {DEFAULT_SPARK_VERSION}. "
            "A status read without it lists the preloads and passes no verdict, "
            "because this tool cannot see the course's VOC_COURSE_SPARK_VERSION."
        ),
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="skip the delete confirmation. For scripted runs.",
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

    if args.create and args.delete:
        print(
            "--create and --delete together name two different intentions. "
            "Run one, then the other.",
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

    # The pool name is read through the same layering the rest of the toolkit
    # uses: a real environment variable wins over .env, and the flag wins over
    # both. Read from the environment file rather than os.environ directly so
    # that --env-file means the same thing here as everywhere else.
    environment = Environment.from_file(args.env_file.expanduser())
    pool_name = pool_name_from(environment.get(POOL_NAME_VAR), args.pool_name)

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
        if args.create:
            return create_pool(client, pool_name, args)
        if args.delete:
            return delete_pool(client, pool_name, config.host, args.yes)
        return show_status(client, pool_name, args.spark_version)
    except (DatabricksError, RequestException) as error:
        print(f"\n{error}", file=sys.stderr)
        return EXIT_API


if __name__ == "__main__":
    raise SystemExit(main())
