"""Instance-pool CLI tests.

Two things are worth testing offline here, and they are not the HTTP calls.

The first is the create body. Every field in it exists to override a platform
default that costs money or costs a lab: ``ON_DEMAND`` because a default pool is
``SPOT`` and a single-node cluster off a spot box can be reclaimed mid-lab, and
``max_capacity`` because the default is no ceiling at all. A body that quietly
stopped sending either would still create a working pool, which is exactly why
it needs a test rather than a review.

The second is drift between the two halves of the repository.
``src/dbx_vocareum_tools/pool.py`` creates the pool and ``labruntime/voclab.py`` finds
it, and they cannot import each other: ``voclab.py`` is uploaded to
``/voc/scripts`` and runs there with the standard library and nothing else. So
the pool name, the node type and the runtime are written twice, and a
disagreement between the two copies is a pool that exists in a workspace and
that no cluster ever finds.
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

from dbx_vocareum_tools import pool

VOCLAB_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dbx_vocareum_tools"
    / "labruntime"
    / "voclab.py"
)


def load_voclab():
    """Import voclab.py by path. See tests/test_voclab.py for why."""
    original = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec = importlib.util.spec_from_file_location("voclab_for_pool", VOCLAB_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.dont_write_bytecode = original


voclab = load_voclab()


def body(**overrides) -> dict:
    arguments = {
        "pool_name": pool.DEFAULT_POOL_NAME,
        "node_type_id": pool.DEFAULT_NODE_TYPE_ID,
        "spark_version": pool.DEFAULT_SPARK_VERSION,
        "min_idle": pool.DEFAULT_MIN_IDLE,
        "max_capacity": pool.DEFAULT_MAX_CAPACITY,
        "idle_minutes": pool.DEFAULT_IDLE_MINUTES,
    }
    arguments.update(overrides)
    return pool.create_body(**arguments)


# --- the defaults that are not defaults --------------------------------------


def test_the_pool_is_created_on_demand_rather_than_on_spot():
    # Measured: a pool created without aws_attributes reads SPOT with a bid of
    # 100%, and a cluster off it reports first_on_demand 0. On a single-node
    # cluster the driver is the whole cluster, so that is a lab a spot reclaim
    # can end halfway through.
    assert body()["aws_attributes"] == {"availability": "ON_DEMAND"}


def test_a_ceiling_is_always_sent():
    # Absent means no ceiling, which is what turns one mistake into an
    # open-ended bill.
    assert body()["max_capacity"] == pool.DEFAULT_MAX_CAPACITY
    assert body(max_capacity=3)["max_capacity"] == 3


def test_nothing_is_launched_unless_idle_boxes_are_asked_for():
    assert body()["min_idle_instances"] == 0
    assert body(min_idle=5)["min_idle_instances"] == 5


def test_the_runtime_is_preloaded_as_a_list():
    assert body()["preloaded_spark_versions"] == [pool.DEFAULT_SPARK_VERSION]


# --- the name, which is the join key -----------------------------------------


def test_the_pool_name_prefers_the_flag_then_the_environment():
    assert pool.pool_name_from("", None) == pool.DEFAULT_POOL_NAME
    assert pool.pool_name_from("course-b", None) == "course-b"
    assert pool.pool_name_from("course-b", "course-c") == "course-c"
    assert pool.pool_name_from("  ", None) == pool.DEFAULT_POOL_NAME


def test_the_two_halves_look_for_the_same_pool():
    # The tool creates it and the hook finds it, by name, without either being
    # able to import the other.
    assert pool.DEFAULT_POOL_NAME == voclab.DEFAULT_POOL_NAME
    assert pool.POOL_NAME_VAR == voclab.POOL_NAME_VAR


def test_the_pool_boots_the_node_type_and_runtime_the_hooks_pin():
    # A pool preloading a runtime the hooks do not ask for is accepted by
    # Databricks, boots fine, and saves nobody any time.
    assert pool.DEFAULT_NODE_TYPE_ID == voclab.DEFAULT_NODE_TYPE_ID
    assert pool.DEFAULT_SPARK_VERSION == voclab.DEFAULT_SPARK_VERSION


def test_the_active_state_the_two_halves_check_for_is_the_same_word():
    assert pool.ACTIVE_STATE == voclab.ACTIVE_POOL_STATE


# --- the CLI, on the paths that make no calls --------------------------------


def test_create_and_delete_together_are_refused_before_any_credential_is_read():
    assert pool.main(["--create", "--delete"]) == pool.EXIT_CONFIG


def test_a_delete_with_nothing_attached_to_answer_is_refused():
    # confirm_delete reads stdin, and pytest gives it a stream that is not a
    # tty, which is the same case as a cron job or a CI run.
    assert pool.confirm_delete("workshop-pool", "https://example.cloud") is False


def test_status_is_the_default_action():
    args = pool.parse_args([])
    assert args.create is False
    assert args.delete is False
    assert isinstance(args, argparse.Namespace)
