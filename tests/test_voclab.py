"""Cluster provisioning tests.

``voclab.py`` ships inside the package as data rather than as a module. It is
uploaded to ``/voc/scripts`` and run there by a shell hook, which is exactly the
arrangement that makes the reference integration's equivalent untestable: 1500
lines that can only be exercised by starting a lab. So it is loaded from its path
here and given a fake ``urlopen``, which is the only dependency it has.

What these tests are for is the branch that decides whether a cluster gets
created, started, or left alone. That decision is the idempotency the whole
design rests on, because ``lab_setup.sh`` and ``user_setup.sh`` both make the
same call and neither may undo the other's work.
"""

from __future__ import annotations

import base64
import importlib.util
import io
import json
import sys
import urllib.error
import urllib.parse
from pathlib import Path

import pytest

VOCLAB_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "dbx_vocareum_tools"
    / "labruntime"
    / "voclab.py"
)


def load_voclab():
    """Import voclab.py by path rather than as a module of the package.

    It ships inside ``dbx_vocareum_tools`` as data and is uploaded to
    ``/voc/scripts``, where it runs under whatever Python 3.9 the Vocareum host
    carries. Importing it here as ``dbx_vocareum_tools.labruntime.voclab`` would
    let it acquire a package-relative import without any test noticing, and that
    import fails only on the host, in front of a student.

    Bytecode writing is suppressed for the same reason and not as a tidiness
    preference: a ``__pycache__`` beside a file that is copied verbatim into a
    lab is a file nobody meant to ship.
    """
    original = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec = importlib.util.spec_from_file_location("voclab", VOCLAB_PATH)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.dont_write_bytecode = original


voclab = load_voclab()

USER = "student1_200000@vocareum.com"
CLUSTER = "student1_200000"
HOST = "https://dbc-22222222-2222.cloud.databricks.com"

# A course's library list, stated here rather than read from voclab.py. There is
# no list in that file any more: the libraries belong to a course and arrive
# through VOC_COURSE_LIBRARIES. Two of them, one pypi and one maven, because the
# maven form is the one whose value carries its own colons.
COURSE_LIBRARIES_RAW = (
    "pypi:neo4j==6.2.0, "
    "maven:org.neo4j:neo4j-connector-apache-spark_2.13:5.4.3_for_spark_3"
)
LIBRARIES = [
    {"pypi": {"package": "neo4j==6.2.0"}},
    {
        "maven": {
            "coordinates": (
                "org.neo4j:neo4j-connector-apache-spark_2.13:5.4.3_for_spark_3"
            )
        }
    },
]


class FakeResponse:
    """The part of an ``http.client.HTTPResponse`` that ``call`` touches."""

    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *args) -> bool:
        return False


class FakeHttp:
    """A queue of canned answers, keyed by the path each request asks for.

    Recording the requests is half the point: several of the assertions below are
    about a call *not* having been made, which is what distinguishes a cluster
    left alone from one restarted needlessly.
    """

    def __init__(self, answers: dict) -> None:
        self.answers = answers
        self.calls = []

    def __call__(self, request, timeout=None):
        path = request.full_url[len(HOST) :]
        body = None
        if request.data:
            body = json.loads(request.data.decode("utf-8"))
        self.calls.append((request.method, path, body))

        answer = self.answers.get(path, {})
        if isinstance(answer, list):
            answer = answer.pop(0) if answer else {}
        if isinstance(answer, Exception):
            raise answer
        return FakeResponse(json.dumps(answer))

    def paths(self, method: str | None = None) -> list:
        return [
            path for verb, path, _ in self.calls if method is None or verb == method
        ]

    def body_for(self, path: str) -> dict:
        for _, seen, body in self.calls:
            if seen == path:
                return body
        raise AssertionError(f"no call was made to {path}")


def http_error(status: int, payload: dict | None = None) -> urllib.error.HTTPError:
    """A Databricks-shaped HTTP failure."""
    body = json.dumps(payload or {}).encode("utf-8")
    return urllib.error.HTTPError(f"{HOST}/api", status, "error", {}, io.BytesIO(body))


def workspace(answers: dict) -> tuple:
    http = FakeHttp(answers)
    return voclab.Workspace(HOST, "dapi-test-token", opener=http), http


def listing(*clusters) -> dict:
    return {"clusters": list(clusters)}


SPARK_VERSIONS_PATH = "/api/2.0/clusters/spark-versions"
NODE_TYPES_PATH = "/api/2.0/clusters/list-node-types"
POOLS_PATH = "/api/2.0/instance-pools/list"


def spark_versions(*keys) -> dict:
    return {"versions": [{"key": key, "name": key} for key in keys]}


def node_types(*ids) -> dict:
    return {"node_types": [{"node_type_id": node_id} for node_id in ids]}


def pool(
    name: str = voclab.DEFAULT_POOL_NAME,
    pool_id: str = "1234-pool",
    state: str = "ACTIVE",
    preloaded=(voclab.DEFAULT_SPARK_VERSION,),
) -> dict:
    return {
        "instance_pool_name": name,
        "instance_pool_id": pool_id,
        "state": state,
        "preloaded_spark_versions": list(preloaded),
    }


def pools(*entries) -> dict:
    return {"instance_pools": list(entries)}


def ensure(workspace_client, **overrides) -> dict:
    """Call cluster_ensure with the defaults the hooks use."""
    arguments = {
        "user_email": USER,
        "node_type_id": voclab.DEFAULT_NODE_TYPE_ID,
        "spark_version": voclab.DEFAULT_SPARK_VERSION,
        "autotermination_minutes": voclab.DEFAULT_AUTOTERMINATION_MINUTES,
        # Passed rather than left to default, because the default reads the
        # environment and a test that depended on VOC_COURSE_LIBRARIES would
        # pass or fail according to the shell it was run from.
        "libraries": LIBRARIES,
    }
    arguments.update(overrides)
    return voclab.cluster_ensure(workspace_client, **arguments)


# --- naming, which is the idempotency key ------------------------------------


def test_cluster_name_is_the_local_part_of_the_lab_identity():
    assert voclab.cluster_name_for(USER) == CLUSTER


def test_cluster_name_replaces_characters_a_cluster_name_may_not_hold():
    assert voclab.cluster_name_for("first.last+lab@vocareum.com") == "first_last_lab"


def test_cluster_name_is_truncated_rather_than_rejected():
    long_user = "x" * 200 + "@vocareum.com"
    name = voclab.cluster_name_for(long_user)
    assert len(name) == voclab.CLUSTER_NAME_MAX_LENGTH


def test_cluster_name_refuses_an_identity_it_cannot_name():
    with pytest.raises(voclab.VoclabError) as failure:
        voclab.cluster_name_for("@vocareum.com")
    assert failure.value.code == "BAD_USER_EMAIL"


# --- the create branch -------------------------------------------------------


def test_absent_cluster_is_created_with_the_workshop_configuration():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "created"
    assert fields["cluster_name"] == CLUSTER
    assert fields["cluster_id"] == "0101-abc"

    body = http.body_for("/api/2.0/clusters/create")
    assert body["cluster_name"] == CLUSTER
    assert body["num_workers"] == 0
    assert body["data_security_mode"] == "SINGLE_USER"
    assert body["single_user_name"] == USER
    assert body["custom_tags"]["ResourceClass"] == "SingleNode"
    assert body["spark_conf"]["spark.databricks.cluster.profile"] == "singleNode"
    # The libraries are installed separately rather than declared on the create
    # call, which is the shape the API requires.
    assert "libraries" not in body


def test_creation_installs_every_workshop_library_and_grants_restart():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["libraries_requested"] == str(len(LIBRARIES))
    installed = http.body_for("/api/2.0/libraries/install")
    assert installed["cluster_id"] == "0101-abc"
    assert len(installed["libraries"]) == len(LIBRARIES)

    acl = http.body_for("/api/2.0/permissions/clusters/0101-abc")
    assert acl["access_control_list"] == [
        {"user_name": USER, "permission_level": "CAN_RESTART"}
    ]


def test_create_failure_carries_the_cluster_name_it_was_working_on():
    client, _ = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            "/api/2.0/clusters/create": http_error(
                400, {"error_code": "INVALID_PARAMETER_VALUE", "message": "no"}
            ),
        }
    )

    with pytest.raises(voclab.VoclabError) as failure:
        ensure(client)

    assert failure.value.code == "CLUSTER_CREATE_FAILED"
    assert "INVALID_PARAMETER_VALUE" in failure.value.message
    assert failure.value.fields["cluster_name"] == CLUSTER


def test_create_that_returns_no_cluster_id_is_a_failure():
    client, _ = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            "/api/2.0/clusters/create": {},
        }
    )

    with pytest.raises(voclab.VoclabError) as failure:
        ensure(client)

    assert failure.value.code == "CLUSTER_CREATE_FAILED"


# --- the create-path preflight -----------------------------------------------


def test_settings_the_workspace_offers_proceed_to_create():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            SPARK_VERSIONS_PATH: spark_versions(
                "16.4.x-cpu-ml-scala2.13", voclab.DEFAULT_SPARK_VERSION
            ),
            NODE_TYPES_PATH: node_types("m5.large", voclab.DEFAULT_NODE_TYPE_ID),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "created"
    assert SPARK_VERSIONS_PATH in http.paths("GET")
    assert NODE_TYPES_PATH in http.paths("GET")


def test_a_spark_version_the_workspace_does_not_offer_is_named_with_the_options():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            SPARK_VERSIONS_PATH: spark_versions(
                "16.4.x-cpu-ml-scala2.13", "15.4.x-cpu-ml-scala2.13"
            ),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    with pytest.raises(voclab.VoclabError) as failure:
        ensure(client)

    assert failure.value.code == "UNAVAILABLE_SPARK_VERSION"
    assert voclab.DEFAULT_SPARK_VERSION in failure.value.message
    assert "16.4.x-cpu-ml-scala2.13" in failure.value.message
    assert "15.4.x-cpu-ml-scala2.13" in failure.value.message
    # A refusal, not a substitution: a course tested on one runtime must not
    # quietly run on another.
    assert "/api/2.0/clusters/create" not in http.paths()


def test_the_versions_offered_are_reported_as_a_bounded_relevant_subset():
    # A workspace offers something like sixty runtimes and this message ends up
    # in voccustomdata.txt, so the message shows the same ML/Scala family and
    # counts the rest.
    # The family is derived from the pinned default, which is a plain runtime
    # rather than an ML one: a course states what it needs in
    # VOC_COURSE_LIBRARIES and gets it installed, so paying for ML's boot time
    # buys nothing.
    family = [f"16.{minor}.x-scala2.13" for minor in range(15)]
    other_families = ["17.3.x-scala2.12", "17.3.x-gpu-ml-scala2.13"]
    client, _ = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            SPARK_VERSIONS_PATH: spark_versions(*family, *other_families),
        }
    )

    with pytest.raises(voclab.VoclabError) as failure:
        ensure(client)

    message = failure.value.message
    shown = voclab.PREFLIGHT_OPTION_LIMIT
    assert f"and {len(family) - shown} more" in message
    assert message.count("16.") == shown
    for irrelevant in other_families:
        assert irrelevant not in message


def test_a_node_type_the_workspace_does_not_offer_is_named_with_the_options():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            SPARK_VERSIONS_PATH: spark_versions(voclab.DEFAULT_SPARK_VERSION),
            NODE_TYPES_PATH: node_types("m5.large", "m5.xlarge"),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    with pytest.raises(voclab.VoclabError) as failure:
        ensure(client)

    assert failure.value.code == "UNAVAILABLE_NODE_TYPE"
    assert voclab.DEFAULT_NODE_TYPE_ID in failure.value.message
    assert "m5.large" in failure.value.message
    assert "/api/2.0/clusters/create" not in http.paths()


def test_a_preflight_that_cannot_run_does_not_block_creation():
    # The one failure here that must not be fatal. A workspace that refuses to
    # list what it offers would otherwise be an unprovisionable lab, and
    # clusters/create is the authority on whether a setting is accepted.
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            SPARK_VERSIONS_PATH: http_error(403, {"error_code": "PERMISSION_DENIED"}),
            NODE_TYPES_PATH: http_error(403, {"error_code": "PERMISSION_DENIED"}),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "created"
    assert fields["cluster_id"] == "0101-abc"
    assert "/api/2.0/clusters/create" in http.paths()


def test_an_unrecognized_preflight_payload_does_not_block_creation():
    client, _ = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            SPARK_VERSIONS_PATH: {"unexpected": "shape"},
            NODE_TYPES_PATH: {"node_types": []},
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    assert ensure(client)["cluster_action"] == "created"


def test_an_existing_cluster_does_not_pay_for_the_preflight():
    # The fast path a student waits on. These two settings are read only by
    # clusters/create, so validating them here would be two round trips that
    # cannot change the outcome.
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": "TERMINATED",
                }
            )
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "started"
    assert SPARK_VERSIONS_PATH not in http.paths()
    assert NODE_TYPES_PATH not in http.paths()


# --- the instance pool, which is the create body's other shape ----------------
#
# Measured: node_type_id, aws_attributes and enable_elastic_disk are
# all pool-owned, and sending any of them beside instance_pool_id answers 400
# INVALID_PARAMETER_VALUE naming the field. A fake urlopen accepts anything, so
# the 400 itself cannot be tested here and is recorded in docs/pre-warm.md. What
# is testable, and what these assert, is that the body never carries both.


def test_a_pool_supplies_the_node_and_the_three_pool_owned_keys_come_out():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool()),
            SPARK_VERSIONS_PATH: spark_versions(voclab.DEFAULT_SPARK_VERSION),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["pool"] == voclab.DEFAULT_POOL_NAME
    body = http.body_for("/api/2.0/clusters/create")
    assert body["instance_pool_id"] == "1234-pool"
    assert "node_type_id" not in body
    assert "aws_attributes" not in body
    assert "enable_elastic_disk" not in body
    # Everything else is the same cluster it always was.
    assert body["num_workers"] == 0
    assert body["data_security_mode"] == "SINGLE_USER"
    assert body["custom_tags"]["ResourceClass"] == "SingleNode"


def test_a_pooled_create_skips_the_node_type_check_and_keeps_the_runtime_one():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool()),
            SPARK_VERSIONS_PATH: spark_versions(voclab.DEFAULT_SPARK_VERSION),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    ensure(client)

    # The pool owns the node type, so asking the workspace whether it offers one
    # is a round trip about a field the body does not carry.
    assert NODE_TYPES_PATH not in http.paths()
    assert SPARK_VERSIONS_PATH in http.paths()


def test_no_pool_builds_exactly_the_body_it_built_before():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["pool"] == "absent"
    assert "pool_preload" not in fields
    assert http.body_for("/api/2.0/clusters/create") == voclab.cluster_settings(
        cluster_name=CLUSTER,
        user_email=USER,
        node_type_id=voclab.DEFAULT_NODE_TYPE_ID,
        spark_version=voclab.DEFAULT_SPARK_VERSION,
        autotermination_minutes=voclab.DEFAULT_AUTOTERMINATION_MINUTES,
    )


@pytest.mark.parametrize("state", ["DELETED", "STOPPED", "UNKNOWN"])
def test_a_pool_that_is_not_active_is_treated_as_absent(state):
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool(state=state)),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["pool"] == "inactive"
    body = http.body_for("/api/2.0/clusters/create")
    assert body["node_type_id"] == voclab.DEFAULT_NODE_TYPE_ID
    assert "instance_pool_id" not in body


def test_a_pool_with_another_name_is_not_the_nearest_match():
    # The name is the join key. A pool named for a different course in the same
    # workspace has to read as absent, not as close enough.
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool(name="workshop-pool-2"), pool(name="other")),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["pool"] == "absent"
    assert "instance_pool_id" not in http.body_for("/api/2.0/clusters/create")


def test_a_named_pool_is_found_when_it_is_the_one_asked_for():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool(name="other"), pool(name="course-b", pool_id="99")),
            SPARK_VERSIONS_PATH: spark_versions(voclab.DEFAULT_SPARK_VERSION),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client, pool_name="course-b")

    assert fields["pool"] == "course-b"
    assert http.body_for("/api/2.0/clusters/create")["instance_pool_id"] == "99"


def test_a_runtime_the_pool_did_not_preload_is_recorded_rather_than_refused():
    # Measured: the create is accepted, the cluster boots, and it saves nothing.
    # Nothing in Databricks says so, so the record is the only place it appears.
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool(preloaded=("16.4.x-cpu-ml-scala2.13",))),
            SPARK_VERSIONS_PATH: spark_versions(voclab.DEFAULT_SPARK_VERSION),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["pool_preload"] == "miss"
    assert fields["cluster_action"] == "created"
    assert http.body_for("/api/2.0/clusters/create")["instance_pool_id"] == "1234-pool"


def test_a_preloaded_runtime_is_recorded_as_a_hit():
    client, _ = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: pools(pool()),
            SPARK_VERSIONS_PATH: spark_versions(voclab.DEFAULT_SPARK_VERSION),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    assert ensure(client)["pool_preload"] == "hit"


def test_a_pool_listing_that_cannot_be_read_still_produces_a_cluster():
    # A student's session is the wrong place to enforce instructor setup, so
    # every way the pool can be missing ends in a cluster.
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(),
            POOLS_PATH: http_error(403, {"error_code": "PERMISSION_DENIED"}),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["pool"] == "unreadable"
    assert fields["cluster_action"] == "created"
    assert http.body_for("/api/2.0/clusters/create")["node_type_id"]


def test_an_existing_cluster_is_not_re_decided_against_the_pool():
    # The fast path a student waits on. The cluster came off whatever the pool
    # situation was when it was created, and re-reading it cannot change that.
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": "RUNNING",
                }
            )
        }
    )

    fields = ensure(client)

    assert "pool" not in fields
    assert POOLS_PATH not in http.paths()


def test_the_pool_name_comes_from_the_flag_then_the_environment(monkeypatch):
    parser = voclab.build_parser()

    monkeypatch.delenv(voclab.POOL_NAME_VAR, raising=False)
    args = parser.parse_args(["cluster-ensure"])
    assert voclab.resolve_pool_name(args) == voclab.DEFAULT_POOL_NAME

    monkeypatch.setenv(voclab.POOL_NAME_VAR, "course-b")
    assert voclab.resolve_pool_name(args) == "course-b"

    flagged = parser.parse_args(["cluster-ensure", "--pool-name", "course-c"])
    assert voclab.resolve_pool_name(flagged) == "course-c"


# --- the start and no-op branches, which are what make it idempotent ---------


def test_terminated_cluster_is_started_rather_than_recreated():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": "TERMINATED",
                }
            )
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "started"
    assert "/api/2.0/clusters/create" not in http.paths()
    assert http.body_for("/api/2.0/clusters/start") == {"cluster_id": "0101-abc"}


@pytest.mark.parametrize("state", sorted(voclab.LIVE_CLUSTER_STATES))
def test_a_live_cluster_is_left_alone(state):
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": state,
                }
            )
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "running"
    assert fields["cluster_state"] == state
    assert "/api/2.0/clusters/start" not in http.paths()
    assert "/api/2.0/clusters/create" not in http.paths()


def test_a_cluster_belonging_to_someone_else_is_not_mistaken_for_this_one():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": "student2_200000",
                    "cluster_id": "0202-xyz",
                    "state": "RUNNING",
                }
            ),
            "/api/2.0/clusters/create": {"cluster_id": "0101-abc"},
        }
    )

    fields = ensure(client)

    assert fields["cluster_action"] == "created"
    assert http.body_for("/api/2.0/clusters/create")["cluster_name"] == CLUSTER


def test_the_second_call_asks_for_no_libraries_it_already_has():
    statuses = [{"library": library, "status": "INSTALLED"} for library in LIBRARIES]
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": "RUNNING",
                }
            ),
            "/api/2.0/libraries/cluster-status?cluster_id=0101-abc": {
                "library_statuses": statuses
            },
        }
    )

    fields = ensure(client)

    assert fields["libraries_requested"] == "0"
    assert "/api/2.0/libraries/install" not in http.paths()


# --- library reconciliation --------------------------------------------------


def test_a_failed_library_is_asked_for_again():
    wanted = [{"pypi": {"package": "neo4j==6.2.0"}}]
    statuses = [{"library": wanted[0], "status": "FAILED"}]
    assert voclab.missing_libraries(statuses, wanted) == wanted


def test_a_pending_library_is_left_to_arrive():
    wanted = [{"pypi": {"package": "neo4j==6.2.0"}}]
    statuses = [{"library": wanted[0], "status": "PENDING"}]
    assert voclab.missing_libraries(statuses, wanted) == []


def test_an_absent_status_list_means_nothing_is_installed():
    wanted = list(LIBRARIES)
    assert voclab.missing_libraries(None, wanted) == wanted


# --- resource tags, whose format is not documented ---------------------------


def test_resource_tags_are_read_as_json_when_they_look_like_json():
    assert voclab.parse_resource_tags('{"course": "neo4j", "part": "1"}') == {
        "course": "neo4j",
        "part": "1",
    }


def test_resource_tags_are_read_as_key_value_pairs_otherwise():
    assert voclab.parse_resource_tags("course=neo4j,part=1") == {
        "course": "neo4j",
        "part": "1",
    }


def test_unparsable_resource_tags_are_dropped_rather_than_guessed_at():
    # A tag Databricks would reject fails the whole create call, so no tags is
    # the cheaper wrong answer than a malformed one.
    assert voclab.parse_resource_tags("course neo4j part 1") == {}
    assert voclab.parse_resource_tags('{"not": ') == {}
    assert voclab.parse_resource_tags("") == {}


def test_resource_tags_cannot_take_the_single_node_marker_away():
    settings = voclab.cluster_settings(
        cluster_name=CLUSTER,
        user_email=USER,
        node_type_id=voclab.DEFAULT_NODE_TYPE_ID,
        spark_version=voclab.DEFAULT_SPARK_VERSION,
        autotermination_minutes=90,
        tags={"ResourceClass": "Serverless", "course": "neo4j"},
    )
    assert settings["custom_tags"]["ResourceClass"] == "SingleNode"
    assert settings["custom_tags"]["course"] == "neo4j"


def test_the_initial_catalog_is_set_only_when_one_is_named():
    without = voclab.cluster_settings(
        cluster_name=CLUSTER,
        user_email=USER,
        node_type_id=voclab.DEFAULT_NODE_TYPE_ID,
        spark_version=voclab.DEFAULT_SPARK_VERSION,
        autotermination_minutes=90,
    )
    assert "spark.databricks.sql.initial.catalog.name" not in without["spark_conf"]

    with_catalog = voclab.cluster_settings(
        cluster_name=CLUSTER,
        user_email=USER,
        node_type_id=voclab.DEFAULT_NODE_TYPE_ID,
        spark_version=voclab.DEFAULT_SPARK_VERSION,
        autotermination_minutes=90,
        catalog="workshop",
    )
    assert (
        with_catalog["spark_conf"]["spark.databricks.sql.initial.catalog.name"]
        == "workshop"
    )


# --- reclaiming ---------------------------------------------------------------


def test_stop_terminates_the_cluster_and_keeps_its_configuration():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": "RUNNING",
                }
            )
        }
    )

    fields = voclab.cluster_release(client, USER, permanent=False)

    assert fields["cluster_action"] == "terminated"
    assert "/api/2.0/clusters/delete" in http.paths()
    assert "/api/2.0/clusters/permanent-delete" not in http.paths()


def test_terminate_removes_the_cluster_outright():
    client, http = workspace(
        {
            "/api/2.0/clusters/list": listing(
                {
                    "cluster_name": CLUSTER,
                    "cluster_id": "0101-abc",
                    "state": "TERMINATED",
                }
            )
        }
    )

    fields = voclab.cluster_release(client, USER, permanent=True)

    assert fields["cluster_action"] == "removed"
    assert "/api/2.0/clusters/permanent-delete" in http.paths()


def test_reclaiming_a_cluster_that_is_already_gone_is_not_a_failure():
    client, http = workspace({"/api/2.0/clusters/list": listing()})

    fields = voclab.cluster_release(client, USER, permanent=True)

    assert fields["cluster_action"] == "absent"
    assert http.paths("POST") == []


# --- handing the student the notebook ----------------------------------------
#
# Vocareum does not deliver it. /voc/startercode was expected to and, measured
# in Student View, reaches the environment beside Databricks rather than the
# Databricks workspace. So user_setup.sh
# imports it, and what these tests pin is that a second start does not overwrite
# the copy the student has been working in.

NOTEBOOK_PATH = f"/Users/{USER}/00_cluster_smoke_test"
IMPORT_PATH = "/api/2.0/workspace/import"


def status_path(path: str) -> str:
    return f"/api/2.0/workspace/get-status?{urllib.parse.urlencode({'path': path})}"


def notebook_file(tmp_path, body: str = '{"cells": []}') -> str:
    source = tmp_path / "00_cluster_smoke_test.ipynb"
    source.write_text(body)
    return str(source)


def absent(path: str) -> urllib.error.HTTPError:
    return http_error(404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": path})


def test_a_student_without_the_notebook_is_given_it(tmp_path):
    source = notebook_file(tmp_path, '{"cells": ["one"]}')
    client, http = workspace({status_path(NOTEBOOK_PATH): absent(NOTEBOOK_PATH)})

    fields = voclab.notebooks_import(client, USER, [source])

    assert fields["notebook_action"] == "imported"
    assert fields["notebook_path"] == NOTEBOOK_PATH
    assert fields["notebook_count"] == "1"
    body = http.body_for(IMPORT_PATH)
    assert body["path"] == NOTEBOOK_PATH
    assert body["format"] == "JUPYTER"
    assert body["overwrite"] is False
    assert base64.b64decode(body["content"]).decode() == '{"cells": ["one"]}'
    # No language on a JUPYTER import. The notebook's own metadata answers it,
    # and stating it here would be a second source for the same fact.
    assert "language" not in body


def test_the_imported_path_carries_no_extension(tmp_path):
    # A Databricks notebook object has no extension, so importing to a path
    # ending in .ipynb names the student's notebook "...ipynb" and it reads as a
    # stray file rather than as the lab.
    assert not voclab.notebook_workspace_path(USER, "a/b/00_smoke.ipynb").endswith(
        ".ipynb"
    )
    assert voclab.notebook_workspace_path(USER, "00_smoke.ipynb") == (
        f"/Users/{USER}/00_smoke"
    )


def test_a_python_module_keeps_its_extension_on_the_way_in():
    # The other half of the AUTO change, and it does not work without this one.
    # Measured: workspace/import with format AUTO stores anything at
    # an extensionless path as a NOTEBOOK, whatever the content, so stripping
    # .py would leave the defect exactly where it was.
    assert voclab.notebook_workspace_path(USER, "Lab_3/data_utils.py") == (
        f"/Users/{USER}/Lab_3/data_utils.py"
    )


def test_a_python_module_is_imported_as_auto_with_no_language(tmp_path):
    # The defect this replaces. SOURCE with language PYTHON always produces a
    # NOTEBOOK, and a notebook is the one thing `from data_utils import ...`
    # cannot read: NotebookImportException, in front of every student in the
    # class. AUTO reads the file, and a module with no
    # "# Databricks notebook source" first line arrives as a workspace file.
    source = tmp_path / "data_utils.py"
    source.write_text('"""Helpers the lab notebooks import."""\n')
    path = f"/Users/{USER}/data_utils.py"
    client, http = workspace({status_path(path): absent(path)})

    fields = voclab.notebooks_import(client, USER, [str(source)])

    assert fields["notebook_action"] == "imported"
    assert fields["notebook_path"] == path
    body = http.body_for(IMPORT_PATH)
    assert body["path"] == path
    assert body["format"] == "AUTO"
    assert "language" not in body


def test_a_module_the_student_already_has_is_left_alone(tmp_path):
    # The check and the write have to name the same path, or the import is not
    # idempotent and user_setup.sh overwrites the student's work on the start
    # after a stop. Keeping .py is only safe because AUTO stores it as written.
    source = tmp_path / "data_utils.py"
    source.write_text('"""Helpers the lab notebooks import."""\n')
    path = f"/Users/{USER}/data_utils.py"
    client, http = workspace({status_path(path): {"object_type": "FILE"}})

    fields = voclab.notebooks_import(client, USER, [str(source)])

    assert fields["notebook_action"] == "kept"
    assert http.paths("POST") == []


def test_a_notebook_the_student_already_has_is_left_alone(tmp_path):
    # The case that matters on a stop-then-resume: overwriting would hand the
    # student a blank copy of the notebook they spent the first session in.
    source = notebook_file(tmp_path)
    client, http = workspace({status_path(NOTEBOOK_PATH): {"object_type": "NOTEBOOK"}})

    fields = voclab.notebooks_import(client, USER, [source])

    assert fields["notebook_action"] == "kept"
    assert http.paths("POST") == []


def test_a_notebook_that_appears_between_the_check_and_the_write_is_not_a_failure(
    tmp_path,
):
    source = notebook_file(tmp_path)
    client, _ = workspace(
        {
            status_path(NOTEBOOK_PATH): absent(NOTEBOOK_PATH),
            IMPORT_PATH: http_error(400, {"error_code": "RESOURCE_ALREADY_EXISTS"}),
        }
    )

    fields = voclab.notebooks_import(client, USER, [source])

    assert fields["notebook_action"] == "kept"


# --- the folders a nested course needs ---------------------------------------
#
# workspace/import does not create missing parents. Measured: a directory tree
# fails with "The parent folder (...) does not exist", which fails
# user_setup.sh partway through the import. Targets directly in the student's
# home folder never meet it.

MKDIRS_PATH = "/api/2.0/workspace/mkdirs"


def nested_course(tmp_path) -> str:
    """The shape that found the bug: two lab folders, one of them holding two
    files, and a smoke test at the top level."""
    root = tmp_path / "notebooks"
    for relative in (
        "00_cluster_smoke_test.ipynb",
        "Lab_2/01_etl.ipynb",
        "Lab_3/01_search.ipynb",
        "Lab_3/data_utils.py",
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('{"cells": []}')
    return str(root)


def test_every_parent_folder_is_created_before_any_notebook_is_written(tmp_path):
    client, http = workspace({})
    for relative in (
        "00_cluster_smoke_test",
        "Lab_2/01_etl",
        "Lab_3/01_search",
        # The extension stays on this one. NOTEBOOK_KEEP_EXTENSIONS says why,
        # and this is the test that would have caught the stripped-path version
        # of the AUTO change: it asks get-status for a path nothing writes, so
        # every run re-imports.
        "Lab_3/data_utils.py",
    ):
        path = f"/Users/{USER}/{relative}"
        http.answers[status_path(path)] = absent(path)

    voclab.notebooks_import(client, USER, [nested_course(tmp_path)])

    # Every folder, then every file. A folder call after an import means a
    # student can still be left half imported, which is the state this ordering
    # exists to make impossible.
    assert http.paths("POST") == [MKDIRS_PATH, MKDIRS_PATH] + [IMPORT_PATH] * 4
    created = [body["path"] for _, path, body in http.calls if path == MKDIRS_PATH]
    assert created == [f"/Users/{USER}/Lab_2", f"/Users/{USER}/Lab_3"]


def test_the_home_folder_is_never_created_and_a_folder_is_never_asked_for_twice():
    # Databricks creates /Users/<email> when it provisions the identity. Asking
    # again buys nothing, and creating it when it is absent would manufacture a
    # home directory for a user who does not exist rather than failing where the
    # problem is. Lab_3 is named twice and must be created once.
    home = f"/Users/{USER}"
    paths = [
        f"{home}/00_cluster_smoke_test",
        f"{home}/Lab_3/01_search",
        f"{home}/Lab_3/data_utils",
        f"{home}/Lab_2/01_etl",
    ]

    assert voclab.notebook_parent_folders(home, paths) == [
        f"{home}/Lab_3",
        f"{home}/Lab_2",
    ]


def test_a_deep_tree_asks_for_the_leaf_folder_only():
    # mkdirs creates the whole chain, so naming the intermediate folders as well
    # would be two calls for one outcome.
    home = f"/Users/{USER}"
    assert voclab.notebook_parent_folders(home, [f"{home}/a/b/c/notebook"]) == [
        f"{home}/a/b/c"
    ]


def test_a_folder_that_already_exists_is_not_a_failure(tmp_path):
    # Measured: mkdirs answers an existing folder with 200 and an
    # empty body, twice running. So a resumed session pays a no-op call rather
    # than needing a get-status check before each one.
    client, http = workspace({MKDIRS_PATH: {}})
    root = tmp_path / "notebooks"
    (root / "Lab_3").mkdir(parents=True)
    (root / "Lab_3" / "01_search.ipynb").write_text('{"cells": []}')
    path = f"/Users/{USER}/Lab_3/01_search"
    http.answers[status_path(path)] = absent(path)

    fields = voclab.notebooks_import(client, USER, [str(root)])

    assert fields["notebook_action"] == "imported"


def test_a_folder_that_cannot_be_created_stops_the_run_before_any_import(tmp_path):
    # The whole reason the folders go first. A caller that may not write here
    # finds out before a student's workspace is half built.
    client, http = workspace(
        {MKDIRS_PATH: http_error(403, {"error_code": "PERMISSION_DENIED"})}
    )
    root = tmp_path / "notebooks"
    (root / "Lab_3").mkdir(parents=True)
    (root / "Lab_3" / "01_search.ipynb").write_text('{"cells": []}')

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebooks_import(client, USER, [str(root)])

    assert failure.value.code == "NOTEBOOK_FOLDER_FAILED"
    assert f"/Users/{USER}/Lab_3" in failure.value.message
    assert IMPORT_PATH not in http.paths()


def test_a_missing_notebook_says_where_it_looked_and_calls_nothing(tmp_path):
    client, http = workspace({})

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebooks_import(client, USER, [str(tmp_path / "not_there.ipynb")])

    assert failure.value.code == "MISSING_NOTEBOOK"
    assert "not_there.ipynb" in failure.value.message
    assert http.calls == []


def test_a_bare_name_is_searched_for_in_the_three_places_voc_find_looks():
    # The hooks reach voclab.py through voc_voclab, and a real session proved $0
    # can carry no directory at all, so a bare name cannot be resolved from the
    # working directory alone. The three candidates are named in the failure so
    # that a course that forgot to upload its content is told where it was
    # expected rather than only that it was absent.
    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebook_source_path("no_such_notebook.ipynb")

    assert failure.value.code == "MISSING_NOTEBOOK"
    assert str(VOCLAB_PATH.parent) in failure.value.message
    assert "/voc/scripts" in failure.value.message
    assert voclab.NOTEBOOKS_VAR in failure.value.message


def test_a_course_that_names_no_notebooks_imports_nothing_and_does_not_fail():
    # Only user_setup.sh is a required hook, and a course whose content reaches
    # the student some other way is a real course. The record has to say which
    # case it was, so an empty import is not silence.
    client, http = workspace({})

    fields = voclab.notebooks_import(client, USER, [])

    assert fields["notebook_action"] == "none"
    assert fields["notebook_count"] == "0"
    assert "notebook_path" not in fields
    assert http.calls == []


def test_a_named_directory_contributes_every_notebook_under_it(tmp_path):
    root = tmp_path / "notebooks"
    (root / "Lab_1").mkdir(parents=True)
    (root / "Lab_1" / "setup.ipynb").write_text('{"cells": []}')
    (root / "Lab_1" / "notes.csv").write_text("a,b\n")
    (root / "00_intro.sql").write_text("SELECT 1")

    pairs = voclab.notebook_sources([str(root)])

    # Sorted, and the relative path is kept so a folder of labs stays a folder
    # of labs. The CSV is skipped rather than imported as a notebook, which
    # would succeed and produce an object nobody can open.
    assert [relative for _, relative in pairs] == ["00_intro.sql", "Lab_1/setup.ipynb"]


def test_a_source_import_is_told_the_language_a_jupyter_import_carries(tmp_path):
    source = tmp_path / "00_intro.sql"
    source.write_text("SELECT 1")
    path = f"/Users/{USER}/00_intro"
    client, http = workspace({status_path(path): absent(path)})

    voclab.notebooks_import(client, USER, [str(source)])

    body = http.body_for(IMPORT_PATH)
    assert body["format"] == "SOURCE"
    assert body["language"] == "SQL"


def test_the_landing_page_is_the_first_notebook_the_course_named(tmp_path):
    first = tmp_path / "00_start.ipynb"
    second = tmp_path / "01_next.ipynb"
    first.write_text('{"cells": []}')
    second.write_text('{"cells": []}')
    client, _ = workspace(
        {
            status_path(f"/Users/{USER}/00_start"): absent("00_start"),
            status_path(f"/Users/{USER}/01_next"): absent("01_next"),
        }
    )

    fields = voclab.notebooks_import(client, USER, [str(first), str(second)])

    assert fields["notebook_path"] == f"/Users/{USER}/00_start"
    assert fields["notebook_count"] == "2"


def test_a_directory_that_ships_nothing_importable_is_a_failure(tmp_path):
    root = tmp_path / "notebooks"
    root.mkdir()
    (root / "data.csv").write_text("a,b\n")

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebook_sources([str(root)])

    assert failure.value.code == "MISSING_NOTEBOOK"


# --- naming individual notebooks by path --------------------------------------
#
# A course excludes one notebook from a folder it otherwise ships whole by
# naming the files it wants instead of the folder. That only works if a named
# path is found the same way a bare name is and arrives in the student's folder
# with its structure intact, which is what these four pin.


def test_a_relative_path_is_searched_for_the_same_way_a_bare_name_is(
    tmp_path, monkeypatch
):
    # The bug this replaces: a name carrying a directory used to be handed to
    # the filesystem as written, so it resolved against the working directory,
    # which is the one thing voclib.sh records as unguaranteed. It worked from a
    # laptop and failed inside Vocareum with nothing printed to say why.
    beside_the_hooks = tmp_path / "scripts"
    (beside_the_hooks / "Lab_3").mkdir(parents=True)
    (beside_the_hooks / "Lab_3" / "01_intro.ipynb").write_text('{"cells": []}')
    monkeypatch.setattr(
        voclab, "NOTEBOOK_SEARCH_DIRECTORIES", (str(beside_the_hooks), "/voc/scripts")
    )

    found = voclab.notebook_source_path("Lab_3/01_intro.ipynb")

    assert found == str(beside_the_hooks / "Lab_3" / "01_intro.ipynb")


def test_a_named_file_keeps_the_folder_the_course_wrote(tmp_path, monkeypatch):
    # The point of the whole change. Naming files rather than a directory is how
    # a course leaves one notebook out, and under the old basename rule that
    # choice flattened three labs' worth of 01_ notebooks into one folder with
    # nothing saying which lab each came from.
    root = tmp_path / "scripts"
    for lab in ("Lab_2", "Lab_3"):
        (root / lab).mkdir(parents=True)
        (root / lab / "01_intro.ipynb").write_text('{"cells": []}')
    (root / "Lab_2" / "02_excluded.ipynb").write_text('{"cells": []}')
    monkeypatch.setattr(voclab, "NOTEBOOK_SEARCH_DIRECTORIES", (str(root),))

    pairs = voclab.notebook_sources(["Lab_2/01_intro.ipynb", "Lab_3/01_intro.ipynb"])

    assert [relative for _, relative in pairs] == [
        "Lab_2/01_intro.ipynb",
        "Lab_3/01_intro.ipynb",
    ]
    assert voclab.notebook_workspace_path(USER, pairs[0][1]) == (
        f"/Users/{USER}/Lab_2/01_intro"
    )


def test_an_absolute_path_still_lands_under_its_basename(tmp_path):
    # The laptop case. An absolute path names a place on this machine, and
    # keeping it whole would give the student a folder named after somebody's
    # home directory.
    source = notebook_file(tmp_path)

    pairs = voclab.notebook_sources([source])

    assert [relative for _, relative in pairs] == ["00_cluster_smoke_test.ipynb"]


def test_a_name_that_climbs_out_of_the_upload_is_refused(tmp_path, monkeypatch):
    # Reachable only by a typo in course.env, and the symptom without this guard
    # is a notebook at a workspace path holding `..` that nobody can explain.
    outside = tmp_path / "elsewhere"
    outside.mkdir()
    (outside / "stray.ipynb").write_text('{"cells": []}')
    monkeypatch.setattr(
        voclab, "NOTEBOOK_SEARCH_DIRECTORIES", (str(tmp_path / "scripts"), str(outside))
    )

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebook_sources(["../elsewhere/stray.ipynb"])

    assert failure.value.code == "MISSING_NOTEBOOK"
    assert "climbs out" in failure.value.message


def test_a_failed_import_carries_the_path_it_was_working_on(tmp_path):
    source = notebook_file(tmp_path)
    client, _ = workspace(
        {
            status_path(NOTEBOOK_PATH): absent(NOTEBOOK_PATH),
            IMPORT_PATH: http_error(403, {"error_code": "PERMISSION_DENIED"}),
        }
    )

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebooks_import(client, USER, [source])

    assert failure.value.code == "NOTEBOOK_IMPORT_FAILED"
    assert failure.value.fields["notebook_path"] == NOTEBOOK_PATH


def test_a_status_read_that_fails_for_another_reason_is_not_read_as_absent(tmp_path):
    source = notebook_file(tmp_path)
    refused = http_error(403, {"error_code": "PERMISSION_DENIED"})
    client, http = workspace({status_path(NOTEBOOK_PATH): refused})

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.notebooks_import(client, USER, [source])

    assert failure.value.code == "NOTEBOOK_STATUS_FAILED"
    assert http.paths("POST") == []


# --- retry, and the failures it must not retry -------------------------------


def test_a_rate_limit_is_retried_even_for_a_create(monkeypatch):
    monkeypatch.setattr(voclab.time, "sleep", lambda seconds: None)
    client, http = workspace(
        {
            "/api/2.0/clusters/create": [
                http_error(429, {"error_code": "REQUEST_LIMIT_EXCEEDED"}),
                {"cluster_id": "0101-abc"},
            ]
        }
    )

    result = client.call("POST", "/api/2.0/clusters/create", {"a": 1})

    assert result == {"cluster_id": "0101-abc"}
    assert len(http.paths("POST")) == 2


def test_a_server_error_is_not_retried_for_a_create(monkeypatch):
    # The request may well have been carried out. Retrying it is how one student
    # ends up with two clusters, so the failure is surfaced instead.
    monkeypatch.setattr(voclab.time, "sleep", lambda seconds: None)
    client, http = workspace(
        {
            "/api/2.0/clusters/create": [
                http_error(500, {"message": "internal"}),
                {"cluster_id": "0101-abc"},
            ]
        }
    )

    with pytest.raises(voclab.ApiError):
        client.call("POST", "/api/2.0/clusters/create", {"a": 1})

    assert len(http.paths("POST")) == 1


def test_a_server_error_is_retried_for_an_idempotent_call(monkeypatch):
    monkeypatch.setattr(voclab.time, "sleep", lambda seconds: None)
    client, http = workspace(
        {
            "/api/2.0/clusters/list": [
                http_error(503, {"message": "unavailable"}),
                listing(),
            ]
        }
    )

    assert client.call("GET", "/api/2.0/clusters/list", idempotent=True) == listing()
    assert len(http.paths("GET")) == 2


def test_retries_are_bounded(monkeypatch):
    monkeypatch.setattr(voclab.time, "sleep", lambda seconds: None)
    attempts = len(voclab.RETRY_BACKOFF_SECONDS) + 1
    client, http = workspace(
        {"/api/2.0/clusters/list": [http_error(429) for _ in range(attempts + 2)]}
    )

    with pytest.raises(voclab.ApiError):
        client.call("GET", "/api/2.0/clusters/list", idempotent=True)

    assert len(http.paths("GET")) == attempts


# --- the output contract the shell reads -------------------------------------


@pytest.fixture
def no_credentials(monkeypatch):
    """Remove every credential, so no test here can reach a real workspace."""
    for name in (
        "VOC_DB_WORKSPACE_URL",
        "VOC_DB_API_TOKEN",
        "VOC_DB_USER_EMAIL",
        "VOC_RESOURCE_TAGS",
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)


def emitted(capsys) -> dict:
    """Read the key=value contract back the way voc_voclab reads it."""
    return dict(
        line.split("=", 1) for line in capsys.readouterr().out.splitlines() if line
    )


def test_every_failure_emits_an_error_code_and_a_message(capsys, no_credentials):
    # The whole reason this program exists rather than a call to the SDK: a hook
    # that exits non-zero without these two produces a lab that failed for no
    # stated reason.
    status = voclab.main(["cluster-ensure", "--user", USER])

    assert status == voclab.EXIT_FAILED
    fields = emitted(capsys)
    assert fields["error_code"] == "MISSING_WORKSPACE_URL"
    assert fields["message"]


def test_a_missing_token_is_named_rather_than_failing_obscurely(
    capsys, no_credentials, monkeypatch
):
    monkeypatch.setenv("VOC_DB_WORKSPACE_URL", f"{HOST}/?o=2222222222222222")

    assert voclab.main(["cluster-ensure", "--user", USER]) == voclab.EXIT_FAILED
    assert emitted(capsys)["error_code"] == "MISSING_API_TOKEN"


def test_the_shared_options_are_accepted_after_the_subcommand(capsys, no_credentials):
    # The order every hook uses. When these options live on the top-level parser
    # instead, argparse rejects this with exit code 2 and writes nothing to
    # stdout, which is the one failure shape this program must never produce.
    assert voclab.main(["cluster-ensure", "--user", USER]) == voclab.EXIT_FAILED
    fields = emitted(capsys)
    assert fields["error_code"] != "BAD_ARGUMENTS"
    assert "--user" not in fields["message"]


def test_a_bad_command_line_still_names_itself(capsys, no_credentials):
    with pytest.raises(SystemExit) as exit_status:
        voclab.main(["cluster-ensure", "--nonsense"])

    assert exit_status.value.code == voclab.EXIT_FAILED
    assert emitted(capsys)["error_code"] == "BAD_ARGUMENTS"


def test_an_unexpected_exception_still_reaches_the_record(
    capsys, monkeypatch, no_credentials
):
    def explode(args):
        raise RuntimeError("something nobody anticipated")

    monkeypatch.setattr(voclab, "run", explode)

    status = voclab.main(["cluster-ensure", "--user", USER])

    assert status == voclab.EXIT_FAILED
    out = capsys.readouterr().out
    assert "error_code=UNEXPECTED_ERROR" in out
    assert "message=RuntimeError: something nobody anticipated" in out


def test_a_multiline_message_is_collapsed_onto_one_line(capsys):
    # The shell reads one field per line, so a Databricks message running to
    # several lines would otherwise become output it reports as unreadable.
    voclab.emit({"message": "first line\nsecond line", "cluster_name": CLUSTER})

    lines = [line for line in capsys.readouterr().out.splitlines() if line]
    assert lines == ["message=first line second line", f"cluster_name={CLUSTER}"]


def test_an_invalid_output_key_is_a_programming_error():
    with pytest.raises(AssertionError):
        voclab.emit({"Cluster-Name": CLUSTER})


def test_the_host_is_reduced_before_an_api_path_is_appended():
    # VOC_DB_WORKSPACE_URL carries the ?o= workspace parameter already.
    assert voclab.normalize_host(f"{HOST}/?o=2222222222222222") == HOST
    assert voclab.normalize_host(f"{HOST}/#workspace/Users/x") == HOST


# --- the course's values, which arrive as environment variables --------------
#
# Every one of these is read at call time rather than as an argparse default,
# because a default is evaluated when the parser is built and course.env is
# sourced by voclib.sh in the process that then runs this program. A value read
# too early is a value silently replaced by the package's default.

WAREHOUSES_PATH = "/api/2.0/sql/warehouses"


def test_a_blank_course_value_counts_as_unset(monkeypatch):
    # course.env is a shell file a human edits, and VOC_COURSE_CATALOG= is a key
    # nobody filled in rather than a catalog named "".
    monkeypatch.setenv(voclab.CATALOG_VAR, "   ")
    assert voclab.course_value(voclab.CATALOG_VAR, "workshop") == "workshop"
    monkeypatch.setenv(voclab.CATALOG_VAR, " main ")
    assert voclab.course_value(voclab.CATALOG_VAR, "workshop") == "main"


def test_a_course_number_that_is_not_a_number_fails_rather_than_defaults(monkeypatch):
    # Substituting the default would mean the course's opinion never arrives and
    # nobody is told, which is the expensive half of the two failures.
    monkeypatch.setenv(voclab.AUTOTERMINATION_VAR, "ninety")

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.course_int(voclab.AUTOTERMINATION_VAR, 90)

    assert failure.value.code == "BAD_COURSE_VALUE"
    assert voclab.AUTOTERMINATION_VAR in failure.value.message
    monkeypatch.delenv(voclab.AUTOTERMINATION_VAR)
    assert voclab.course_int(voclab.AUTOTERMINATION_VAR, 90) == 90


def test_a_maven_coordinate_keeps_the_colons_that_are_part_of_it():
    # The kind is split off at the first colon only. A generic split would leave
    # a Maven coordinate missing two of its three fields, and the install would
    # fail minutes into the lab rather than here.
    assert voclab.parse_libraries(COURSE_LIBRARIES_RAW) == LIBRARIES
    assert voclab.parse_libraries("") == []
    assert voclab.parse_libraries("pypi:neo4j\npypi:pandas") == [
        {"pypi": {"package": "neo4j"}},
        {"pypi": {"package": "pandas"}},
    ]


def test_a_library_kind_nobody_recognizes_fails_rather_than_being_skipped():
    # A dropped library is a cluster that comes up fine and a notebook that
    # fails at its first import, which is the most expensive place to find out.
    for bad in ("conda:numpy", "neo4j==6.2.0", "pypi:"):
        with pytest.raises(voclab.VoclabError) as failure:
            voclab.parse_libraries(bad)
        assert failure.value.code == "BAD_COURSE_VALUE"


def test_a_course_value_is_read_when_the_subcommand_runs_not_when_it_is_parsed(
    monkeypatch,
):
    # The regression this pins: an argparse default is evaluated while the
    # parser is built, and course.env reaches this process through the
    # environment. So every course option defaults to None and the environment
    # is consulted afterwards.
    parser = voclab.build_parser()
    args = parser.parse_args(["cluster-ensure", "--user", USER])
    assert args.node_type is None
    assert args.spark_version is None
    assert args.catalog is None
    assert args.libraries is None

    monkeypatch.setenv(voclab.NODE_TYPE_VAR, "m5d.xlarge")
    assert voclab.resolve_course_value(args.node_type, voclab.NODE_TYPE_VAR) == (
        "m5d.xlarge"
    )
    # The flag still wins, which is what makes the command runnable from a
    # laptop against a workspace whose course.env is not on this machine.
    assert voclab.resolve_course_value("i3.xlarge", voclab.NODE_TYPE_VAR) == "i3.xlarge"


# --- the SQL warehouse -------------------------------------------------------
#
# Not per student and never deleted. It exists because the Statement Execution
# API takes a warehouse_id and there is no other way to run SQL from a hook, so
# a course that provisions a catalog with COMMENT ON and GRANT statements needs
# one and a course that does not should not be billed for one.


def warehouses(*entries) -> dict:
    return {"warehouses": list(entries)}


def running(name: str = "workshop-sql", warehouse_id: str = "abc123") -> dict:
    return warehouses({"name": name, "id": warehouse_id, "state": "RUNNING"})


def sent(http, method: str, path: str) -> dict:
    """The body of one request, chosen by method as well as path.

    The list and the create share a path and differ only in verb, so ``body_for``
    would answer the list's ``None`` for a question about the create.
    """
    for verb, seen, body in http.calls:
        if verb == method and seen == path:
            return body
    raise AssertionError(f"no {method} was made to {path}")


def test_a_workspace_without_the_warehouse_gets_a_serverless_one():
    # One path, two verbs: the list answers first and the create answers second.
    client, http = workspace(
        {
            WAREHOUSES_PATH: [
                warehouses({"name": "other", "id": "zzz"}),
                {"id": "abc123"},
            ]
        }
    )

    fields = voclab.warehouse_ensure(client, "workshop-sql", "2X-Small", 10)

    assert fields["warehouse_action"] == "created"
    assert fields["warehouse_id"] == "abc123"
    body = sent(http, "POST", WAREHOUSES_PATH)
    # Serverless is stated by the pair. Either field alone produces a classic
    # warehouse that succeeds and bills differently.
    assert body["warehouse_type"] == voclab.WAREHOUSE_TYPE
    assert body["enable_serverless_compute"] is True
    assert body["auto_stop_mins"] == 10
    assert body["max_num_clusters"] == 1


def test_a_running_warehouse_is_left_alone():
    client, http = workspace({WAREHOUSES_PATH: running()})

    fields = voclab.warehouse_ensure(client, "workshop-sql", "2X-Small", 10)

    assert fields["warehouse_action"] == "running"
    assert fields["warehouse_id"] == "abc123"
    assert http.paths("POST") == []


def test_a_stopped_warehouse_is_started_rather_than_recreated():
    client, http = workspace(
        {
            WAREHOUSES_PATH: warehouses(
                {"name": "workshop-sql", "id": "abc123", "state": "STOPPED"}
            ),
            "/api/2.0/sql/warehouses/abc123/start": {},
        }
    )

    fields = voclab.warehouse_ensure(client, "workshop-sql", "2X-Small", 10)

    assert fields["warehouse_action"] == "started"
    assert http.paths("POST") == ["/api/2.0/sql/warehouses/abc123/start"]


def test_the_lab_group_is_granted_use_and_nothing_more():
    client, http = workspace(
        {
            WAREHOUSES_PATH: running(),
            "/api/2.0/permissions/warehouses/abc123": {},
        }
    )

    fields = voclab.warehouse_ensure(
        client, "workshop-sql", "2X-Small", 10, group_name="Group-lab-1-2-3"
    )

    assert fields["warehouse_granted_to"] == "Group-lab-1-2-3"
    body = http.body_for("/api/2.0/permissions/warehouses/abc123")
    assert body["access_control_list"] == [
        {"group_name": "Group-lab-1-2-3", "permission_level": "CAN_USE"}
    ]


def test_no_group_yet_is_recorded_rather_than_failed():
    # workspace_init.sh runs before any student exists, so there is no
    # VOC_DB_GROUP_NAME to grant to. The warehouse is still worth creating then,
    # and user_setup.sh grants later.
    client, http = workspace({WAREHOUSES_PATH: running()})

    fields = voclab.warehouse_ensure(client, "workshop-sql", "2X-Small", 10)

    assert fields["warehouse_granted_to"] == "none"
    assert http.paths("PUT") == []


def test_a_warehouse_is_never_created_under_a_name_nobody_chose(monkeypatch):
    # This object is billable, so it is named or it is not created. There is no
    # default the way there is for the runtime and the node type.
    monkeypatch.delenv(voclab.WAREHOUSE_NAME_VAR, raising=False)
    args = voclab.build_parser().parse_args(["warehouse-ensure"])

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.resolve_warehouse_name(args)

    assert failure.value.code == "MISSING_WAREHOUSE_NAME"
    monkeypatch.setenv(voclab.WAREHOUSE_NAME_VAR, "workshop-sql")
    assert voclab.resolve_warehouse_name(args) == "workshop-sql"


def test_the_warehouse_needs_no_student_to_exist(monkeypatch):
    # Dispatched before resolve_user, because workspace_init.sh has no
    # VOC_DB_USER_EMAIL and asking for one there would fail the only hook that
    # can create this object once for everybody.
    monkeypatch.setenv("VOC_DB_WORKSPACE_URL", HOST)
    monkeypatch.setenv("VOC_DB_TOKEN", "dapi-test-token")
    monkeypatch.setenv(voclab.WAREHOUSE_NAME_VAR, "workshop-sql")
    monkeypatch.delenv("VOC_DB_USER_EMAIL", raising=False)
    monkeypatch.delenv("VOC_DB_GROUP_NAME", raising=False)
    client, _ = workspace({WAREHOUSES_PATH: running()})
    monkeypatch.setattr(voclab, "build_workspace", lambda args: client)

    fields = voclab.run(voclab.build_parser().parse_args(["warehouse-ensure"]))

    assert fields["warehouse_name"] == "workshop-sql"


# --- the starter warehouse ---------------------------------------------------
#
# The one warehouse no course asked for. Databricks creates it with the
# workspace at Small and grants `users` CAN_USE on it, and Vocareum creates a
# workspace per part, so a fix made by hand lasts one class.

STARTER = "Serverless Starter Warehouse"


def starter(size: str = "Small", warehouse_id: str = "d265", name: str = STARTER):
    return {
        "name": name,
        "id": warehouse_id,
        "cluster_size": size,
        "state": "STOPPED",
    }


def test_the_starter_warehouse_is_resized_to_what_the_course_asked_for():
    client, http = workspace({WAREHOUSES_PATH: warehouses(starter())})

    fields = voclab.starter_warehouse_resize(client, "2X-Small")

    assert fields["starter_warehouse_action"] == "resized"
    assert fields["starter_warehouse_size_before"] == "Small"
    assert http.paths("POST") == ["/api/2.0/sql/warehouses/d265/edit"]
    # POST /edit is an incremental edit, so cluster_size travels alone. Writing
    # the whole object back would make this the owner of fields it does not own.
    assert sent(http, "POST", "/api/2.0/sql/warehouses/d265/edit") == {
        "cluster_size": "2X-Small"
    }


def test_a_starter_warehouse_already_the_right_size_is_left_alone():
    client, http = workspace({WAREHOUSES_PATH: warehouses(starter(size="2X-Small"))})

    fields = voclab.starter_warehouse_resize(client, "2X-Small")

    assert fields["starter_warehouse_action"] == "unchanged"
    assert http.paths("POST") == []


def test_no_starter_warehouse_is_recorded_rather_than_failed():
    # Databricks documents that it creates one and does not promise to keep
    # doing so. Taking a class down over an object no course asked for would
    # save nothing.
    client, http = workspace(
        {WAREHOUSES_PATH: warehouses({"name": "shared_warehouse", "id": "bd98"})}
    )

    fields = voclab.starter_warehouse_resize(client, "2X-Small")

    assert fields["starter_warehouse_action"] == "absent"
    assert fields["starter_warehouse_name"] == "none"
    assert http.paths("POST") == []


def test_the_documented_name_is_tried_when_the_workspace_name_is_absent():
    # Databricks' documentation calls it "Starter Warehouse" and the workspace
    # calls it "Serverless Starter Warehouse". Both are tried.
    client, _ = workspace(
        {WAREHOUSES_PATH: warehouses(starter(name="Starter Warehouse"))}
    )

    fields = voclab.starter_warehouse_resize(client, "2X-Small")

    assert fields["starter_warehouse_action"] == "resized"
    assert fields["starter_warehouse_name"] == "Starter Warehouse"


def test_the_workspace_spelling_wins_over_the_documented_one():
    # Order comes from STARTER_WAREHOUSE_NAMES, not from the order the API
    # happens to return warehouses in, which is undocumented.
    client, _ = workspace(
        {
            WAREHOUSES_PATH: warehouses(
                starter(name="Starter Warehouse", warehouse_id="doc"),
                starter(name=STARTER, warehouse_id="ws"),
            )
        }
    )

    fields = voclab.starter_warehouse_resize(client, "2X-Small")

    assert fields["starter_warehouse_id"] == "ws"


def test_a_misspelled_size_is_caught_before_the_api_sees_it():
    # A 400 from Databricks names the field and not the course.env line that
    # holds the typo, and the sizes are case sensitive.
    client, http = workspace({WAREHOUSES_PATH: warehouses(starter())})

    with pytest.raises(voclab.VoclabError) as failure:
        voclab.starter_warehouse_resize(client, "2x-small")

    assert failure.value.code == "INVALID_WAREHOUSE_SIZE"
    assert http.paths("GET") == []


def test_naming_no_size_leaves_the_starter_warehouse_alone(monkeypatch):
    # This is the one object a course cannot decline, so a course that never
    # set the key gets no edit rather than an edit it did not ask for.
    monkeypatch.setenv("VOC_DB_WORKSPACE_URL", HOST)
    monkeypatch.setenv("VOC_DB_TOKEN", "dapi-test-token")
    monkeypatch.delenv(voclab.STARTER_WAREHOUSE_SIZE_VAR, raising=False)
    monkeypatch.delenv("VOC_DB_USER_EMAIL", raising=False)
    client, http = workspace({WAREHOUSES_PATH: warehouses(starter())})
    monkeypatch.setattr(voclab, "build_workspace", lambda args: client)

    fields = voclab.run(voclab.build_parser().parse_args(["starter-warehouse"]))

    assert fields["starter_warehouse_action"] == "skipped"
    assert http.calls == []


def test_the_starter_warehouse_needs_no_student_to_exist(monkeypatch):
    # Same reason as warehouse-ensure: workspace_init.sh has no
    # VOC_DB_USER_EMAIL, and this object is one per workspace.
    monkeypatch.setenv("VOC_DB_WORKSPACE_URL", HOST)
    monkeypatch.setenv("VOC_DB_TOKEN", "dapi-test-token")
    monkeypatch.setenv(voclab.STARTER_WAREHOUSE_SIZE_VAR, "2X-Small")
    monkeypatch.delenv(voclab.STARTER_WAREHOUSE_NAME_VAR, raising=False)
    monkeypatch.delenv("VOC_DB_USER_EMAIL", raising=False)
    client, _ = workspace(
        {
            WAREHOUSES_PATH: warehouses(starter()),
            "/api/2.0/sql/warehouses/d265/edit": {},
        }
    )
    monkeypatch.setattr(voclab, "build_workspace", lambda args: client)

    fields = voclab.run(voclab.build_parser().parse_args(["starter-warehouse"]))

    assert fields["starter_warehouse_action"] == "resized"
