"""Offline checks for the probe classifier, the status map, and the guards.

Kept in the shipped package rather than in ``tests/`` because ``--self-test`` has
to run inside a lab environment, where the test suite is not installed and no
Databricks call is allowed. The pytest suite imports the same case tables, so the
two cannot drift: a case added here is checked in both places.

Every case is data. ``run_self_test`` walks the tables and prints a line per
case, because the transcript is the artifact someone pastes into a ticket.
"""

from __future__ import annotations

from .databricks import (
    GRADE_SCRIPT,
    LIFECYCLE_SCRIPTS,
    DatabricksHostError,
    find_field,
    normalize_host,
    workspace_id_from_url,
)
from .output import section
from .probe import (
    CATALOG,
    EXIT_OK,
    EXIT_SELF_TEST,
    NEXT_ACTION,
    TRACKER_STATUS,
    ApiOperation,
    Config,
    GuardError,
    Probe,
    ProbeResult,
    Verdict,
    classify,
    guard_credentials,
    is_read_only,
    looks_empty,
)

# Codes whose meaning does not depend on the message body.
CLASSIFIER_CASES: tuple[tuple[str, str, Verdict], ...] = (
    (
        "PERMISSION_DENIED",
        "User does not have MODIFY on cluster 0101-123456-abcdefgh",
        Verdict.PERMISSION_DENIED,
    ),
    (
        "UNAUTHENTICATED",
        "Invalid access token.",
        Verdict.UNAUTHENTICATED,
    ),
    (
        "RESOURCE_DOES_NOT_EXIST",
        "Cluster 0000-000000-doesnotexist does not exist",
        Verdict.NOT_FOUND_OR_HIDDEN,
    ),
    # Unity Catalog names the absent thing in the code instead of sending one
    # generic absence. All three messages below are verbatim from a live
    # workspace. Pinned because an absent catalog reaching
    # UNCLASSIFIED is how the ship gate stopped being able to say "rebuild this"
    # and started saying "the call failed, go look".
    (
        "CATALOG_DOES_NOT_EXIST",
        "Catalog 'databricks-neo4j-workshop' does not exist.",
        Verdict.NOT_FOUND_OR_HIDDEN,
    ),
    (
        "SCHEMA_DOES_NOT_EXIST",
        "Schema 'samples.nope' does not exist.",
        Verdict.NOT_FOUND_OR_HIDDEN,
    ),
    (
        "TABLE_DOES_NOT_EXIST",
        "Table 'samples.nyctaxi.nope' does not exist.",
        Verdict.NOT_FOUND_OR_HIDDEN,
    ),
    (
        "ENDPOINT_NOT_FOUND",
        "No API found for 'GET /api/2.0/clusters/lst'",
        Verdict.UNKNOWN_ENDPOINT,
    ),
    (
        "FEATURE_DISABLED",
        "Serverless compute is not enabled for this workspace.",
        Verdict.FEATURE_DISABLED,
    ),
    (
        "INVALID_PARAMETER_VALUE",
        "Missing required field: cluster_id",
        Verdict.INCONCLUSIVE_BAD_INPUT,
    ),
    (
        "MALFORMED_REQUEST",
        "Could not parse request object.",
        Verdict.INCONCLUSIVE_BAD_INPUT,
    ),
    (
        "REQUEST_LIMIT_EXCEEDED",
        "Your request was rejected due to API rate limiting.",
        Verdict.THROTTLED,
    ),
    (
        "QUOTA_EXCEEDED",
        "The workspace has reached its limit of concurrent clusters.",
        Verdict.QUOTA_EXCEEDED,
    ),
    (
        "RESOURCE_EXHAUSTED",
        "Cloud provider capacity is unavailable in this region.",
        Verdict.QUOTA_EXCEEDED,
    ),
    (
        "METASTORE_DOES_NOT_EXIST",
        "No metastore was found.",
        Verdict.METASTORE_NOT_ASSIGNED,
    ),
    (
        "SOME_BRAND_NEW_CODE",
        "Nobody has seen this before.",
        Verdict.UNCLASSIFIED,
    ),
)

# PERMISSION_DENIED covers three grants made by three different admins in three
# different consoles, and only the message body says which one is missing.
# Collapsing them would turn an actionable result into a shrug.
DENIAL_CASES: tuple[tuple[str, Verdict], ...] = (
    (
        "PERMISSION_DENIED: User does not have USE CATALOG on Catalog 'main'.",
        Verdict.UNITY_CATALOG_DENIED,
    ),
    (
        "User does not have USE SCHEMA on Schema 'main.workshop'.",
        Verdict.UNITY_CATALOG_DENIED,
    ),
    (
        (
            "User does not have SELECT on Table 'main.workshop.customers'. "
            "Grant it on the metastore."
        ),
        Verdict.UNITY_CATALOG_DENIED,
    ),
    (
        "This workspace has no Unity Catalog metastore assigned.",
        Verdict.METASTORE_NOT_ASSIGNED,
    ),
    (
        "Missing entitlement: allow-cluster-create",
        Verdict.ENTITLEMENT_MISSING,
    ),
    (
        "User lacks the workspace-access entitlement for this workspace.",
        Verdict.ENTITLEMENT_MISSING,
    ),
    (
        "User does not have CAN_MANAGE on secret scope 'neo4j-aura'.",
        Verdict.PERMISSION_DENIED,
    ),
    (
        "Something unfamiliar was refused.",
        Verdict.PERMISSION_DENIED,
    ),
)

# Verdict to tracker Status, checked so the two tables cannot drift apart.
STATUS_CASES: tuple[tuple[Verdict, str], ...] = (
    (Verdict.OK, "Allowed"),
    (Verdict.ALLOWED_EMPTY, "Allowed"),
    (Verdict.PERMISSION_DENIED, "Denied (ACL)"),
    (Verdict.UNITY_CATALOG_DENIED, "Denied (UC)"),
    (Verdict.ENTITLEMENT_MISSING, "Denied (entitlement)"),
    (Verdict.METASTORE_NOT_ASSIGNED, "Blocked"),
    # A resource that is absent and a resource that is invisible are the same
    # response, so this cannot be recorded as Allowed the way the AWS
    # equivalent is.
    (Verdict.NOT_FOUND_OR_HIDDEN, "Inconclusive"),
    (Verdict.INCONCLUSIVE_BAD_INPUT, "Inconclusive"),
    (Verdict.UNCLASSIFIED, "Untested"),
)

HOST_CASES: tuple[tuple[str, str], ...] = (
    (
        "https://dbc-33333333-3333.cloud.databricks.com",
        "https://dbc-33333333-3333.cloud.databricks.com",
    ),
    (
        "https://dbc-33333333-3333.cloud.databricks.com/",
        "https://dbc-33333333-3333.cloud.databricks.com",
    ),
    (
        "  https://dbc-33333333-3333.cloud.databricks.com/?o=3333333333333333  ",
        "https://dbc-33333333-3333.cloud.databricks.com",
    ),
    (
        "dbc-33333333-3333.cloud.databricks.com",
        "https://dbc-33333333-3333.cloud.databricks.com",
    ),
)

BAD_HOST_CASES: tuple[str, ...] = (
    "",
    "   ",
    "http://dbc-33333333-3333.cloud.databricks.com",
    "https://",
    # Anything before the first slash is a netloc as far as urlsplit is
    # concerned, so these normalize into a plausible-looking host unless the
    # hostname is checked. They fail at the SDK rather than here if it is not.
    "not a url",
    "https://dbc 33333333.cloud.databricks.com",
    "https://dbc-33333333-3333.cloud.databricks.com:notaport",
)

WORKSPACE_ID_CASES: tuple[tuple[str, str | None], ...] = (
    (
        (
            "https://dbc-33333333-3333.cloud.databricks.com/?o=3333333333333333"
            "#notebook/12345"
        ),
        "3333333333333333",
    ),
    ("https://dbc-33333333-3333.cloud.databricks.com/", None),
    ("https://dbc-33333333-3333.cloud.databricks.com/?o=", None),
    ("https://dbc-33333333-3333.cloud.databricks.com/?o=notanumber", None),
)

# A Databricks list response with nothing in it arrives in more than one shape,
# and every one of them has to read as empty rather than as a populated result.
EMPTY_CASES: tuple[tuple[str, object, bool], ...] = (
    ("absent key", {}, True),
    ("empty list", {"clusters": []}, True),
    ("null response", None, True),
    ("one cluster", {"clusters": [{"cluster_id": "x"}]}, False),
    ("scalar only", {"enableTokensConfig": "true"}, False),
)

SAFETY_CASES: tuple[tuple[str, str, bool], ...] = (
    ("clusters:List", "GET", True),
    ("workspace:GetStatus", "GET", True),
    ("clusters:Create", "POST", False),
    ("secrets:CreateScope", "POST", False),
    ("workspace:Delete", "DELETE", False),
)

# Every way a credential can be present in name and absent in fact. The SDK
# reads each of these as "not supplied" and falls through its provider chain.
SP_GUARD_CASES: tuple[tuple[str, str, str], ...] = (
    ("both empty", "", ""),
    ("empty secret", "11111111-2222-4333-8444-555555555555", ""),
    ("whitespace secret", "11111111-2222-4333-8444-555555555555", "   "),
    ("empty client id", "", "dose1234567890abcdef"),
)

# Verdicts no classifier case can produce because nothing in classify() emits
# them. The harness around it raises them instead, so they are exempt from the
# reachability check rather than missing from it.
VERDICTS_OUTSIDE_CLASSIFY = frozenset(
    {
        Verdict.OK,
        Verdict.ALLOWED_EMPTY,
        Verdict.ENDPOINT_UNREACHABLE,
        Verdict.UNKNOWN_OPERATION,
    }
)


def make_config(
    *,
    source: str = "service-principal",
    client_id: str = "11111111-2222-4333-8444-555555555555",
    client_secret: str = "dose1234567890abcdef",
    lab_user_token: str = "",
) -> Config:
    """Build a :class:`Config` with no side effects, for guard checks."""
    return Config(
        host="https://dbc-33333333-3333.cloud.databricks.com",
        account_host="https://accounts.cloud.databricks.com",
        account_id="aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        client_id=client_id,
        client_secret=client_secret,
        lab_user_token=lab_user_token,
        expected_workspace_id="3333333333333333",
        source=source,
    )


def unreachable_verdicts() -> list[Verdict]:
    """Return every verdict that no classifier case can produce.

    A verdict nothing can emit is a lie in the vocabulary: it reads as a case
    that gets handled when in fact that outcome would fall through to
    ``UNCLASSIFIED``.
    """
    produced = {expected for _, _, expected in CLASSIFIER_CASES}
    produced |= {expected for _, expected in DENIAL_CASES}
    produced |= VERDICTS_OUTSIDE_CLASSIFY
    return [verdict for verdict in Verdict if verdict not in produced]


def verdicts_without_status() -> list[Verdict]:
    """Return every verdict the tracker status map does not cover."""
    return [verdict for verdict in Verdict if verdict not in TRACKER_STATUS]


def verdicts_without_next_action() -> list[Verdict]:
    """Return every verdict with no next step written for it."""
    return [verdict for verdict in Verdict if verdict not in NEXT_ACTION]


def mutating_catalog_entries() -> list[str]:
    """Return every catalog entry whose method is not a read.

    The catalog is the list that ``--all`` runs, so anything in it that writes is
    a cluster somebody starts by accident.
    """
    return [
        name
        for name, operation in CATALOG.items()
        if not is_read_only(operation.method)
    ]


def run_self_test() -> int:
    """Check the classifier, the tables, and the guards with no Databricks call."""
    failures: list[str] = []

    section("SELF-TEST 1: error classification")
    for code, message, expected in CLASSIFIER_CASES:
        verdict = classify(code, message)
        ok = verdict is expected
        print(f"  {'PASS' if ok else 'FAIL'}  {code:28s} -> {verdict}")
        if not ok:
            failures.append(f"{code} classified as {verdict}, expected {expected}")

    section("SELF-TEST 2: which grant is missing, inside one denial code")
    for message, expected in DENIAL_CASES:
        verdict = classify("PERMISSION_DENIED", message)
        ok = verdict is expected
        excerpt = message[:52]
        print(f"  {'PASS' if ok else 'FAIL'}  {excerpt:54s} -> {verdict}")
        if not ok:
            failures.append(f"denial {excerpt!r} gave {verdict}, expected {expected}")

    section("SELF-TEST 3: verdict to tracker status")
    for verdict, expected_status in STATUS_CASES:
        actual = ProbeResult("x", verdict, "").status
        ok = actual == expected_status
        print(f"  {'PASS' if ok else 'FAIL'}  {verdict:24s} -> {actual}")
        if not ok:
            failures.append(f"{verdict} mapped to {actual}, expected {expected_status}")

    missing_status = verdicts_without_status()
    if missing_status:
        print(f"  FAIL  verdicts with no tracker status: {missing_status}")
        failures.append(f"verdicts missing from TRACKER_STATUS: {missing_status}")
    else:
        print(f"  PASS  every one of {len(list(Verdict))} verdicts has a status")

    missing_action = verdicts_without_next_action()
    if missing_action:
        print(f"  FAIL  verdicts with no next step: {missing_action}")
        failures.append(f"verdicts missing from NEXT_ACTION: {missing_action}")
    else:
        print("  PASS  every verdict has a next step written for it")

    unreachable = unreachable_verdicts()
    if unreachable:
        print(f"  FAIL  verdicts nothing can emit: {unreachable}")
        failures.append(f"unreachable verdicts: {unreachable}")
    else:
        print("  PASS  every verdict is reachable")

    section("SELF-TEST 4: host normalization")
    for raw, expected_host in HOST_CASES:
        actual = normalize_host(raw)
        ok = actual == expected_host
        print(f"  {'PASS' if ok else 'FAIL'}  {raw.strip()[:48]:50s} -> {actual}")
        if not ok:
            failures.append(f"{raw!r} normalized to {actual}, expected {expected_host}")

    for raw in BAD_HOST_CASES:
        try:
            normalize_host(raw)
        except DatabricksHostError:
            print(f"  PASS  {raw!r:52s} refused")
        else:
            print(f"  FAIL  {raw!r:52s} was accepted")
            failures.append(f"normalize_host accepted {raw!r}")

    for url, expected_id in WORKSPACE_ID_CASES:
        actual_id = workspace_id_from_url(url)
        ok = actual_id == expected_id
        print(f"  {'PASS' if ok else 'FAIL'}  o= from {url[:44]:46s} -> {actual_id}")
        if not ok:
            failures.append(
                f"{url} gave workspace id {actual_id}, expected {expected_id}"
            )

    section("SELF-TEST 5: an empty result is not an absent workspace")
    for label, response, expected_empty in EMPTY_CASES:
        actual_empty = looks_empty(response)
        ok = actual_empty is expected_empty
        print(f"  {'PASS' if ok else 'FAIL'}  {label:16s} empty={actual_empty}")
        if not ok:
            failures.append(
                f"{label} read as empty={actual_empty}, expected {expected_empty}"
            )

    section("SELF-TEST 6: credential guard refuses blank fields")
    for label, client_id, client_secret in SP_GUARD_CASES:
        config = make_config(client_id=client_id, client_secret=client_secret)
        try:
            guard_credentials(config)
        except GuardError:
            print(f"  PASS  {label:20s} refused, nothing reported")
        else:
            print(f"  FAIL  {label:20s} was accepted")
            failures.append(f"credential guard accepted {label}")

    try:
        guard_credentials(make_config())
    except GuardError:
        print("  FAIL  a complete OAuth pair was refused")
        failures.append("credential guard refused a complete OAuth pair")
    else:
        print("  PASS  a complete OAuth pair was accepted")

    try:
        guard_credentials(make_config(source="token", lab_user_token=""))
    except GuardError:
        print("  PASS  an empty lab-user token refused")
    else:
        print("  FAIL  an empty lab-user token was accepted")
        failures.append("credential guard accepted an empty lab-user token")

    section("SELF-TEST 7: probe safety classification")
    for name, method, expected_safe in SAFETY_CASES:
        probe = Probe(name=name, operation=ApiOperation(method, "/api/2.0/x"))
        actual_safe = probe.is_safe()
        ok = actual_safe is expected_safe
        mark = "PASS" if ok else "FAIL"
        print(f"  {mark}  {name:24s} {method:6s} safe={actual_safe}")
        if not ok:
            failures.append(
                f"{name} safety was {actual_safe}, expected {expected_safe}"
            )

    mutating = mutating_catalog_entries()
    if mutating:
        print(f"  FAIL  the built-in catalog holds writes: {mutating}")
        failures.append(f"catalog entries that are not reads: {mutating}")
    else:
        print(f"  PASS  all {len(CATALOG)} catalog operations are reads")

    section("SELF-TEST 8: the lifecycle contract")
    order = [script.name for script in LIFECYCLE_SCRIPTS]
    expected_order = [
        "workspace_init.sh",
        "lab_setup.sh",
        "user_setup.sh",
        "lab_end.sh",
    ]
    if order == expected_order:
        print(f"  PASS  execution order is {' -> '.join(order)}")
    else:
        print(f"  FAIL  execution order is {order}, expected {expected_order}")
        failures.append(f"lifecycle order is {order}, expected {expected_order}")

    required = [script.name for script in LIFECYCLE_SCRIPTS if script.required]
    if required == ["user_setup.sh"]:
        print("  PASS  user_setup.sh is the only required script")
    else:
        print(f"  FAIL  required scripts are {required}, expected ['user_setup.sh']")
        failures.append(f"required scripts are {required}")

    # Only the two scripts that run while a student is waiting can redirect
    # that student, so only those two are handed VOC_IPC_DATA_FILE. A lab that
    # tries to write the notebook URL from workspace_init.sh or lab_end.sh has
    # nowhere to write it.
    ipc_writers = [script.name for script in LIFECYCLE_SCRIPTS if script.writes_ipc]
    if ipc_writers == ["lab_setup.sh", "user_setup.sh"]:
        print("  PASS  only lab_setup.sh and user_setup.sh receive the IPC file")
    else:
        print(f"  FAIL  IPC writers are {ipc_writers}")
        failures.append(f"IPC writers are {ipc_writers}")

    if GRADE_SCRIPT.name not in order:
        print("  PASS  grade.sh is held apart from the lifecycle")
    else:
        print("  FAIL  grade.sh is in the lifecycle sequence")
        failures.append("grade.sh appears in LIFECYCLE_SCRIPTS")

    section("SELF-TEST 9: reading an undocumented session record")
    record = {
        "state": "running",
        "databricks": {
            "workspace_url": "https://dbc-33333333-3333.cloud.databricks.com",
            "userEmail": "student1@labs.vocareum.com",
        },
        "labs": [{"group_name": "voc-group-1"}],
        "blank": "",
    }
    field_cases: tuple[tuple[str, str | None], ...] = (
        ("workspace_url", "databricks.workspace_url"),
        ("user_email", "databricks.userEmail"),
        ("group_name", "labs[0].group_name"),
        ("notebook_url", None),
        ("blank", None),
    )
    for fragment, expected_path in field_cases:
        found = find_field(record, fragment)
        actual_path = found[0] if found else None
        ok = actual_path == expected_path
        print(f"  {'PASS' if ok else 'FAIL'}  {fragment:14s} -> {actual_path}")
        if not ok:
            failures.append(
                f"{fragment} found at {actual_path}, expected {expected_path}"
            )

    section("SELF-TEST RESULT")
    if failures:
        print(f"{len(failures)} failure(s):")
        for item in failures:
            print(f"  - {item}")
        return EXIT_SELF_TEST
    print("All self-tests passed. No Databricks call was made.")
    return EXIT_OK
