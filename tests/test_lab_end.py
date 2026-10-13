"""What the template's ``lab_end.sh`` reclaims, per ``VOC_END_LAB_BEHAVIOR``.

These tests run the hook itself under ``bash`` rather than reasoning about it,
because the defect they cover was invisible to reading. The hook read the
behavior with a ``:-unset`` default, so it survived an unset variable rather
than dying on it under ``set -euo pipefail``, fell into the unrecognized branch,
reclaimed nothing, and exited 0. Nothing in the record said so. A student
cluster was measured still RUNNING after its session record read Terminated,
and ``GET .../parts/{id}`` carries no End lab behavior field under any spelling,
so unset and empty are the cases to test first rather than the exotic ones.

Offline like every test here. ``voclab.py`` is replaced by a stub that records
the subcommand it was handed and prints the ``key=value`` lines the real one
prints, so what is exercised is the shell: the branch, the default, and what
reaches ``voccustomdata.txt``.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from dbx_vocareum_tools.databricks import (
    END_LAB_BEHAVIOR_COMMANDS,
    END_LAB_BEHAVIOR_FALLBACK,
    END_LAB_BEHAVIORS,
)
from dbx_vocareum_tools.labruntime import RUNTIME_DIR, TEMPLATE_DIR

# The two halves the upload puts side by side in /voc/scripts. The hook is
# template, so a course owns its copy; voclib.sh is runtime and no course has
# one. Assembling the run directory from both is what this test has to do
# because it is what dbx-vocareum-upload does.
HOOK_SOURCES = ((TEMPLATE_DIR, "lab_end.sh"), (RUNTIME_DIR, "voclib.sh"))

# /bin/bash on macOS is 3.2, which is the oldest shell these hooks have to run
# under and the one that has already caught this repository out once, with
# `declare -g` in voclib.sh. So it is preferred over whatever newer bash is on
# PATH, and PATH is the fallback for hosts where /bin/bash is not there.
BASH = "/bin/bash" if Path("/bin/bash").exists() else shutil.which("bash") or "bash"

# Enough of voclab.py to exercise the shell side. It records the argv it was
# given so a test can assert which subcommand ran, and prints the two fields the
# hook reads back through voc_voclab.
STUB_VOCLAB = """\
import json
import sys

with open("argv.json", "w") as handle:
    json.dump(sys.argv[1:], handle)

print("cluster_name=student1_200000")
print("cluster_action=terminated")
"""

# The same stub, failing the way a Databricks error does: an error_code and a
# message on stdout, and a non-zero exit.
STUB_VOCLAB_FAILING = """\
import json
import sys

with open("argv.json", "w") as handle:
    json.dump(sys.argv[1:], handle)

print("error_code=CLUSTER_STOP_FAILED")
print("message=the workspace refused the call")
sys.exit(2)
"""


def build_lab(tmp_path: Path, stub: str = STUB_VOCLAB) -> Path:
    """Lay out the hook, the library it sources, and a stub voclab.py."""
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    for directory, name in HOOK_SOURCES:
        shutil.copy(directory / name, run_dir / name)
    (run_dir / "voclab.py").write_text(stub)
    return run_dir


def run_hook(run_dir: Path, behavior=None, user_email="student1_200000@vocareum.com"):
    """Run lab_end.sh in ``run_dir`` and return its result.

    ``behavior`` of ``None`` means the variable is not in the environment at all,
    which is a different case from an empty string and the one ``set -u`` makes
    awkward to ask about.
    """
    env = {
        # Only what this hook is documented to be handed, plus PATH, because the
        # hook shells out to date, sed, awk, tr, and python3.
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(run_dir),
        "VOC_DB_WORKSPACE_URL": "https://dbc-22222222-2222.cloud.databricks.com/?o=1",
        "VOC_DB_GROUP_NAME": "Group-lab-1-2-3",
        "VOC_CUSTOM_DATA": '{"script": "user_setup.sh"}',
    }
    if user_email is not None:
        env["VOC_DB_USER_EMAIL"] = user_email
    if behavior is not None:
        env["VOC_END_LAB_BEHAVIOR"] = behavior

    completed = subprocess.run(
        [BASH, "./lab_end.sh"],
        cwd=run_dir,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    custom_path = run_dir / "voccustomdata.txt"
    custom = json.loads(custom_path.read_text()) if custom_path.exists() else None
    argv_path = run_dir / "argv.json"
    argv = json.loads(argv_path.read_text()) if argv_path.exists() else None
    return completed, custom, argv


@pytest.mark.parametrize(
    ("behavior", "status"),
    [
        (None, "unset"),
        ("", "empty"),
        ("Suspend", "unrecognized"),
        ("stopp", "unrecognized"),
    ],
)
def test_unknown_behavior_still_reclaims_the_compute(tmp_path, behavior, status):
    """The bug this file exists for. Not one of the two words, still reclaimed.

    The old branch logged "nothing is removed" and exited 0 for all four of
    these, which is what leaves a cluster billing after the student has left.
    """
    run_dir = build_lab(tmp_path)
    completed, custom, argv = run_hook(run_dir, behavior=behavior)

    assert completed.returncode == 0
    assert argv == ["cluster-stop", "--user", "student1_200000@vocareum.com"]
    assert custom["end_behavior_status"] == status
    assert custom["cleanup_command"] == "cluster-stop"
    assert custom["cluster_action"] == "terminated"
    assert custom["cleanup_error"] == "none"


@pytest.mark.parametrize(
    ("behavior", "command"),
    [("stop", "cluster-stop"), ("terminate", "cluster-remove")],
)
def test_the_two_documented_words_are_unchanged(tmp_path, behavior, command):
    """A stop keeps the configuration, a terminate removes it. As before."""
    run_dir = build_lab(tmp_path)
    completed, custom, argv = run_hook(run_dir, behavior=behavior)

    assert completed.returncode == 0
    assert argv[0] == command
    assert custom["end_behavior_status"] == behavior
    assert custom["cleanup_command"] == command


@pytest.mark.parametrize("behavior", ["Terminate", "TERMINATE", " terminate "])
def test_terminate_is_matched_case_and_whitespace_insensitively(tmp_path, behavior):
    """Vocareum's exact spelling is not something this hook gets to assume.

    A value that means terminate but arrives capitalized or padded has to reach
    the terminate branch, because defaulting it would keep a cluster the
    environment is about to revert anyway.
    """
    run_dir = build_lab(tmp_path)
    completed, custom, argv = run_hook(run_dir, behavior=behavior)

    assert completed.returncode == 0
    assert argv[0] == "cluster-remove"
    assert custom["end_behavior_status"] == "terminate"


@pytest.mark.parametrize("behavior", [None, "", "Suspend", "stop", " terminate "])
def test_the_value_that_arrived_is_recorded_verbatim(tmp_path, behavior):
    """The field that would have made this diagnosis immediate.

    ``end_lab_behavior`` carries what Vocareum sent and nothing else, so a later
    reader sees the value rather than inferring it from which branch ran. An
    unset variable and an empty one are both recorded as empty here, which is
    why ``end_behavior_status`` separates them.
    """
    run_dir = build_lab(tmp_path)
    _, custom, _ = run_hook(run_dir, behavior=behavior)

    assert custom["end_lab_behavior"] == (behavior or "")


def test_a_missing_user_email_is_recorded_not_failed(tmp_path):
    """No identity means no cluster name, which is nothing to act on.

    Recorded rather than raised, because the student has already left and a
    failed lab end is not something anyone can act on.
    """
    run_dir = build_lab(tmp_path)
    completed, custom, argv = run_hook(run_dir, behavior="stop", user_email=None)

    assert completed.returncode == 0
    assert argv is None
    assert custom["cleanup_error"] == "MISSING_USER_EMAIL"
    assert custom["cluster_action"] == "none"
    assert custom["user_email"] == "unset"


def test_a_failed_reclaim_is_recorded_not_failed(tmp_path):
    """A leftover cluster has to be traceable to the run that left it."""
    run_dir = build_lab(tmp_path, stub=STUB_VOCLAB_FAILING)
    completed, custom, argv = run_hook(run_dir, behavior=None)

    assert completed.returncode == 0
    assert argv[0] == "cluster-stop"
    assert custom["cleanup_error"] == "CLUSTER_STOP_FAILED"
    assert custom["cluster_action"] == "none"


def test_the_custom_data_is_one_json_object_with_the_failure_fields_first(tmp_path):
    """``ran_at`` and ``script`` lead, whatever else the record carries.

    ``voc_custom_write`` truncates, so a record naming no script cannot be told
    apart from a hook Vocareum never invoked.
    """
    run_dir = build_lab(tmp_path)
    _, custom, _ = run_hook(run_dir, behavior="stop")

    assert list(custom)[:2] == ["ran_at", "script"]
    assert custom["script"] == "lab_end.sh"


def test_the_hook_default_is_the_contract_default(tmp_path):
    """The hook and ``databricks.py`` have to name the same fallback.

    The constant is what documentation and diagnosis read; the hook is what
    runs. Asserting the pair here is what keeps the two from drifting.
    """
    assert END_LAB_BEHAVIOR_FALLBACK in END_LAB_BEHAVIORS
    run_dir = build_lab(tmp_path)
    _, _, argv = run_hook(run_dir, behavior=None)

    assert argv[0] == END_LAB_BEHAVIOR_COMMANDS[END_LAB_BEHAVIOR_FALLBACK]


def test_the_fallback_reclaims_without_deleting():
    """Cost is reclaimed; data is not destroyed. That is the whole choice.

    Stated as a test rather than only as a comment because the reverse default
    is the one that reads as safer and is not: a cluster left RUNNING bills
    until somebody notices, and nobody is paged for it.
    """
    assert END_LAB_BEHAVIOR_FALLBACK == "stop"
    assert END_LAB_BEHAVIOR_COMMANDS[END_LAB_BEHAVIOR_FALLBACK] == "cluster-stop"
