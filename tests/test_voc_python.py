"""How a course runs a program of its own.

``voclab.py`` is the runtime's, and a course does not edit it. So a course that
provisions anything the runtime does not know about, a catalog of its own shape,
a volume, a pipeline, has a second program to run, and ``voc_python`` is how a
hook runs it and reads its ``key=value`` output back.

The property under test is the variable prefix. Two programs sharing one
namespace means a hook can read a field the other program set, and it reads as a
success rather than as a failure, which is the worst way for this to go wrong.
So the prefix comes from the program's name and each program's results are its
own.

Offline, and under ``/bin/bash`` for the same reason ``test_voclib.py`` is: that
is bash 3.2 on macOS, the oldest shell these hooks have to survive, and the
uppercasing and the character class in ``voc_python`` are both written around
what it does not have.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from dbx_vocareum_tools.labruntime import RUNTIME_DIR

BASH = "/bin/bash" if Path("/bin/bash").exists() else shutil.which("bash") or "bash"

# Two programs answering on the contract: narration to stderr, key=value to
# stdout. Neither writes voccustomdata.txt, which is the calling hook's file.
VOCLAB = """\
import sys

print("running", file=sys.stderr)
print("cluster_name=labuser10000003")
"""

WORKSHOP = """\
import sys

print("provisioning", file=sys.stderr)
print("catalog=databricks-neo4j-workshop")
print("data_files_uploaded=27")
"""


def build(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    shutil.copy(RUNTIME_DIR / "voclib.sh", run_dir / "voclib.sh")
    (run_dir / "voclab.py").write_text(VOCLAB)
    (run_dir / "workshop.py").write_text(WORKSHOP)
    return run_dir


def run_hook(run_dir: Path, body: str) -> subprocess.CompletedProcess:
    script = f'VOC_SCRIPT_NAME="hook.sh"\n. ./voclib.sh\n{body}'
    (run_dir / "hook.sh").write_text(script)
    return subprocess.run(
        [BASH, "./hook.sh"],
        cwd=run_dir,
        env={
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(run_dir),
            "VOC_CUSTOM_DATA": "{}",
        },
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_a_courses_own_program_answers_under_its_own_name(tmp_path):
    run_dir = build(tmp_path)

    result = run_hook(
        run_dir,
        "voc_python workshop.py provision\n"
        'printf "%s|%s\\n" "$workshop_catalog" "$workshop_data_files_uploaded"\n',
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "databricks-neo4j-workshop|27"


def test_voc_voclab_still_answers_under_voclab(tmp_path):
    # The wrapper exists so that every hook already written keeps working. A
    # rename here would be a change to four hooks in every course that has ever
    # been scaffolded, for no gain.
    run_dir = build(tmp_path)

    result = run_hook(
        run_dir, 'voc_voclab cluster-ensure\nprintf "%s\\n" "$voclab_cluster_name"\n'
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "labuser10000003"


def test_one_program_does_not_clear_or_claim_anothers_results(tmp_path):
    # The reason the prefix is derived rather than shared. voc_python clears
    # stale variables so a second call cannot be read through the first one's
    # values, and that clearing has to stop at the program's own prefix.
    run_dir = build(tmp_path)

    result = run_hook(
        run_dir,
        "voc_voclab cluster-ensure\n"
        "voc_python workshop.py provision\n"
        'printf "%s|%s\\n" "$voclab_cluster_name" "$workshop_catalog"\n',
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "labuser10000003|databricks-neo4j-workshop"


def test_every_program_gets_the_two_fields_a_failure_is_read_through(tmp_path):
    # voc_fail needs an error_code and a message whatever happened, including
    # when the program produced no output at all. Under set -u an unset variable
    # is a hook that dies with nothing written, which is the unexplained failure
    # this whole boundary exists to prevent.
    run_dir = build(tmp_path)

    result = run_hook(
        run_dir,
        "set -u\n"
        "voc_python workshop.py provision\n"
        'printf "[%s][%s]\\n" "$workshop_error_code" "$workshop_message"\n',
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "[][]"


def test_a_missing_program_names_itself_in_the_error_code(tmp_path):
    # MISSING_VOCLAB_PY was the code before this generalized, and voclab.py has
    # to keep producing it: it is in the record of every run that hit it. A
    # course's program produces the same shape under its own name, so a reader
    # of voccustomdata.txt learns which file was absent.
    run_dir = build(tmp_path)
    (run_dir / "workshop.py").unlink()

    result = run_hook(run_dir, "voc_python workshop.py provision\n")

    assert result.returncode != 0
    written = (run_dir / "voccustomdata.txt").read_text()
    assert "MISSING_WORKSHOP_PY" in written
    assert "workshop.py" in written


def test_voclab_keeps_the_error_code_it_has_always_written(tmp_path):
    run_dir = build(tmp_path)
    (run_dir / "voclab.py").unlink()

    result = run_hook(run_dir, "voc_voclab cluster-ensure\n")

    assert result.returncode != 0
    assert "MISSING_VOCLAB_PY" in (run_dir / "voccustomdata.txt").read_text()
