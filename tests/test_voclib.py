"""How ``voclib.sh`` gets a course's values to the program that reads them.

``course.env`` is the seam between a course-agnostic package and one course. The
hooks source it, ``voclab.py`` reads it out of its own environment, and those are
two processes: a value set but not exported is visible to the hook and invisible
to the program the hook exists to run, which is a course whose configuration is
silently ignored on the one call that uses it. So these tests run the real shell
and look at what a *child* process sees.

Offline, and under ``/bin/bash`` for the same reason ``test_lab_end.py`` is: that
is bash 3.2 on macOS, the oldest shell these hooks have to survive, and it has
caught this repository out once already.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

from dbx_vocareum_tools.labruntime import COURSE_ENV_FILE, RUNTIME_DIR

BASH = "/bin/bash" if Path("/bin/bash").exists() else shutil.which("bash") or "bash"

# A child process printing one variable. Sourcing voclib.sh has to make the value
# reach here, not merely reach the shell that sourced it.
CHILD = """\
import os
import sys

print(os.environ.get(sys.argv[1], "<unset>"))
"""


def build(tmp_path: Path, course_env: str | None) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    shutil.copy(RUNTIME_DIR / "voclib.sh", run_dir / "voclib.sh")
    (run_dir / "child.py").write_text(CHILD)
    if course_env is not None:
        (run_dir / COURSE_ENV_FILE).write_text(course_env)
    return run_dir


def read_in_child(run_dir: Path, name: str) -> subprocess.CompletedProcess:
    script = f'. ./voclib.sh\npython3 ./child.py "{name}"\n'
    (run_dir / "probe.sh").write_text(script)
    return subprocess.run(
        [BASH, "./probe.sh"],
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


def test_a_course_value_reaches_the_program_the_hook_runs(tmp_path):
    run_dir = build(tmp_path, "VOC_COURSE_CATALOG=workshop\n")

    result = read_in_child(run_dir, "VOC_COURSE_CATALOG")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "workshop"


def test_no_course_env_is_defaults_and_not_a_failure(tmp_path):
    # A course that names nothing gets voclab.py's defaults, which are a working
    # single-node cluster with no libraries and no notebooks. Logged rather than
    # silent, because "never loaded" and "set to this" read very differently.
    run_dir = build(tmp_path, None)

    result = read_in_child(run_dir, "VOC_COURSE_CATALOG")

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "<unset>"
    assert COURSE_ENV_FILE in result.stderr


def test_a_value_holding_spaces_survives_the_trip(tmp_path):
    # The library list is the case that matters: comma separated, and a course
    # writes it with spaces after the commas because that is how it reads.
    run_dir = build(
        tmp_path,
        'VOC_COURSE_LIBRARIES="pypi:neo4j==6.2.0, pypi:pandas"\n',
    )

    result = read_in_child(run_dir, "VOC_COURSE_LIBRARIES")

    assert result.stdout.strip() == "pypi:neo4j==6.2.0, pypi:pandas"


def test_a_course_env_that_will_not_source_stops_the_hook(tmp_path):
    # Carrying on would provision something the course did not ask for, under a
    # record that reports success. voc_fail is what makes the reason readable.
    run_dir = build(tmp_path, "VOC_COURSE_CATALOG=(\n")

    result = read_in_child(run_dir, "VOC_COURSE_CATALOG")

    assert result.returncode != 0
    written = (run_dir / "voccustomdata.txt").read_text()
    assert "BAD_COURSE_ENV" in written


def test_the_courses_own_variables_are_not_reported_as_vocareums(tmp_path):
    # voc_report_env exists to record what Vocareum hands a hook. A value the
    # course put there itself would make that measurement answer a different
    # question than the one it is asked.
    run_dir = build(tmp_path, "VOC_COURSE_CATALOG=workshop\n")

    assert (
        subprocess.run(
            [BASH, "-c", ". ./voclib.sh; voc_own_variable VOC_COURSE_CATALOG"],
            cwd=run_dir,
            env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(run_dir)},
            capture_output=True,
            text=True,
            timeout=60,
        ).returncode
        == 0
    )
