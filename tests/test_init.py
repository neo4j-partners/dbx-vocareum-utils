"""What ``dbx-vocareum-init`` writes, and what it refuses to overwrite.

The scaffold is one half of a two-part rule about ownership. It copies the four
lifecycle hooks into a directory a course owns from that moment on, and it
deliberately does not copy ``voclib.sh`` or ``voclab.py``, which stay in the
package and are injected by ``upload.py`` at deployment time. Both halves are
tested here, because the failure the rule prevents is silent: a course holding
its own runtime copy uploads and verifies exactly as cleanly as one that does
not, and provisions with whatever version it was scaffolded with.

Offline like every test here. Nothing below opens a socket.
"""

from __future__ import annotations

import stat
from pathlib import Path

import pytest

from dbx_vocareum_tools import init, upload
from dbx_vocareum_tools.config import ConfigError
from dbx_vocareum_tools.labruntime import (
    COURSE_ENV_FILE,
    COURSE_ENV_TEMPLATE,
    RUNTIME_FILE_NAMES,
    template_files,
)

HOOKS = ("workspace_init.sh", "lab_setup.sh", "user_setup.sh", "lab_end.sh")


def scaffolded(tmp_path: Path) -> Path:
    destination = tmp_path / "lab"
    assert init.main([str(destination)]) == init.EXIT_OK
    return destination


# --- what lands in a new course ----------------------------------------------


def test_the_four_hooks_and_a_course_env_are_written(tmp_path):
    destination = scaffolded(tmp_path)

    for name in HOOKS:
        assert (destination / name).is_file()
    assert (destination / COURSE_ENV_TEMPLATE).is_file()
    assert (destination / COURSE_ENV_FILE).is_file()


def test_a_hook_arrives_executable(tmp_path):
    # Vocareum runs these directly. A hook without its exec bit fails with
    # "Permission denied" and exit 126, and copyfile does not carry the mode,
    # which is why the scaffold copies it separately.
    destination = scaffolded(tmp_path)

    for name in HOOKS:
        assert (destination / name).stat().st_mode & stat.S_IXUSR


def test_the_runtime_is_not_written_into_a_course(tmp_path):
    # The whole seam. voclib.sh and voclab.py belong to the package forever, and
    # a course that had a copy would upgrade its runtime by being rescaffolded
    # rather than by bumping a dependency.
    destination = scaffolded(tmp_path)

    for name in RUNTIME_FILE_NAMES:
        assert not (destination / name).exists()


def test_seeding_course_env_changes_no_behavior(tmp_path):
    # Every line in the example is commented, so a course that fills in nothing
    # gets exactly the defaults it would have got with no file at all. That is
    # what makes seeding it safe rather than an opinion the scaffold imposed.
    destination = scaffolded(tmp_path)

    lines = (destination / COURSE_ENV_FILE).read_text().splitlines()
    assert lines
    for line in lines:
        stripped = line.strip()
        assert not stripped or stripped.startswith("#")


# --- what a second run does --------------------------------------------------


def test_a_hook_a_course_has_edited_is_left_alone(tmp_path):
    destination = scaffolded(tmp_path)
    edited = destination / "user_setup.sh"
    edited.write_text("#!/bin/sh\n# this course's own\n")

    assert init.main([str(destination)]) == init.EXIT_OK

    assert "this course's own" in edited.read_text()


def test_force_replaces_a_hook_but_never_course_env(tmp_path):
    # course.env is the one file here holding values a human typed, so
    # overwriting it would discard a course's configuration to fix a scaffold.
    destination = scaffolded(tmp_path)
    (destination / "user_setup.sh").write_text("#!/bin/sh\n# stale\n")
    (destination / COURSE_ENV_FILE).write_text("VOC_COURSE_CATALOG=workshop\n")

    assert init.main([str(destination), "--force"]) == init.EXIT_OK

    assert "stale" not in (destination / "user_setup.sh").read_text()
    assert (destination / COURSE_ENV_FILE).read_text() == (
        "VOC_COURSE_CATALOG=workshop\n"
    )


def test_a_destination_that_is_a_file_is_refused_rather_than_clobbered(tmp_path):
    target = tmp_path / "lab"
    target.write_text("not a directory")

    assert init.main([str(target)]) == init.EXIT_CONFIG
    assert target.read_text() == "not a directory"


def test_plan_copies_reports_what_it_skipped(tmp_path):
    # Both halves are returned, because a scaffold that says nothing about the
    # four files it skipped reads as a scaffold that wrote them.
    destination = scaffolded(tmp_path)

    copies, kept = init.plan_copies(destination, force=False)

    assert copies == []
    assert sorted(path.name for path in kept) == sorted(
        source.name for source in template_files()
    )


# --- and what the upload does with the result --------------------------------


def test_a_scaffolded_directory_uploads_the_hooks_plus_the_packaged_runtime(tmp_path):
    destination = scaffolded(tmp_path)

    args = upload.parse_args([str(destination)])
    members = upload.prepare(args)

    names = [member.arcname for member in members]
    assert set(HOOKS).issubset(names)
    assert set(RUNTIME_FILE_NAMES).issubset(names)
    # The README documents the directory to whoever opens it and the example is
    # the source for course.env. Neither is a file Vocareum has any use for.
    assert "README.md" not in names
    assert COURSE_ENV_TEMPLATE not in names
    assert COURSE_ENV_FILE in names


def test_a_local_copy_of_the_runtime_is_refused_rather_than_preferred(tmp_path):
    destination = scaffolded(tmp_path)
    (destination / "voclab.py").write_text("# last year's runtime\n")

    args = upload.parse_args([str(destination)])
    with pytest.raises(ConfigError) as failure:
        upload.prepare(args)

    assert "voclab.py" in str(failure.value)


def test_uploading_somewhere_other_than_scripts_injects_nothing(tmp_path):
    # The runtime belongs in /voc/scripts. A file sent anywhere else is content,
    # and adding two shell scripts to it would be surprising.
    source = tmp_path / "roster.csv"
    source.write_text("a,b\n")

    args = upload.parse_args([str(source), "--target", "startercode"])
    members = upload.prepare(args)

    assert [member.arcname for member in members] == ["roster.csv"]
