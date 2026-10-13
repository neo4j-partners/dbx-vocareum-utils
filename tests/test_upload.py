"""Upload packaging tests.

The write path is asynchronous and the listing carries no hash, so an upload that
went to the wrong place, dropped a file, or lost an executable bit cannot be seen
from the response. These tests check the packaging, which is the part that can be
checked without the API.
"""

from __future__ import annotations

import io
import stat
import zipfile
from pathlib import Path

import pytest

from dbx_vocareum_tools.config import ConfigError
from dbx_vocareum_tools.databricks import LIFECYCLE_SCRIPT_NAMES
from dbx_vocareum_tools.upload import (
    EXIT_OK,
    KNOWN_TARGETS,
    SKIPPED_NAMES,
    ZIP_EPOCH,
    build_zip,
    collect_members,
    digest,
    main,
    validate_arcname,
    validate_path_segment,
)


def write_script(directory: Path, name: str, body: str = "echo hello") -> Path:
    path = directory / name
    path.write_text(f"#!/usr/bin/env bash\n{body}\n", encoding="utf-8")
    return path


def test_a_directory_uploads_as_a_set(tmp_path: Path) -> None:
    """A Databricks lab's scripts call each other, so a partial write is worse
    than a failed one. Collecting the whole directory is what makes the write
    all-or-nothing."""
    for name in LIFECYCLE_SCRIPT_NAMES:
        write_script(tmp_path, name)

    members = collect_members([tmp_path], None)
    assert sorted(member.arcname for member in members) == sorted(
        LIFECYCLE_SCRIPT_NAMES
    )


def test_readme_and_ds_store_stay_local(tmp_path: Path) -> None:
    """Notes for the reader can live beside the scripts without being deployed."""
    write_script(tmp_path, "user_setup.sh")
    for name in SKIPPED_NAMES:
        (tmp_path / name).write_text("local only\n", encoding="utf-8")

    members = collect_members([tmp_path], None)
    assert [member.arcname for member in members] == ["user_setup.sh"]


def test_a_shebang_sets_the_executable_bit(tmp_path: Path) -> None:
    """A mode lost in a checkout produces a lab that fails at the first hook.

    Vocareum executes these files, so the bit is derived from the shebang rather
    than copied from the local filesystem, where it may never have been set.
    """
    write_script(tmp_path, "user_setup.sh")
    (tmp_path / "notes.txt").write_text("not a script\n", encoding="utf-8")

    members = {member.arcname: member for member in collect_members([tmp_path], None)}
    assert members["user_setup.sh"].executable
    assert not members["notes.txt"].executable

    with zipfile.ZipFile(io.BytesIO(build_zip(list(members.values())))) as archive:
        modes = {
            info.filename: stat.S_IMODE(info.external_attr >> 16)
            for info in archive.infolist()
        }
    assert modes["user_setup.sh"] == 0o755
    assert modes["notes.txt"] == 0o644


def test_zip_member_times_are_pinned(tmp_path: Path) -> None:
    """Two runs over unchanged files have to produce the same bytes.

    The verify step compares hashes, so a timestamp baked into the archive would
    report a difference on every run and make the check meaningless.
    """
    write_script(tmp_path, "user_setup.sh")
    members = collect_members([tmp_path], None)

    first = build_zip(members)
    second = build_zip(members)
    assert first == second

    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        assert all(info.date_time == ZIP_EPOCH for info in archive.infolist())


def test_a_subdirectory_travels_as_a_nested_member(tmp_path: Path) -> None:
    """Measured: a member carrying a directory prefix uploads, lands at that
    exact path under the target, and downloads with the bytes sent."""
    write_script(tmp_path, "user_setup.sh")
    (tmp_path / "courseware" / "data").mkdir(parents=True)
    (tmp_path / "courseware" / "data" / "nodes.csv").write_text(
        "id,name\n1,a\n", encoding="utf-8"
    )
    write_script(tmp_path / "courseware", "helper.sh")

    members = collect_members([tmp_path], None)
    assert [member.arcname for member in members] == [
        "courseware/data/nodes.csv",
        "courseware/helper.sh",
        "user_setup.sh",
    ]


def test_nested_arcnames_are_relative_to_the_source_root(tmp_path: Path) -> None:
    """An absolute arcname would address somebody else's workspace tree, and a
    path relative to the process working directory would change meaning with
    the shell that ran the command."""
    source = tmp_path / "lab"
    (source / "courseware").mkdir(parents=True)
    write_script(source, "user_setup.sh")
    (source / "courseware" / "nodes.csv").write_text("id\n1\n", encoding="utf-8")

    members = collect_members([source], None)
    arcnames = [member.arcname for member in members]
    assert arcnames == ["courseware/nodes.csv", "user_setup.sh"]
    assert not any(name.startswith("/") for name in arcnames)
    assert not any(str(tmp_path) in name for name in arcnames)


def test_nested_members_sort_deterministically(tmp_path: Path) -> None:
    """The archive is hashed and compared, so two runs over unchanged files have
    to produce the same member order as well as the same bytes."""
    (tmp_path / "b_dir" / "inner").mkdir(parents=True)
    (tmp_path / "a_dir").mkdir()
    (tmp_path / "b_dir" / "inner" / "z.txt").write_text("z\n", encoding="utf-8")
    (tmp_path / "b_dir" / "a.txt").write_text("a\n", encoding="utf-8")
    (tmp_path / "a_dir" / "m.txt").write_text("m\n", encoding="utf-8")
    (tmp_path / "top.txt").write_text("top\n", encoding="utf-8")

    first = [member.arcname for member in collect_members([tmp_path], None)]
    second = [member.arcname for member in collect_members([tmp_path], None)]
    assert (
        first
        == second
        == [
            "a_dir/m.txt",
            "b_dir/a.txt",
            "b_dir/inner/z.txt",
            "top.txt",
        ]
    )
    assert build_zip(collect_members([tmp_path], None)) == build_zip(
        collect_members([tmp_path], None)
    )


def test_skipped_names_apply_inside_a_subdirectory(tmp_path: Path) -> None:
    """A README documents the directory it sits in at any depth. Uploading one
    from a subdirectory is the same mistake as uploading one from the top."""
    write_script(tmp_path, "user_setup.sh")
    (tmp_path / "courseware").mkdir()
    for name in SKIPPED_NAMES:
        (tmp_path / "courseware" / name).write_text("local only\n", encoding="utf-8")
    (tmp_path / "courseware" / "nodes.csv").write_text("id\n1\n", encoding="utf-8")

    members = collect_members([tmp_path], None)
    assert [member.arcname for member in members] == [
        "courseware/nodes.csv",
        "user_setup.sh",
    ]


def test_a_pycache_directory_stays_local(tmp_path: Path) -> None:
    """A course's __pycache__ would otherwise reach /voc/scripts. The API has
    no delete, so it stops here."""
    write_script(tmp_path, "user_setup.sh")
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__" / "workshop.cpython-313.pyc").write_bytes(b"\xcb\r\r\n")

    members = collect_members([tmp_path], None)
    assert [member.arcname for member in members] == ["user_setup.sh"]


def test_a_flat_directory_is_unchanged_by_nesting_support(tmp_path: Path) -> None:
    """Every course deployed so far ships a flat lab/, and lifting the refusal
    must not have moved any of those files."""
    for name in LIFECYCLE_SCRIPT_NAMES:
        write_script(tmp_path, name)
    (tmp_path / "course.env").write_text("VOC_COURSE_CATALOG=demo\n", encoding="utf-8")

    members = collect_members([tmp_path], None)
    assert [member.arcname for member in members] == sorted(
        [*LIFECYCLE_SCRIPT_NAMES, "course.env"]
    )


def test_a_nested_member_keeps_its_mode_and_pinned_time(tmp_path: Path) -> None:
    """The two packaging guarantees are per member, so they have to survive a
    member that is no longer at the top of the archive."""
    (tmp_path / "courseware").mkdir()
    write_script(tmp_path / "courseware", "helper.sh")
    (tmp_path / "courseware" / "nodes.csv").write_text("id\n1\n", encoding="utf-8")

    members = collect_members([tmp_path], None)
    with zipfile.ZipFile(io.BytesIO(build_zip(members))) as archive:
        infos = {info.filename: info for info in archive.infolist()}
    assert stat.S_IMODE(infos["courseware/helper.sh"].external_attr >> 16) == 0o755
    assert stat.S_IMODE(infos["courseware/nodes.csv"].external_attr >> 16) == 0o644
    assert all(info.date_time == ZIP_EPOCH for info in infos.values())


def test_a_symlinked_directory_is_followed(tmp_path: Path) -> None:
    """A course whose data is generated elsewhere in its repository points at it
    once. The alternative is a second copy of 27 files that drifts, which is the
    failure the one-owner-per-file rule exists to prevent."""
    elsewhere = tmp_path / "generated"
    elsewhere.mkdir()
    (elsewhere / "nodes.csv").write_text("id\n1\n", encoding="utf-8")

    source = tmp_path / "lab"
    source.mkdir()
    write_script(source, "user_setup.sh")
    (source / "courseware").symlink_to(elsewhere, target_is_directory=True)

    members = collect_members([source], None)
    assert [member.arcname for member in members] == [
        "courseware/nodes.csv",
        "user_setup.sh",
    ]


def test_a_symlink_loop_stops_rather_than_recursing(tmp_path: Path) -> None:
    """Following symlinks buys the single-copy layout and costs a cycle risk."""
    source = tmp_path / "lab"
    (source / "inner").mkdir(parents=True)
    (source / "inner" / "nodes.csv").write_text("id\n1\n", encoding="utf-8")
    (source / "inner" / "loop").symlink_to(source, target_is_directory=True)

    members = collect_members([source], None)
    assert "inner/nodes.csv" in {member.arcname for member in members}


@pytest.mark.parametrize(
    "arcname",
    [
        "../escape.sh",
        "courseware/../../escape.sh",
        "/absolute.sh",
        "a//b.sh",
        "a/ b.sh",
    ],
)
def test_a_nested_arcname_that_could_escape_is_refused(arcname: str) -> None:
    """Nesting is allowed. Escaping the target still is not, at any depth."""
    with pytest.raises(ConfigError):
        validate_arcname(arcname, "an archive name")


def test_an_empty_directory_is_refused(tmp_path: Path) -> None:
    """An upload of nothing would report success while deploying nothing."""
    with pytest.raises(ConfigError):
        collect_members([tmp_path], None)


def test_a_single_file_can_be_renamed(tmp_path: Path) -> None:
    source = write_script(tmp_path, "local_name.sh")
    members = collect_members([source], "user_setup.sh")
    assert [member.arcname for member in members] == ["user_setup.sh"]


def test_a_rename_is_refused_for_more_than_one_file(tmp_path: Path) -> None:
    write_script(tmp_path, "a.sh")
    write_script(tmp_path, "b.sh")
    with pytest.raises(ConfigError):
        collect_members([tmp_path], "user_setup.sh")


@pytest.mark.parametrize(
    "segment", ["..", ".", "a/b", "", "/scripts", "back\\slash", "bell\x07"]
)
def test_a_target_that_could_escape_is_refused(segment: str) -> None:
    """The target becomes a path in someone else's workspace tree."""
    with pytest.raises(ConfigError):
        validate_path_segment(segment, "--target")


@pytest.mark.parametrize("segment", ["   ", "\t", " scripts", "scripts ", "\nscripts"])
def test_a_target_carrying_whitespace_is_refused(segment: str) -> None:
    """Whitespace escapes nothing, which is why the escape checks miss it.

    A target of "   " addresses /voc/   and " scripts" addresses a folder that
    looks in a terminal exactly like the one that exists. Neither is a directory
    Vocareum reads, and both would be written by an accepted upload that then
    reports success.
    """
    with pytest.raises(ConfigError):
        validate_path_segment(segment, "--target")


@pytest.mark.parametrize("segment", sorted(KNOWN_TARGETS))
def test_the_known_targets_are_accepted(segment: str) -> None:
    validate_path_segment(segment, "--target")


def test_a_dry_run_needs_no_configuration(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The dry run answers which files, under which names, with which hashes.

    None of that addresses a course, an assignment, or a part, so pointing it at
    an environment file that does not exist has to still work. It is the one
    command worth running before there is a part to upload to.
    """
    write_script(tmp_path, "user_setup.sh")
    exit_code = main(
        [str(tmp_path), "--dry-run", "--env-file", str(tmp_path / "absent.env")]
    )
    assert exit_code == EXIT_OK
    printed = capsys.readouterr().out
    assert "user_setup.sh" in printed
    assert "Nothing was uploaded" in printed


def test_digest_is_content_addressed() -> None:
    assert digest(b"same") == digest(b"same")
    assert digest(b"same") != digest(b"different")
