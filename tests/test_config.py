"""Configuration loading tests.

Every tool reads the same layered configuration: real environment variables over
a dotenv file. Getting the precedence backwards would mean a value exported for
one run being silently ignored in favor of a stale file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dbx_vocareum_tools.config import (
    VOCAREUM_PART_VARS,
    VOCAREUM_SESSION_VARS,
    ConfigError,
    Environment,
    repository_root,
)


def test_a_real_environment_variable_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """So the same script runs unchanged in CI, where there is no .env."""
    env_file = tmp_path / ".env"
    env_file.write_text("VOCAREUM_COURSE_ID=from-file\n", encoding="utf-8")
    monkeypatch.setenv("VOCAREUM_COURSE_ID", "from-environment")

    assert Environment.from_file(env_file).get("VOCAREUM_COURSE_ID") == (
        "from-environment"
    )


def test_dotenv_parsing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "# a comment",
                "",
                "PLAIN=value",
                "  SPACED  =  padded  ",
                'QUOTED="quoted value"',
                "SINGLE='single value'",
                "export EXPORTED=exported",
                "NOEQUALS",
                "=novalue",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    for name in ("PLAIN", "SPACED", "QUOTED", "SINGLE", "EXPORTED"):
        monkeypatch.delenv(name, raising=False)

    environment = Environment.from_file(env_file)
    assert environment.get("PLAIN") == "value"
    assert environment.get("SPACED") == "padded"
    assert environment.get("QUOTED") == "quoted value"
    assert environment.get("SINGLE") == "single value"
    assert environment.get("EXPORTED") == "exported"
    assert environment.get("NOEQUALS") == ""


def test_a_missing_file_is_not_an_error(tmp_path: Path) -> None:
    """The tools run from real environment variables alone."""
    environment = Environment.from_file(tmp_path / "absent.env")
    assert environment.file_values == {}


def test_require_names_everything_missing_at_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One message rather than one per run, so the fix takes one pass."""
    for name in VOCAREUM_SESSION_VARS:
        monkeypatch.delenv(name, raising=False)
    environment = Environment.from_file(tmp_path / "absent.env")

    with pytest.raises(ConfigError) as caught:
        environment.require(VOCAREUM_SESSION_VARS)

    message = str(caught.value)
    for name in VOCAREUM_SESSION_VARS:
        assert name in message


def test_a_blank_value_counts_as_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A variable present in name and absent in fact is the dangerous case."""
    env_file = tmp_path / ".env"
    env_file.write_text("VOCAREUM_API_KEY=   \n", encoding="utf-8")
    monkeypatch.delenv("VOCAREUM_API_KEY", raising=False)

    with pytest.raises(ConfigError):
        Environment.from_file(env_file).require(("VOCAREUM_API_KEY",))


def test_the_session_variables_extend_the_part_variables() -> None:
    """A workspace belongs to a part; a session belongs to a student in it.

    Upload needs only the part variables, so the two sets have to stay nested
    rather than drift into two independent lists.
    """
    assert VOCAREUM_SESSION_VARS[: len(VOCAREUM_PART_VARS)] == VOCAREUM_PART_VARS
    assert set(VOCAREUM_SESSION_VARS) - set(VOCAREUM_PART_VARS) == {"VOCAREUM_USER_ID"}


def test_repository_root_finds_the_checkout() -> None:
    assert (repository_root() / "pyproject.toml").is_file()
