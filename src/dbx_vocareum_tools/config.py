"""Shared configuration loading for the command-line tools."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or unusable."""


# What it takes to address a part, and what it takes to address one student's
# session within it. The second is the first plus the user, mirroring the
# PartEndpoints and SessionEndpoints pair these variables are read into. Upload
# needs only the part variables, since a workspace belongs to the part rather
# than to a student.
VOCAREUM_PART_VARS = (
    "VOCAREUM_API_KEY",
    "VOCAREUM_COURSE_ID",
    "VOCAREUM_ASSIGNMENT_ID",
    "VOCAREUM_PART_ID",
)
VOCAREUM_SESSION_VARS = (*VOCAREUM_PART_VARS, "VOCAREUM_USER_ID")


@dataclass(frozen=True)
class Environment:
    """Environment variables layered over values from a dotenv file."""

    path: Path
    file_values: dict[str, str]

    @classmethod
    def from_file(cls, path: Path) -> Environment:
        """Load a minimal dotenv file."""
        values: dict[str, str] = {}
        if path.is_file():
            try:
                with path.open(encoding="utf-8") as handle:
                    for raw_line in handle:
                        line = raw_line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if line.startswith("export "):
                            line = line.removeprefix("export ").lstrip()
                        key, separator, value = line.partition("=")
                        key = key.strip()
                        if not separator or not key:
                            continue
                        value = value.strip()
                        if (
                            len(value) >= 2
                            and value[0] == value[-1]
                            and value[0] in "\"'"
                        ):
                            value = value[1:-1]
                        values[key] = value
            except OSError as error:
                raise ConfigError(
                    f"Could not read environment file {path}: {error}"
                ) from error
        return cls(path=path, file_values=values)

    def get(self, name: str, default: str = "") -> str:
        """Return an environment value, falling back to the dotenv file."""
        if name in os.environ:
            return os.environ[name].strip()
        return self.file_values.get(name, default).strip()

    def require(self, names: tuple[str, ...]) -> None:
        """Raise once with every required value that is missing."""
        missing = [name for name in names if not self.get(name)]
        if missing:
            raise ConfigError(
                "Missing required configuration: "
                + ", ".join(missing)
                + f"\nChecked real environment variables and {self.path}."
            )


def repository_root() -> Path:
    """Return the source checkout root, or the working directory when installed."""
    source_root = Path(__file__).resolve().parents[2]
    if (source_root / "pyproject.toml").is_file():
        return source_root
    return Path.cwd()
