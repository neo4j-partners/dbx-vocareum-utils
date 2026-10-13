"""Scaffold a course's ``lab/`` directory from the packaged template.

``dbx-vocareum-init lab/`` writes the four Vocareum lifecycle hooks, a
``course.env.example`` and a ``README.md`` into a directory a course owns from
that moment on. It is the counterpart to the runtime injection in ``upload.py``:
between them, a course holds exactly the files it has decisions to make about
and none of the files it does not.

Why a scaffold rather than a second package directory the hooks are read from.
A hook is where a course states what it wants: which subcommands run, in what
order, and what reaches ``voccustomdata.txt``. Making the four hooks uneditable
would push every course-specific decision into an environment variable, and the
first course with a fifth thing to provision would fork the package. So the
hooks are copied once and then belong to the course, while ``voclib.sh`` and
``voclab.py``, which no course has a reason to edit, never leave the package.

Nothing here is clever about upgrades, deliberately. A file that already exists
is left alone and reported, and ``--force`` overwrites the lot. A merge would be
the wrong tool: the whole point of the split is that a copied hook is the
course's, so a scaffold that reached back into it later would be a second owner
of a file this design gives exactly one.

Exit codes: ``0`` ok, ``1`` configuration. There is no API call here and
therefore no ``2``.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from .config import ConfigError
from .labruntime import (
    COURSE_ENV_FILE,
    COURSE_ENV_TEMPLATE,
    RUNTIME_FILE_NAMES,
    template_files,
)

EXIT_OK = 0
EXIT_CONFIG = 1


def plan_copies(
    destination: Path, force: bool
) -> tuple[list[tuple[Path, Path]], list[Path]]:
    """Decide what to write, and what is already there.

    Returns the copies to make and the destinations left alone. Both halves are
    returned rather than the first half only, because a scaffold that says
    nothing about the four files it skipped reads as a scaffold that wrote them.
    """
    copies: list[tuple[Path, Path]] = []
    kept: list[Path] = []
    for source in template_files():
        target = destination / source.name
        if target.exists() and not force:
            kept.append(target)
        else:
            copies.append((source, target))
    return copies, kept


def course_env_source(destination: Path, force: bool) -> Path | None:
    """Whether ``course.env`` should be seeded, and from what.

    The example is copied to the real name as well as under its own, so a course
    has a file to edit rather than a file to remember to copy. Every line in it
    is commented, so seeding one changes no behavior: a course that fills in
    nothing gets exactly the defaults it would have got with no file at all.

    Skipped when the course already has one, ``--force`` or not. This is the one
    file in the directory a deployer types values into by hand, and overwriting
    it would discard a course's configuration to fix a scaffold.
    """
    target = destination / COURSE_ENV_FILE
    if target.exists():
        return None
    example = destination / COURSE_ENV_TEMPLATE
    return example if example.exists() or force else None


def scaffold(destination: Path, force: bool) -> int:
    """Write the template into ``destination`` and say what happened."""
    if destination.exists() and not destination.is_dir():
        raise ConfigError(f"{destination} exists and is not a directory.")

    copies, kept = plan_copies(destination, force)
    destination.mkdir(parents=True, exist_ok=True)

    for source, target in copies:
        shutil.copyfile(source, target)
        # The mode is copied separately, because ``copyfile`` does not carry it
        # and a hook that arrives without its exec bit fails with
        # "Permission denied" and exit 126. ``upload.py`` re-derives the bit
        # from the shebang for exactly the same reason, and both are worth
        # having: this one keeps a local run of the hook working too.
        shutil.copymode(source, target)
        print(f"wrote {target}")

    for target in kept:
        print(f"kept {target}, which already existed")

    example = course_env_source(destination, force)
    if example is not None:
        shutil.copyfile(example, destination / COURSE_ENV_FILE)
        print(f"wrote {destination / COURSE_ENV_FILE} from {COURSE_ENV_TEMPLATE}")

    print()
    print(f"{destination} is now a course lab directory.")
    print(f"Fill in {destination / COURSE_ENV_FILE}, put the course's notebooks")
    print("beside the hooks, then:")
    print(f"    uv run dbx-vocareum-upload {destination}/ --dry-run")
    print()
    print(
        f"{' and '.join(RUNTIME_FILE_NAMES)} are not written here. They ship "
        "inside dbx-vocareum-tools"
    )
    print("and are injected into the archive at upload time.")
    return EXIT_OK


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="dbx-vocareum-init",
        description=(
            "Scaffold a course's lab directory: the four Vocareum lifecycle "
            "hooks, a course.env to fill in, and a README naming the rules "
            "the hooks depend on."
        ),
    )
    parser.add_argument(
        "directory",
        help="Where to write the template. Created if it does not exist.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Overwrite hooks that already exist. course.env is never "
            "overwritten, because it is the one file here holding values a "
            "human typed."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point. See the module docstring for exit code meanings."""
    args = parse_args(argv)
    try:
        return scaffold(Path(args.directory).expanduser(), args.force)
    except (ConfigError, FileNotFoundError, OSError) as error:
        print("Could not scaffold the lab directory.")
        print(error)
        return EXIT_CONFIG


if __name__ == "__main__":
    raise SystemExit(main())
