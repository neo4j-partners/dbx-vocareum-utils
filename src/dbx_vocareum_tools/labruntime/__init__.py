"""The files that run inside Vocareum, shipped as package data.

Everything under this directory is uploaded to ``/voc/scripts`` and executed
there. It is in the package rather than in a course's checkout because a course
should not own a copy of it: ``voclib.sh`` and ``voclab.py`` are the runtime,
they are the same for every course, and the moment a second course exists the
copies drift. ``dbx-vocareum-upload`` injects them into the archive at upload
time, so a course's ``lab/`` holds only the four hooks and whatever content that
course ships.

Two kinds of file live here, and the difference is who owns them afterwards.

**Runtime** (``voclib.sh``, ``voclab.py``) is owned by this package forever. A
course never edits it, a course never has a copy of it, and a course upgrades it
by bumping the dependency. ``upload.py`` refuses an upload that carries a local
file of either name, because two files with one name is the merge problem this
arrangement exists to avoid.

**Template** (``template/``) is copied once, by ``dbx-vocareum-init``, and is
then owned by the course. The four hooks are template rather than runtime
because a hook is where a course states what it wants: which subcommands run, in
what order, and what reaches ``voccustomdata.txt``. Handing a course an
uneditable hook would push every course-specific decision into an environment
variable, which is how a course-agnostic tool acquires a course's vocabulary.

``course.env`` sits on the seam. It is generated from the template's
``course.env.example`` and it is what carries a course's values into a hook,
which is why ``voclib.sh`` sources it and nothing in this package reads it
directly.
"""

from __future__ import annotations

from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = RUNTIME_DIR / "template"

# The two files uploaded from this package on every deployment. Named rather
# than globbed so that adding a file here is a deliberate act: anything landing
# in /voc/scripts is a file Vocareum's host will have on disk during a student's
# session, and the four hook names plus grade.sh are the only ones it executes.
RUNTIME_FILE_NAMES = ("voclib.sh", "voclab.py")

# What a course's ``lab/`` may not hold. Same names as above, and the check is
# worth having because the failure it prevents is silent: a stale local
# ``voclab.py`` uploads perfectly and the course runs last year's runtime.
COURSE_OWNED_ELSEWHERE = frozenset(RUNTIME_FILE_NAMES)

# Written by ``dbx-vocareum-init`` and sourced by ``voclib.sh``. Not
# ``.env``, because that name already means the deployer's laptop credentials in
# every repository here and this file is uploaded to a host students share.
COURSE_ENV_FILE = "course.env"
COURSE_ENV_TEMPLATE = "course.env.example"


def runtime_files() -> list[Path]:
    """Return the runtime files, in a fixed order, checking each is present.

    A missing file here means a broken install rather than a broken course, and
    saying so at upload time is much cheaper than finding out from a hook that
    could not source its own library.
    """
    found = []
    for name in RUNTIME_FILE_NAMES:
        path = RUNTIME_DIR / name
        if not path.is_file():
            raise FileNotFoundError(
                f"{path} is missing, so this install of dbx-vocareum-tools "
                "cannot supply the lab runtime. Reinstall the package."
            )
        found.append(path)
    return found


def template_files() -> list[Path]:
    """Return everything ``dbx-vocareum-init`` copies into a new course.

    The whole directory rather than a fixed list, so a file added to the
    template reaches new courses without a second edit here. Sorted, because the
    scaffold prints what it wrote and a stable order makes two runs comparable.
    """
    if not TEMPLATE_DIR.is_dir():
        raise FileNotFoundError(
            f"{TEMPLATE_DIR} is missing, so this install of dbx-vocareum-tools "
            "cannot scaffold a course. Reinstall the package."
        )
    return sorted(item for item in TEMPLATE_DIR.iterdir() if item.is_file())
