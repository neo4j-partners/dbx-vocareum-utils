# The lab directory

`dbx-vocareum-init` wrote this directory. It is the course's half of a Vocareum
Databricks lab: the four lifecycle hooks, the course's values, and whatever
content the course hands a student.

This file is never uploaded. `dbx-vocareum-upload` skips `README.md` by name.

## What is here, and who owns it

| File | Owner | Edit it? |
| --- | --- | --- |
| `workspace_init.sh` | this course | yes |
| `lab_setup.sh` | this course | yes |
| `user_setup.sh` | this course | yes |
| `lab_end.sh` | this course | yes |
| `course.env.example` | the package | no, copy it |
| `course.env` | this course | yes, and keep it out of git if it ever holds anything sensitive |

Two files that run inside Vocareum are **not** here and should not be added:
`voclib.sh` and `voclab.py`. They ship inside `dbx-vocareum-tools` and
`dbx-vocareum-upload` injects them into the archive at upload time. A local copy
of either is refused by the upload rather than preferred over the package's,
because a stale `voclab.py` uploads and verifies exactly as cleanly as a current
one and nothing downstream would notice. Upgrade the runtime by bumping the
dependency.

## First run

1. `cp course.env.example course.env`, then fill in what this course needs.
   Every key is optional and the file's own comments say what each one costs.
2. Put the course's notebooks in this directory beside the hooks, and name them
   in `VOC_COURSE_NOTEBOOKS`. They travel in `/voc/scripts`, which is where the
   hooks can read them; `/voc/startercode` was measured not to reach a student's
   Databricks workspace at all.
3. `uv run dbx-vocareum-upload <this directory> --dry-run` to see the archive,
   then without the flag to send it. Exit `3` is a hash mismatch: do not start a
   lab against it.

Only five names execute out of `/voc/scripts`: the four hooks above and
`grade.sh`. Everything else there, including the notebooks and `course.env`, is
inert, and the upload warns about each one so a typo in a hook name is visible.

## Adding provisioning

The hooks call `voclab.py` through `voc_voclab`, which turns its `key=value`
output into `voclab_<key>` shell variables. Course-specific provisioning that
`voclab.py` does not cover belongs in `workspace_init.sh` when it is shared by
every student, and in `user_setup.sh` when it is per student.

`voclab.py` is the runtime's and you do not edit it. For anything it does not
cover, put your own program beside these hooks and call it with `voc_python
yours.py [args...]`, which is the general form `voc_voclab` wraps. Same three
place search, same `python3` guard, same `key=value` contract. The prefix is
your program's name, so `workshop.py` answers as `$workshop_catalog` and a
missing one fails as `MISSING_WORKSHOP_PY`. Write it standard library only
against Python 3.9, the same as `voclab.py`, and `import voclab` if you want its
HTTP layer rather than a second copy of one.

Three rules the hooks depend on, all of them the hard way:

- **`voccustomdata.txt` has one writer.** A hook that ends badly goes through
  `voc_fail`, which writes `ran_at`, `script`, `error_code` and `message` to it.
  A script that exits 1 without those produces a lab that failed for no stated
  reason, because Vocareum does not expose a hook's standard output.
- **Grant to `$VOC_DB_GROUP_NAME`, not to the user.** Vocareum tears the group
  down; a direct user grant outlives the session.
- **`lab_end.sh` reads `$VOC_END_LAB_BEHAVIOR` first.** On `stop` the student's
  work is preserved for their next session, so deleting it destroys their work.
  Anything billable created in setup is reclaimed there and recorded in
  `voccustomdata.txt` rather than raised as a failure, because the student has
  already left.
