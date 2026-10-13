# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Response style

- **Condensed:** No preamble, no throat clearing. Only what is necessary.
- **Bullets:** Use `**term:** description` form.
- **Small words, short sentences, short paragraphs.** If a big word is unavoidable, define it right after.
- **Report shape:** what you did, whether it worked, what to do now.
- **Decisions:** 2 options max, the context needed to pick fast, and which one you would take.
- **Exactness:** Keep paths and commands verbatim.

## Commands

```bash
uv sync --group dev                 # install dev deps
uv run pytest                       # full suite (offline, no network)
uv run pytest tests/test_voclab.py  # one file
uv run pytest -k cluster_ensure     # one test by name
uv run ruff check .
uv run ruff format --check .
```

The seven CLI entry points, all defined in `pyproject.toml`:

```bash
uv run dbx-vocareum-init lab/                # scaffold a course: four hooks, course.env, README
uv run dbx-vocareum-init lab/ --force        # rewrite the hooks. course.env is never overwritten
uv run dbx-vocareum-probe --self-test        # offline check of the classifier and guards
uv run dbx-vocareum-probe --catalog          # list measurable API operations
uv run dbx-vocareum-probe --all              # measure every read-only op as the service principal
uv run dbx-vocareum-probe --source token --probes-file probes/lab-user.json
uv run dbx-vocareum-upload lab/ --dry-run    # show the archive, send nothing
uv run dbx-vocareum-upload lab/              # upload and hash-verify /voc/scripts
uv run dbx-vocareum-diagnose                 # read the existing session, start nothing
uv run dbx-vocareum-session                  # status only (default)
uv run dbx-vocareum-session --end --yes      # end the session
uv run dbx-vocareum-pool                     # read the workshop instance pool
uv run dbx-vocareum-pool --create --min-idle 5
uv run dbx-vocareum-pool --delete --yes      # nothing else ever deletes it
uv run dbx-vocareum-orphans                  # read only. Every /Users/labuser* folder, LIVE or ORPHAN
uv run dbx-vocareum-orphans --delete         # delete the orphans, after an interactive confirmation
uv run dbx-vocareum-orphans --delete --yes   # skip the confirmation, for scripted runs
```

**Costly flags.** `dbx-vocareum-diagnose --start` starts a Vocareum session, which runs `user_setup.sh`, which creates billable Databricks compute. Never pass it casually. A failed session keeps billing, so `dbx-vocareum-session --end` is the cheapest and most necessary call in the repo. `dbx-vocareum-pool --create --min-idle N` holds N booted EC2 boxes from that moment until somebody deletes the pool, and nothing automatic ever does, so `--min-idle 0` is the default and the pool is per class rather than per course.

Config comes from `.env` at the repository root (copy `.env.example`). Every tool takes `--env-file` to point elsewhere. `.env` is gitignored and the service principal's OAuth secret never goes into git.

## Architecture

Two halves that never import each other.

### `src/dbx_vocareum_tools/` runs on your laptop

An installable package behind the seven CLIs. Uses the `databricks-sdk` (for the OAuth machine-to-machine exchange against the account API) and `urllib` for Vocareum's REST API.

- **`config.py`** loads env vars layered over a dotenv file. Real env vars win. A blank value counts as missing.
- **`endpoints.py`** holds every Vocareum URL shape. `PartEndpoints`, `SessionEndpoints`, `OrgEndpoints`. Centralized because a wrong path gets a bare `400 Invalid Request` naming nothing, so URL drift across files is expensive.
- **`client.py`** HTTP, redaction-on-the-way-out, and Vocareum's async transaction poll (`202` plus a `transactionid`).
- **`security.py`** redacts by name fragment *and* by value shape. Both, because name matching misses tokens embedded in prose errors and shape matching misses short values under secret-ish names.
- **`databricks.py`** the Vocareum lifecycle contract (which hooks run, in what order, with what env vars) plus Databricks host normalization and `error_code` parsing. The lab scripts and their tests both read the contract from here rather than restating it.
- **`upload.py`** packages a course's `lab/` as one zip, injects `voclib.sh` and `voclab.py` from `labruntime/`, and `PUT`s it to the part. See "Upload gotchas" below.
- **`probe.py`** measures what one principal may do. Refuses to report until three guards pass. See "The two principals".
- **`diagnose.py`** six-step recovery of why a lab did not come up.
- **`pool.py`** creates, reads and deletes the instance pool the hooks look for by name. Reuses `probe.py`'s OAuth exchange and guards, because a pool created against the wrong host bills where no student ever goes and reports success doing it. Always sends `max_capacity` (absent means no ceiling) and `ON_DEMAND` (the default is spot, and on a single-node cluster the driver is the whole cluster). `--spark-version` defaults to nothing rather than to `DEFAULT_SPARK_VERSION`: creating falls back to it, but a status read without it lists the preloads and passes no verdict, because the runtime the hooks ask for is the course's `VOC_COURSE_SPARK_VERSION` and it lives in the course's repository where this tool cannot see it.
- **`orphans.py`** reports and reclaims the `/Users/<identity>` home folders Vocareum's deleted lab identities leave behind. Reuses `probe.py`'s OAuth exchange and guards for the same reason `pool.py` does, and reads SCIM before it reads the folder listing, because a folder is only a name and the identity plane is the only thing that can say whether the principal behind it still exists. See `docs/databricks-labs.md`.
- **`selftest.py`** offline case tables for the probe classifier, shipped inside the package so `--self-test` works inside a lab where `tests/` is absent. `tests/test_selftest_cases.py` imports the same tables so the two cannot drift.
- **`init.py`** scaffolds a course's `lab/` from `labruntime/template/`. Copies the mode as well as the bytes, because a hook that loses its exec bit fails with `Permission denied` and exit 126. Nothing here merges: a copied hook is the course's, so a scaffold that reached back into it later would be a second owner of a file this design gives exactly one.

### `src/dbx_vocareum_tools/labruntime/` runs inside Vocareum

Uploaded as a set to `/voc/scripts`. Vocareum executes only four named hooks plus `grade.sh`; anything else there is inert.

| File | When it runs |
| --- | --- |
| `template/workspace_init.sh` | once per Vocareum-created workspace |
| `template/lab_setup.sh` | on pre-warm |
| `template/user_setup.sh` | when a student starts. **The only required hook** |
| `template/lab_end.sh` | on stop or terminate |
| `voclib.sh` | never. Sourced by the four above |
| `voclab.py` | never. Invoked by the four through `voc_voclab` |

**Runtime and template are owned differently, and that is the point of the directory.** `voclib.sh` and `voclab.py` are *runtime*: the package owns them forever, no course has a copy, and `upload.py` injects them into every archive bound for `/voc/scripts`. `template/` is *template*: `dbx-vocareum-init <dir>` copies it once and the course owns it from then on, because a hook is where a course states which subcommands run and in what order. A course's own `lab/` therefore holds four hooks, a `course.env`, and its notebooks, and nothing else.

`labruntime/README.md` is the canonical account of what runs in Vocareum. Read it before touching any hook. `labruntime/template/README.md` is the shorter version a scaffolded course gets.

**Course values arrive through `course.env`, never as argparse defaults.** `voclib.sh` sources it with `set -a` at source time, so `VOC_COURSE_*` reaches `voclab.py`, which is a separate process. An argparse default is evaluated when the parser is built, which is before the environment matters, so every course option defaults to `None` and `resolve_course_value` reads the variable when the subcommand runs.

**Shell/Python boundary.** `voclab.py` narrates to stderr and returns `key=value` lines on stdout, keys from `[a-z0-9_]`. `voc_voclab` turns each into a `voclab_<key>` shell variable, clearing stale ones first. `key=value` rather than JSON because there is no `jq` to assume. `voclab.py` writes neither `voccustomdata.txt` nor `$VOC_IPC_DATA_FILE`; the calling hook does, from those variables. Keep both files single-writer.

**A course runs its own program through `voc_python <program.py>`,** which `voc_voclab` is now a wrapper over. The variable prefix is the program's name, so `workshop.py` answers as `$workshop_catalog` and a missing one fails as `MISSING_WORKSHOP_PY`. Derived rather than shared, because two programs on one namespace lets a hook read a field the other set and call it success.

**`voclab.py` is standard library only, against Python 3.9.** No `pip install` on a hook's critical path: it is a PyPI round trip a student waits through and it fails outright on a PEP 668 managed host. Do not add an SDK import here.

## Invariants worth not breaking

- **`voccustomdata.txt` is the hook's record of its own outcome, and the session record does not carry it.** Measured on one Databricks part, the session record has no `voccustomdata` key, so the ship gate (`dbx-vocareum-diagnose --expect`) and the `voc-diag` beacon are how a lab is verified. Vocareum does not expose a hook's stdout through its REST API. `voc_log` reaches the console and nobody else. A hook that exits non-zero must go through `voc_fail`, which writes `ran_at`, `script`, `error_code`, `message` in that order. A script exiting 1 without the last two produces a lab that failed for no stated reason.
- **`voclib.sh` is searched for, not sourced from one path.** Each hook tries beside `$0`, then `/voc/scripts/voclib.sh`, then the working directory. When none holds it, the hook writes `MISSING_VOCLIB` with plain `printf`, spelled out inline, because `voc_fail` lives in the file that went missing. A real session proved `$0` can carry no directory at all.
- **Cluster name is the idempotency key.** `cluster-ensure` is run by both `lab_setup.sh` and `user_setup.sh`. Whichever runs first creates; the other finds by name and starts if terminated. Pre-warming is an optimization, not a second code path.
- **Only `user_setup.sh` writes `$VOC_IPC_DATA_FILE`.** Two writers is a race with no documented winner.
- **Grant to `$VOC_DB_GROUP_NAME`, not the user.** Vocareum manages and tears down the group; a direct user grant outlives the session. The per-student cluster's `CAN_RESTART` is the deliberate exception, because `lab_end.sh` deletes the object the grant is attached to.
- **`lab_end.sh` must read `$VOC_END_LAB_BEHAVIOR` first.** On `stop` the student's work is preserved for their next session, so deleting notebooks destroys it. On `terminate` cleanup is free.
- **Anything billable created in setup must be reclaimed in `lab_end.sh`,** recorded in `voccustomdata.txt` rather than raised as a hook failure. The student has already left.

## The two principals

Every permissions statement must name which principal it describes.

- **Service principal.** OAuth creds from `.env`. Holds Account admin. Answers "will the lifecycle scripts work". This is the default for every tool.
- **Lab user.** The student's identity. A Vocareum Databricks session record publishes *no* Databricks credential (unlike an AWS lab, which returns a key triple), so the only way to measure a student is a PAT generated inside a live session and exported as `DATABRICKS_LAB_USER_TOKEN`, read by `dbx-vocareum-probe --source token` and nothing else.

`docs/permissions.md` is the single source of truth for permission results. Add measurements there and link to them; do not state a permission result anywhere else.

**Probe guards run before any result is reported:** credential presence (the SDK's provider chain will otherwise silently fall through to `DATABRICKS_TOKEN`, `~/.databrickscfg`, or a cached CLI login and report on the wrong account), host match, then identity via `GET /api/2.0/preview/scim/v2/Me`.

**Databricks hides rather than refuses.** No permission on an object is reported as the object not existing, and list calls return only what the caller can see. So the probe reports `NOT_FOUND_OR_HIDDEN` and `ALLOWED_EMPTY` rather than treating either as allowed.

## Upload gotchas

- **`content` must be a JSON array.** The object form it looks like it should be returns `HTTP 500` with a zero-length body, which reads as a server fault rather than a request-shape problem.
- **Upload the whole `lab/` directory, not the one file you changed.** The hooks chain through `VOC_CUSTOM_DATA` and share `voclib.sh`, so a partial write leaves a lab built by one version and torn down by another.
- **Exec bit comes from the shebang.** `collect_members` sets `0755` on any member starting with `#!` and `0644` otherwise, because a checkout or editor can lose the mode and a hook without it fails with `Permission denied` (exit 126).
- **Zip member times are pinned to 1980-01-01,** so the same input hashes the same twice and the verify step does not report a spurious difference.
- **The runtime is injected, and a local copy is refused.** Any upload to `/voc/scripts` gains `voclib.sh` and `voclab.py` from the package. A course's `lab/` holding either name fails the upload rather than winning it, because a stale `voclab.py` uploads perfectly, verifies by hash, and provisions with last year's runtime with nothing in the transcript saying which copy went up.
- **`README.md` and `course.env.example` never travel.** They document the directory to whoever opens it. `course.env` does travel: it is what carries the course's values to the hooks.
- **Exit `3` means hash mismatch.** Do not start a lab against it.

Exit codes: `0` ok, `1` config, `2` API. Third code varies by tool: upload `3` = hash mismatch, probe `3` = self-test failure, session `3` = refused, orphans `3` = refused. `init` makes no API call and so has no `2`.

## Docs

`docs/` is the reference layer and is meant to be read, not summarized here.

| Question | Doc |
| --- | --- |
| The four hooks, what each is handed, and what a finished session leaves behind | `docs/databricks-labs.md` |
| What each principal may do | `docs/permissions.md` |
| Getting the scripts to Vocareum | `docs/deploying.md` |
| Why a lab came up empty | `docs/diagnosing.md` |
| Why a student waits 16 minutes, and what an instance pool does about it | `docs/pre-warm.md` |
| Vocareum REST API shapes | `docs/vocareum-api.md` |

## Repository conventions

- **Prose over terse.** Module and function docstrings here explain *why*, and record the session log or measurement that forced a decision. Match that when adding code. A comment that says what a line does is not the house style; one that says why the obvious alternative was worse is.
- **Measured, not assumed.** Nothing claims a result it has not observed. `src/dbx_vocareum_tools/labruntime/README.md` and `docs/` mark open questions as open. Do not close one in prose without a measurement.
- **Placeholders in committed docs.** Real account, workspace, and metastore identifiers are written `${PLACEHOLDER}` and resolved from `.env`.
- **Tests are offline.** No test makes a network call. `voclab.py` ships as package data rather than as a module, so `tests/test_voclab.py` loads it by path via `importlib` and injects a fake `urlopen`. Loading it as `dbx_vocareum_tools.labruntime.voclab` would let it acquire a package-relative import that fails only on the Vocareum host.
