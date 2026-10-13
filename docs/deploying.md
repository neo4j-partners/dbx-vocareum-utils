# Deploying the lifecycle scripts to Vocareum

The four hooks have to reach `/voc/scripts` in the instructor workspace of a part whose labtype is Databricks. That is the only directory Vocareum executes anything from, and the filenames have to be exact: `workspace_init.sh`, `lab_setup.sh`, `user_setup.sh`, `lab_end.sh`. [`databricks-labs.md`](databricks-labs.md) says what each one is handed.

## The set is the unit, not the file

An AWS lab deploys one CloudFormation template, so one file is the whole deployment and a partial upload is not a state that exists. A Databricks lab deploys scripts that chain through `VOC_CUSTOM_DATA` and share `voclib.sh`, so a partial upload leaves a lab that half works: the student's environment gets built by the `user_setup.sh` that landed and torn down by a `lab_end.sh` that expects a different one.

`dbx-vocareum-upload` therefore defaults to a directory, sends every file in it in one archive, and verifies each by hash before reporting success. Prefer uploading the course directory whole over uploading the one script you changed.

## Cloning a course deploys them for you

Cloning a course in the Vocareum UI carries the assignment, the part, and the entire contents of `/voc/scripts` to the clone. The new part listed every file with no upload, and its `labtype`, `databricks_maxusers` and `session_length` matched the original. The clone's assignment and part both report `create_method: "Clone by Copy"` and name the originals as `masterid`, which is how to tell a cloned part from a hand-built one when you are wondering where its scripts came from.

So the deploy step is not needed after a clone, and running it anyway is not harmless: it is a `PUT` of whatever the course directory currently holds in your checkout, over files that already match the course you cloned from. Verify first with `dbx-vocareum-upload <course dir> --dry-run`, or list the part's `/voc/scripts` directly.

What the clone does **not** carry is the roster, which is empty on the new course, and no Databricks workspace exists for the new part until its first session. A cloned course therefore reads as fully deployed and is still unusable until somebody is enrolled. [`vocareum-api.md`](vocareum-api.md#enrolling-a-diagnostic-student) has the enrolment call.

## Through the API

```bash
uv run dbx-vocareum-upload <course dir>                  # the directory to /voc/scripts
uv run dbx-vocareum-upload <course dir> --dry-run        # show what would be sent, send nothing
uv run dbx-vocareum-upload <course dir> --force          # write even when every hash already matches
uv run dbx-vocareum-upload <course dir>/user_setup.sh    # one file, same target
```

The run is four steps. It hashes each local file, reads what is up there now and skips the write when everything already matches, sends one archive, polls the transaction to a terminal state, then downloads each file back and compares hashes. It reports success only when every hash agrees.

| Exit code | Meaning |
| --- | --- |
| `0` | Every file was uploaded and verified byte for byte |
| `1` | Configuration is incomplete |
| `2` | A Vocareum API call could not complete, or the transaction did not reach success |
| `3` | The upload was accepted, but a file that came back differs from what was sent |

Exit `3` is the one to take seriously. Do not start a lab against it: a mismatch is exactly the partial-write case above, and the lab will run with two versions of the same set.

Underneath it is one call:

```
PUT /courses/{c}/assignments/{a}/parts/{p}
{"content": [{"target": "scripts", "zipcontent": "<base64 zip of the four scripts>"}]}
```

`content` must be a JSON array. The object form it looks like it should be returns `HTTP 500` with an empty body and no hint as to why. [`vocareum-api.md`](vocareum-api.md#uploading-the-lifecycle-scripts) has the full request shape and the seven body variants that were tried.

Setup is the same `.env` everything else reads, and only four values are needed. A workspace belongs to a part rather than to a student, so `VOCAREUM_USER_ID` is not one of them. [`diagnosing.md`](diagnosing.md#one-time-setup) covers the file.

### Two things the packaging does on purpose

**The executable bit comes from the shebang, not from the filesystem.** Vocareum executes these files, and a script that arrives without its mode is a lab that fails at the first hook with `Permission denied`. A checkout, a zip round trip, or an editor can all lose the bit locally, so `collect_members` sets mode `0755` on any member starting with `#!` and `0644` on everything else. This is not theoretical: all four scripts exited 126 on their first local run because the tool that wrote them does not set exec bits.

**Zip member times are pinned to 1980-01-01.** Zip entries carry a timestamp, so the same file archived twice produces two different archives and two different payload hashes, which would make the verify step report a difference on every run. 1980-01-01 is the zero of the DOS timestamp the format uses. It changes nothing about what Vocareum stores.

### Two warnings that are not errors

**`NOTE: Vocareum does not execute voclib.sh`.** Correct and expected. Vocareum runs only the four hooks plus `grade.sh`, and `voclib.sh` is sourced by the four rather than executed. The warning exists because the one case it cannot tell apart from a sourced helper is a misspelled hook name, which produces a lab that silently skips a stage.

**`WARNING: --target ... is not one of the folders seen on this course`.** The known folders are `docs`, `scripts`, `startercode`, and `private`. A wrong target is one of the shapes that produces the bodyless `500`, so the warning fires before the request rather than after.

A subdirectory is walked, and every file keeps its path relative to the directory you named. A nested member was measured to upload, reach transaction state `success`, list at exactly that path under the target, and download with the bytes that went up. Nesting is legal on the member side and refused on the `--target` side, which answers a `400` enumerating the folders it will accept. [`vocareum-api.md`](vocareum-api.md#nesting) has the table. Symlinked directories are followed, so a course can point its directory at notebooks that live elsewhere in its repository rather than keeping a second copy that drifts.

`README.md`, `course.env.example`, `__pycache__` and `.DS_Store` are skipped, along with anything whose name starts with a dot. So notes for the reader can live beside the scripts without reaching the workspace, and the scaffold a deployer copies does not travel beside the `course.env` a hook actually reads. `__pycache__` is on that list because a `.pyc` written by whatever imported the course's program between the dry run and the real one would otherwise reach `/voc/scripts`, and this API has no delete.

### `voclib.sh` has to be found, and Vocareum does not help

**Getting the flat set into `/voc/scripts` is necessary and it is not sufficient.** Vocareum guarantees neither the working directory a hook runs in nor the form of `$0`. A hook that sources `"$(dirname "$0")/voclib.sh"` was measured to fail with every file present and verified by hash, and the only place it showed was the console Session log. [`diagnosing.md`](diagnosing.md#open-the-session-log-before-you-run-anything) quotes that log line.

The hooks therefore search three candidate locations for the helper and self-report `MISSING_VOCLIB` rather than dying at the source line if none resolves. `src/dbx_vocareum_tools/labruntime/voclib.sh`'s own header carries the full rationale, and [`src/dbx_vocareum_tools/labruntime/README.md`](../src/dbx_vocareum_tools/labruntime/README.md) is the reference for the scripts themselves.

## Deploying a course that consumes this package

The course is a separate repository. This one ships the tooling and the runtime. A course ships its four hooks, its `course.env`, its notebooks, its courseware, and its own program.

### The course declares the tooling as a git dependency

The course's `pyproject.toml` carries it in the dev group:

```toml
[dependency-groups]
dev = [
    "dbx-vocareum-tools @ git+https://github.com/<owner>/dbx-vocareum.git",
]
```

The dev group rather than the runtime dependencies, because nothing the course teaches imports it. It is the deployer's toolchain. A dependency rather than a vendored copy, because `voclib.sh` and `voclab.py` then have exactly one owner and a runtime upgrade is a version bump rather than a merge. `uv sync --group dev` puts all seven entry points on `uv run` inside the course directory. The course's own package need contain nothing importable at all. Setting `bypass-selection = true` under `[tool.hatch.build.targets.wheel]` is enough, and the metadata exists only so the dependency has somewhere to be declared.

**Bump the pin whenever the tooling changes.** No `rev` is declared, so `uv.lock` records whatever the default branch pointed at when the lock was written, as a full commit SHA. The course does not see a tooling change until that moves:

```bash
uv sync --group dev --upgrade-package dbx-vocareum-tools
```

Push the tooling first, then bump, then run. Skipping it fails in a way that names the wrong thing: a stale pin can predate a command the course is calling. The same rule governs anything that ships inside the package rather than inside the course. A fix to `voclab.py` touches no course file, so a course that redeploys without bumping uploads the old runtime and hash-verifies it successfully.

### What belongs to the course, and what never does

| In the course repository | Why it is there |
| --- | --- |
| `<course dir>/workspace_init.sh`, `lab_setup.sh`, `user_setup.sh`, `lab_end.sh` | `dbx-vocareum-init` copies the template once and the course owns it from then on, because a hook is where a course states which subcommands run and in what order |
| `<course dir>/course.env` | The course's values, sourced by `voclib.sh` under `set -a` so they reach `voclab.py` as environment |
| `<course dir>/workshop.py` | The course's own program, if it has one, invoked from a hook through `voc_python workshop.py provision` |
| The notebooks a student gets, named one by one in `VOC_COURSE_NOTEBOOKS` | They can be reached from the course directory by symlink rather than kept as a second copy |
| `<course dir>/courseware/` | Data and pipeline notebooks, uploaded nested and landing at `/voc/scripts/courseware/` |
| `expected.json`, at the repository root | The ship gate manifest. Deliberately outside the course directory, because everything there is uploaded to `/voc/scripts` and this file is for the deployer rather than for the hooks |

Two things never live in the course. **`voclib.sh` and `voclab.py`** are injected into every archive bound for `/voc/scripts` from the installed package, and a course holding either name fails the upload rather than winning it. A stale `voclab.py` would upload perfectly, verify by hash, and provision with last year's runtime with nothing in the transcript saying which copy went up. **`.env`** stays here, because it holds this repository's admin credentials and the Vocareum API key, and nothing in it belongs on a host students share.

### Every call from a course repository needs `--env-file`

The course repository has no `.env` of its own and is not meant to acquire one. Point each call at the tooling's:

```bash
cd ~/projects/my-course
export VOC_ENV=~/projects/dbx-vocareum/.env
```

Then every `dbx-vocareum-*` call from that directory carries it:

```bash
uv run dbx-vocareum-upload <course dir> --dry-run
uv run dbx-vocareum-upload <course dir> --env-file "$VOC_ENV"
uv run dbx-vocareum-diagnose --expect expected.json --env-file "$VOC_ENV"
uv run dbx-vocareum-session --end --yes --env-file "$VOC_ENV"
```

`--dry-run` is the one exception and it is worth knowing why: it reads no configuration at all, so it proves the archive and says nothing about credentials. `dbx-vocareum-init` makes no API call and takes no `--env-file` either.

**A path that names no file is not an error, and the failure blames the wrong thing.** A missing `--env-file` loads as empty, so the run fails with `Missing required configuration` naming keys that are sitting in the real file. A trailing dot did exactly this: `--env-file .env.` names a file that does not exist, and the message reported four absent keys rather than an absent file. The message does print the path it read. Read that path before reading `.env`.

### The ship gate, `--expect`

```bash
uv run dbx-vocareum-diagnose --expect expected.json --env-file "$VOC_ENV"
```

Exit `0` is the ship signal. Exit `3` means something the course names is not in the workspace, and the run lists which. A `diagnose` run without the flag still exits `0` whatever it finds, so nothing that already calls the tool changes behaviour.

The gate exists because the channel a hook uses to report on itself does not survive. Measured on one part, the hooks ran, the cluster was created, the notebook was imported, and the session record's `databricks` block carried six fields and no `voccustomdata` key at all. So "did the lab come up" has to be answered from the workspace rather than from anything the hooks said. That also makes it a check that keeps working on every redeploy rather than only on the day somebody opened the console.

**The manifest belongs to the course.** This package knows how to ask whether a catalog named X exists, and it must never learn what X is, or the split between tooling and course has leaked back together. Eight keys are accepted, `catalogs`, `schemas`, `tables`, `volumes`, `warehouses`, `pipelines`, `genie_spaces` and `workspace_paths`, each holding names in the form that kind of object is addressed by. An unknown key is refused rather than skipped, because a misspelled key that is quietly ignored is an expectation nobody checks and a gate reporting a pass it never measured is worse than no gate.

Three outcomes rather than two. `PRESENT` passes. `ABSENT_OR_HIDDEN` fails, spelled that way because [Databricks hides rather than refuses](permissions.md#databricks-hides-rather-than-refuses) and a caller with no permission is told the object does not exist. `NOT_CHECKED` fails as well, because a throttled or refused call says something about the caller rather than about the object, and a gate that passed on "the call did not finish" would pass on a revoked credential. A manifest that never parsed fails for the same reason, since empty lists would otherwise read as nothing missing.

**Wait for a pipeline update to settle before believing a failure.** A gate run taken while a course's pipeline update was still going was measured to report the pipeline's output tables absent, then pass minutes later with nothing changed and nothing rerun. A hook that waits on the update has already settled when it returns. A gate an operator runs alongside it has not. Check the pipeline's latest update state before acting on the list, because this failure is indistinguishable from the real one and resolves itself.

**What the gate does not cover is the student.** It reads what the workspace holds, which is what `workspace_init.sh` built. Whether a student gets their cluster and every one of their notebooks comes from `user_setup.sh` and is measured by launching a session, from the web UI rather than from `--start`, since a hook failure arrives as a `#voc-diag=` fragment in the student's browser and a CLI launch discards it. [`diagnosing.md`](diagnosing.md) has that channel.

## Through the UI

1. Open the assignment part in Vocareum and click Configure Workspace.
2. In the file browser, go to the `scripts` folder under `voc`.
3. Upload the four hooks, `voclib.sh`, `voclab.py`, `course.env`, and the course's notebooks. The hook filenames must be exact.
4. Check the executable bit on each one, because the browser upload does not set it.
5. Save the part, publish the assignment, then verify through Student View.

The UI leaves no hash to check against, and the file listing returns names only with no size or modified time, so it cannot tell you whether what is up there is the version you meant to upload. Prefer the script.

## Validate before uploading

```bash
bash -n <course dir>/*.sh          # parse check, runs nothing
shellcheck <course dir>/*.sh       # if installed
```

A script that cannot parse uploads exactly as successfully as one that can, and the failure then surfaces minutes later inside a student session as a lab that will not start.

Neither tool can see the Vocareum environment. A script that parses cleanly and runs on a laptop can still fail in a lab because a `VOC_*` variable is absent, or because the Databricks credential it assumed is not there. [`permissions.md`](permissions.md) records what is established so far. The service principal side has been measured and the lab user side has not, because a Vocareum Databricks session publishes no Databricks credential to measure with.

For a local end-to-end run, export a simulated environment and execute the four hooks in order, chaining `VOC_CUSTOM_DATA` between them the way Vocareum does. That is how the double-`?o=` bug in the landing URL was found, and it costs no lab launch.

## After uploading

The next Start Lab uses the new files. Vocareum caches nothing. The equivalent was confirmed for an AWS lab's template: a replaced artifact is picked up on the very next launch.

Open Student View and start the lab. Then:

- **Open the Session log in the Vocareum console.** It is per session, it records the LAUNCH, START, INIT, and END transitions, and for a hook that exits non-zero it carries the exit code and the script's captured output. Read it first, because it is the only channel that carries what a script printed, and it carried the reason on a live failure where the API carried nothing. [`diagnosing.md`](diagnosing.md#open-the-session-log-before-you-run-anything) has it in full.
- **Read the browser's address bar.** Vocareum takes `notebook_url` out of `$VOC_IPC_DATA_FILE` and redirects the student there, so a hook that failed lands as `#voc-diag=<code>&script=<hook>` in the URL. It carries a code and the hook's name, never a message. [`diagnosing.md`](diagnosing.md#the-beacon-reaches-the-browser-not-the-api) has the three states and what each one means.
- **Launch from the web UI when you are diagnosing, not from the CLI.** A CLI launch discards the redirect, which is the only error channel a hook has.
- **Do not read the session record for `voccustomdata.txt`.** Measured on one Databricks part, the record's `databricks` block carried six fields and none of them was `voccustomdata`, on a live session whose cluster and notebook both existed. An absent field says nothing about whether the hook ran.
- **Judge the launch by the workspace.** The per-student cluster and every notebook the course names, in the student's home folder. Those are what only `cluster-ensure` and `notebook-import` create. `uv run dbx-vocareum-diagnose --expect expected.json` reads that for you.

If the lab does not come up at all, [`diagnosing.md`](diagnosing.md) recovers the reason without console access. If nothing happens whatsoever, check first that the part's labtype is Databricks and that `user_setup.sh` is actually in `/voc/scripts`: an absent hook is skipped in silence, so a lab with no `user_setup.sh` starts successfully and does nothing.
