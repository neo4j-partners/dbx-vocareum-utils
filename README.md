# dbx-vocareum

Tooling and a lab runtime for running Databricks courses on Vocareum.

Vocareum gives each student a user identity and a group inside a shared Databricks workspace, rather than an account of their own. There is no CloudFormation hook, so the environment is built by four shell scripts that Vocareum reads from `/voc/scripts` and runs as the session opens: `workspace_init.sh` once per workspace, `lab_setup.sh` when a lab is pre-warmed, `user_setup.sh` when a student starts, and `lab_end.sh` when the lab ends. Only `user_setup.sh` is required. That script is where the student's compute and their copy of the course notebooks come from, and the notebook URL it writes into `$VOC_IPC_DATA_FILE` is what the student lands on. The notebooks travel in `/voc/scripts` with the four scripts, and `user_setup.sh` imports them into the student's Databricks workspace. `/voc/startercode` was measured not to do that: a notebook uploaded there reaches the Vocareum environment beside Databricks, and the student's Databricks folder `/Users/<lab identity>` stays empty.

An AWS equivalent of this tooling exists, built around a CloudFormation template. The differences from it are deliberate and are noted where they appear.

## Admin guide

Everything an admin does here is one of seven tools. Fill in `.env` first, by copying `.env.example`. A course lives in its own repository, so the tools that touch a course run from there and every one of them needs `--env-file`. [`docs/deploying.md`](docs/deploying.md) covers that layout.

| I want to | Run |
| --- | --- |
| Scaffold a new course's lab directory | `uv run dbx-vocareum-init <course dir>` |
| Push a change to the lab | `uv run dbx-vocareum-upload <course dir>` |
| Speed up how long students wait for compute | `uv run dbx-vocareum-pool --create --min-idle 5` |
| See what a session is doing | `uv run dbx-vocareum-diagnose` |
| Prove a course is ready to ship | `uv run dbx-vocareum-diagnose --expect expected.json` |
| Stop paying for a test session | `uv run dbx-vocareum-session --end --yes` |
| Stop paying for the pool | `uv run dbx-vocareum-pool --delete --yes` |
| Check what a principal may do | `uv run dbx-vocareum-probe --all` |
| See the home folders deleted lab identities left behind | `uv run dbx-vocareum-orphans` |
| Reclaim them | `uv run dbx-vocareum-orphans --delete` |

Two of these spend money, and one changes what every later student gets. `dbx-vocareum-diagnose --start` starts a Vocareum session, which runs `user_setup.sh`, which creates billable Databricks compute. `dbx-vocareum-pool --create --min-idle N` holds N booted EC2 boxes from that moment until somebody deletes the pool, and nothing automatic ever does. `dbx-vocareum-upload` writes to the part that every later Start Lab reads.

Three destroy something, and each takes an explicit flag to do it: `dbx-vocareum-session --end`, `dbx-vocareum-pool --delete`, and `dbx-vocareum-orphans --delete`. Without that flag all three only read. Everything else here reads and changes nothing.

### Configuration, and `--env-file`

Configuration comes from `.env` at the repository root. A real environment variable beats what `.env` says, and a blank value counts as missing. Every tool except `dbx-vocareum-init` takes `--env-file PATH` to read that configuration from somewhere else:

```bash
uv run dbx-vocareum-diagnose --env-file ~/courses/my-course/.env
```

**Type the path carefully.** A `--env-file` that names something other than the file you meant loads as an empty file, and the tool then reports every key missing rather than reporting a bad path. A trailing dot did exactly that. When a tool says the whole configuration is absent, check the path before you check `.env`.

`dbx-vocareum-init` makes no API call, reads no configuration, and so has no `--env-file`.

### Scaffolding a course

```bash
uv run dbx-vocareum-init <course dir>            # four hooks, course.env, and a README
uv run dbx-vocareum-init <course dir> --force    # rewrite the hooks in place
```

The template is copied once and the course owns its copy from then on, which is why a hook is where a course states which subcommands run and in what order. `course.env` is never overwritten, with or without `--force`, because it is the one file in the directory holding values a human typed. `voclib.sh` and `voclab.py` are not written there at all: they ship inside the package, and `dbx-vocareum-upload` injects them into every archive bound for `/voc/scripts`.

### Updating the lab

One directory holds everything a student is affected by, and one command puts it there.

```bash
uv run dbx-vocareum-upload <course dir> --dry-run    # show the archive, send nothing
uv run dbx-vocareum-upload <course dir>              # send it
uv run dbx-vocareum-upload <course dir> --env-file ~/courses/my-course/.env
```

The command hashes every local file, uploads one archive, waits for Vocareum's write to finish, then downloads each file back and compares. It reports success only when every hash agrees.

The rest of the flags are for the cases the default does not cover:

- **`--target`** is the folder under `/voc` to write into. Default `scripts`, which is where Vocareum reads the lifecycle scripts from.
- **`--name`** renames a single file on the Vocareum side, so it cannot be combined with a directory or with several paths.
- **`--dry-run`** prints the archive and stops before writing. It reads no configuration, so it works before a part is known.
- **`--force`** uploads even when every remote file already matches, which proves the write path still works without changing what is deployed.
- **`--no-verify`** skips the download-and-compare step. Only worth it when a file is too large to fetch back, since nothing else in the API confirms what Vocareum stored.

Four rules, each of which exists because breaking it produced a lab that failed in a way that named nothing:

- **Upload the course directory whole, never the one file you changed.** The hooks chain through `VOC_CUSTOM_DATA` and share `voclib.sh`, so a partial write leaves a lab that one version built and a different version tears down. The notebooks are in the same set because `user_setup.sh` imports them by name from beside itself.
- **Notebooks go to `scripts` with the hooks, not to `startercode`.** Vocareum executes only `workspace_init.sh`, `lab_setup.sh`, `user_setup.sh`, `lab_end.sh`, and `grade.sh` out of `/voc/scripts`. An `.ipynb` sitting there is inert until `user_setup.sh` reaches for it.
- **Exit `3` means the file that came back differs from the file that was sent.** Do not start a lab against it. Re-upload until it exits `0`.
- **The next Start Lab uses whatever is up there.** Vocareum caches nothing, so there is no publish step and no propagation delay to wait out.

[`docs/deploying.md`](docs/deploying.md) is the full runbook for the upload tool, including deploying from a course's own repository. [`src/dbx_vocareum_tools/labruntime/README.md`](src/dbx_vocareum_tools/labruntime/README.md) is the canonical account of what is in the set and how the notebooks reach a student.

### Pre-warming compute for a class

```bash
uv run dbx-vocareum-pool                          # read only
uv run dbx-vocareum-pool --create --min-idle 5    # before the class
uv run dbx-vocareum-pool --delete --yes           # after the class
```

An instance pool keeps booted boxes on standby, so a student waits less for a cluster. Idle boxes cost money from create to delete, and nothing deletes the pool automatically. [`docs/pre-warm.md`](docs/pre-warm.md) covers the flags, the defaults, and what was measured.

### Checking a lab, and ending it

```bash
uv run dbx-vocareum-diagnose                 # read the session, start nothing
uv run dbx-vocareum-session                  # status only
uv run dbx-vocareum-session --end --yes      # end it
```

**End your session when you are done testing.** This is the cheapest and most necessary command here. A lab session runs `user_setup.sh`, which creates billable Databricks compute, and a session that failed keeps billing until somebody ends it. `--end` is what ends it, `--yes` skips the confirmation prompt, and without `--end` the tool only reads. `--action` exists and takes only `end`. Vocareum documents a second action alongside it and does not say how the two differ, so the tool refuses it with exit `3` rather than guessing on a lab holding a student's work.

`dbx-vocareum-diagnose` starts nothing unless you pass `--start`. **Pass `--start` only when a launch is worth spending**, because it runs `user_setup.sh` for this student and that is what creates billable compute. Its other flags all read:

- **`--raw-record`** prints the whole session record, redacted. It is how the undocumented Databricks record shape gets mapped.
- **`--skip-workspace`** reads the Vocareum side and makes no Databricks calls.
- **`--principals`** compares the account and workspace identity planes for this session's user and group. Off by default because it needs `DATABRICKS_ACCOUNT_ID` and makes account-scoped calls. Turn it on when the record reports a principal Databricks could not find.
- **`--expect MANIFEST`** is the ship gate, described next.
- **`--json`** also prints the observations as JSON.
- **`--env-file`** points at a different `.env`.

[`docs/diagnosing.md`](docs/diagnosing.md) is the guide to reading what it tells you.

### The ship gate

```bash
uv run dbx-vocareum-diagnose --expect expected.json
```

`--expect` reads a course's JSON manifest, looks up every object named in it, and exits `3` if any of them is missing. It exists because `voccustomdata.txt` does not surface on a Databricks part. Measured on one part, the hooks ran, the cluster came up, the notebook imported, and the session record carried no `voccustomdata` key at all. So the question "did the lab come up" is answered from the workspace rather than from anything the hooks said about themselves.

The manifest belongs to the course, not to this repository. It is one JSON object whose keys are `catalogs`, `schemas`, `tables`, `volumes`, `warehouses`, `pipelines`, `genie_spaces`, and `workspace_paths`, each holding a list of names in whatever form that kind is addressed by:

```json
{
  "catalogs": ["${COURSE_CATALOG}"],
  "schemas": ["${COURSE_CATALOG}.sales"],
  "tables": ["${COURSE_CATALOG}.sales.orders"],
  "warehouses": ["course_warehouse"],
  "pipelines": ["Course ETL"],
  "workspace_paths": ["/Shared/course/etl"]
}
```

An unknown key is an error rather than something to skip, because a misspelled key that is quietly ignored is an expectation nobody checks, and the gate would then report a pass it never measured.

Three ways to fail and one way to pass. An object that is absent fails. An object whose lookup did not complete fails too, because a gate that passed on "the call did not finish" would pass on a revoked credential. A manifest that never ran fails, because empty lists otherwise read as nothing missing. Only `PRESENT` counts, and absence is reported as `ABSENT_OR_HIDDEN`, since Databricks answers a caller with no permission by saying the object does not exist.

### Checking permissions

```bash
uv run dbx-vocareum-probe --self-test        # offline, no calls
uv run dbx-vocareum-probe --catalog          # the measurable operations, then exit
uv run dbx-vocareum-probe --all              # every read-only call, as the service principal
uv run dbx-vocareum-probe --source token --probes-file probes/lab-user.json
```

What to measure, and as whom:

- **`--probe SERVICE:OPERATION`** measures one catalog operation and is repeatable. `--catalog` prints the names it accepts.
- **`--probes-file PATH`** reads a JSON list of probe objects, each with an operation and optionally a method, path, query, body, scope, and note.
- **`--all`** measures every read-only operation in the catalog. With nothing named at all the probe falls back to `scim:Me`, the one call every later probe depends on.
- **`--source`** is `service-principal` or `token`. The first uses the OAuth credentials from `.env` and describes the tooling. The second uses `DATABRICKS_LAB_USER_TOKEN` and describes the student.
- **`--host`** and **`--expect-workspace-id`** override `DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID` for one run. The host must resolve to that workspace ID or the probe refuses.
- **`--allow-mutating`** permits probes whose method is not a read. Without it, a probe list holding one is refused before any call is made.
- **`--json`** also prints results as JSON, and **`--env-file`** points at a different `.env`.

Every report names which principal it describes. Record results in [`docs/permissions.md`](docs/permissions.md) and nowhere else.

### Reclaiming leftover home folders

```bash
uv run dbx-vocareum-orphans                 # read only. Every /Users/labuser* folder, LIVE or ORPHAN
uv run dbx-vocareum-orphans --delete        # delete the orphans, after an interactive confirmation
uv run dbx-vocareum-orphans --delete --yes  # skip the confirmation, for scripted runs
```

Vocareum deletes each session's Databricks principal when the session ends, but not the `/Users/<identity>` folder Databricks made for it. Run this between classes. Nothing automatic ever calls it. [`docs/databricks-labs.md`](docs/databricks-labs.md#the-home-folder-outlives-the-identity) has the full account and the safety rules.

### Exit codes

Every tool returns `0` for success, `1` for a configuration problem, and `2` for an API or guard failure. The third code is the one that differs, and it is the one worth reading:

| Tool | `2` | `3` |
| --- | --- | --- |
| `dbx-vocareum-init` | not used, it makes no API call | not used |
| `dbx-vocareum-upload` | the API call failed | a file that came back differs from the file that was sent |
| `dbx-vocareum-probe` | a guard refused, so nothing was reported | the offline self-test failed |
| `dbx-vocareum-diagnose` | the diagnostic could not complete | `--expect` found an object missing |
| `dbx-vocareum-session` | the API call failed | the action was refused as unmeasured |
| `dbx-vocareum-pool` | a guard refused, so nothing was created or deleted | not used |
| `dbx-vocareum-orphans` | a guard refused, so nothing was reported or deleted | the deletion was refused |

## Where to start

| I want to | Read |
| --- | --- |
| Understand the four scripts, what each one is handed, and what a finished session leaves behind | [`docs/databricks-labs.md`](docs/databricks-labs.md) |
| Know what the service principal may do, and what a student may do | [`docs/permissions.md`](docs/permissions.md) |
| Get the lifecycle scripts up to Vocareum | [`docs/deploying.md`](docs/deploying.md) |
| Get a course notebook in front of a student | [`src/dbx_vocareum_tools/labruntime/README.md`](src/dbx_vocareum_tools/labruntime/README.md) |
| Find out why a lab came up empty or landed the student nowhere | [`docs/diagnosing.md`](docs/diagnosing.md) |
| Cut how long a student waits for compute | [`docs/pre-warm.md`](docs/pre-warm.md) |
| Call the Vocareum REST API from code | [`docs/vocareum-api.md`](docs/vocareum-api.md) |

`docs/permissions.md` is the single source of truth for what each principal permits. Nothing else in the repository states a permission result, so add measurements there and link to them from anywhere else.

## The two principals

A Vocareum Databricks lab has two principals, and they are not close to equivalent. The service principal holds Account admin and is what every tool uses by default. The lab user is the student, and the only way to measure one is a personal access token from inside a live session, passed as `DATABRICKS_LAB_USER_TOKEN`. [`docs/permissions.md`](docs/permissions.md) has the full account, including why Databricks hides objects rather than refusing access to them.

## Files

| File | What it is |
| --- | --- |
| `docs/` | The reference documents linked above |
| `probes/` | Probe lists for `dbx-vocareum-probe --probes-file`. `lab-user.json` is the set to run inside a live student session |
| `src/dbx_vocareum_tools/` | Installable Python package behind the seven command-line entry points, including shared configuration, API, transaction, redaction, the expectation manifest, and the Vocareum lifecycle contract |
| `src/dbx_vocareum_tools/labruntime/` | What runs inside Vocareum: the injected runtime `voclib.sh` and `voclab.py`, and the `template/` that `dbx-vocareum-init` copies into a course |
| `tests/` | Offline regression tests for configuration, redaction, host handling, error classification, the lifecycle contract, and upload packaging |
| `pyproject.toml` | Python dependencies, command entry points, and Ruff/pytest configuration |
| `.env.example` | Template for the `.env` the tools read. Copy it, fill it in, and never commit the result |
| `LICENSE` | The Apache License 2.0 |

The service principal's OAuth secret never goes into git. Keep it in a password manager, and let it reach the tools through `.env`, which the repository ignores. Committed documents write real account, workspace, and metastore identifiers as `${PLACEHOLDER}` and resolve them from `.env`.

## Quickstart

```bash
cp .env.example .env        # then fill in the Vocareum IDs and the Databricks service principal
uv run dbx-vocareum-probe --self-test
uv run dbx-vocareum-upload <course dir> --dry-run
uv run dbx-vocareum-upload <course dir> --env-file ../dbx-vocareum/.env
uv run dbx-vocareum-diagnose --expect expected.json --env-file ../dbx-vocareum/.env
```

The tools are a [uv](https://docs.astral.sh/uv/) project, so the first `uv run` resolves the locked environment automatically.

For development:

```bash
uv sync --group dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

`dbx-vocareum-diagnose` reads the existing session by default and starts nothing. Pass `--start` only when a launch is worth spending: starting a Databricks session runs `user_setup.sh`, which is what creates billable compute.

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
