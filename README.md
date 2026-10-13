# dbx-vocareum

dbx-vocareum helps you run Databricks courses on Vocareum. Vocareum gives each student a user and a group inside a shared Databricks workspace. When the student clicks Start Lab, four shell scripts that you upload build the student's compute and copy in the course notebooks. The tools here let a coding agent, such as Claude Code, handle the work around that. The agent can scaffold and upload the course, check that each lab came up, and clean up when the class is over.

## How this differs from aws-vocareum-utils

[aws-vocareum-utils](https://github.com/neo4j-partners/aws-vocareum-utils) does the same job for AWS cloud labs. The two platforms build a lab in different ways, so the tools differ too.

| | AWS cloud lab | Databricks lab |
| --- | --- | --- |
| What a student gets | An AWS account of their own | A user and a group in a shared workspace |
| What builds the lab | One CloudFormation template | Four shell scripts, called hooks |
| Where it lives in Vocareum | `/voc/private/lab.template` | `/voc/scripts` |
| Unit of upload | One file | The whole set, since the hooks depend on each other |
| Where notebooks come from | `/voc/startercode` | `/voc/scripts`, imported by `user_setup.sh` |
| How a lab is checked | `vocareum-expect` against `expected.json` | `dbx-vocareum-diagnose --expect expected.json` |

The last row is a measurement. A notebook uploaded to `/voc/startercode` reaches the Vocareum environment beside Databricks. The student's Databricks folder stays empty.

## Before you start

These pieces are set up once per Databricks account, not once per course. Vocareum's [Setting Up Databricks Resources](https://help.vocareum.com/en/articles/12030031-setting-up-databricks-resources) is the full procedure.

- **Vocareum organization:** An admin has to turn on Databricks in the course's Cloud Resources settings. Until then, Databricks is not offered as a lab type.
- **Vocareum API token:** You generate it under Personal Access Tokens in your Vocareum account settings. It needs Organization Admin, and it is shown only once. Its scope must cover your course.
- **Databricks account:** The account tier limits how many workspaces can be active at once. Standard allows 3, Premium 10, and Enterprise 50. Vocareum creates one workspace per part, so this limit caps how many labs run at once.
- **AWS account:** This account holds the workspace root bucket and the `vocareumvm`, `vocareum-db`, and `vocareum-db-uc` roles.
- **Account-level Databricks objects:** You create the credential configuration, the storage configuration, and the metastore by hand. Every workspace Vocareum creates reuses them.
- **Service principal:** It holds Account admin. Every tool here uses its OAuth credentials by default. [`docs/permissions.md`](docs/permissions.md) explains how far it differs from a student.

## What a course needs

A course lives in its own repository. It holds a small set of files, and this repository supplies the tools and the shared runtime.

- **The four hooks:** `workspace_init.sh`, `lab_setup.sh`, `user_setup.sh`, and `lab_end.sh` are shell scripts Vocareum runs by name. Only `user_setup.sh` is required. `dbx-vocareum-init` writes all four, and the course owns them from then on.
- **`course.env`:** This file holds the course's values, such as the runtime, node type, libraries, catalog, and notebook list. Every key is optional. It is uploaded with the hooks, so nothing secret belongs in it.
- **Notebooks:** These are what the student is handed. They sit beside the hooks, often as symlinks, and `VOC_COURSE_NOTEBOOKS` names them. The first one named is where the student lands.
- **The course's own program:** A course that builds a catalog, tables, or a pipeline adds its own Python file, such as `workshop.py`. A hook calls it with `voc_python workshop.py provision`. It uses the standard library only, against Python 3.9.
- **`expected.json`:** This file lists what a working lab must hold. `dbx-vocareum-diagnose --expect` checks the workspace against it. It sits outside the uploaded directory, because the deployer reads it and the hooks never do.
- **The tooling dependency:** The course's `pyproject.toml` lists `dbx-vocareum-tools` in its dev group. That puts all seven commands on `uv run` inside the course.

Two files are deliberately absent from a course. `voclib.sh` and `voclab.py` ship inside this package, and `dbx-vocareum-upload` adds them to every upload. A course that holds its own copy of either fails the upload. `.env` is absent too: it holds the admin credentials and stays in this repository.

## How the Neo4j workshop does it

The [databricks-neo4j-workshop](https://github.com/neo4j-partners/databricks-neo4j-workshop) repository is a working course built on these tools.

- **Lab directory:** `lab/` holds the four hooks, `workshop.py`, `course.env`, and a `courseware/` folder with the pipeline code and data.
- **Notebooks:** The six lab folders live at the repository root. `lab/` reaches them through symlinks, so there is one copy of each notebook.
- **Workspace build:** `workspace_init.sh` creates the SQL warehouse. It then runs `voc_python workshop.py provision`, which creates the catalog, schemas, volume, data, pipeline, and Genie grants.
- **Pre-warm:** `lab_setup.sh` builds nothing. A cluster warmed there would carry a name the student's session never looks for. [`docs/pre-warm.md`](docs/pre-warm.md) has the measurement.
- **Student session:** `user_setup.sh` creates or restarts the student's cluster, imports the notebooks, and writes the landing page.
- **Cleanup:** `lab_end.sh` terminates the cluster on a stop and deletes it on a terminate. It leaves notebooks alone in both cases.
- **Ship gate:** `expected.json` sits at the repository root, outside `lab/`.
- **Neo4j:** Each student creates their own Aura Free database in Lab 1. It runs outside the Databricks workspace.
- **One-way link:** The workshop depends on this repository. This repository knows nothing about the workshop.

The workshop's `vocareum/SETUP_GUIDE.md` covers what is specific to that course, such as the manual steps before class.

## How a course goes live

Each step below is one subsection with commands you can paste. Run them from the course repository. Every command there needs `--env-file`, so set it once:

```bash
export VOC_ENV=~/projects/dbx-vocareum/.env
```

Only a launched session proves what a student gets, so steps 8 and 9 are required.

### 1. Create the course in Vocareum

An instructor does this step in the Vocareum web UI. No command does it for you.

- **Course, assignment, and part:** Create all three. The part's lab type must be Databricks.
- **Part settings:** Set the session length and the budget on the part. A lab that will not start is a budget question before it is a script question.
- **Test student:** Enroll one student so the session tools have a user to act as. A plus address such as `you+student1@example.com` keeps it apart from your instructor account. You can also enroll through the API. [`docs/vocareum-api.md`](docs/vocareum-api.md#enrolling-a-diagnostic-student) has the call.

Cloning an existing course is a shortcut. A clone carries the part settings and everything in `/voc/scripts`. It does not carry the roster, so enroll a test student in the clone.

### 2. Fill in `.env`

`.env` lives in this repository, not in the course. Copy the example and fill it in.

```bash
cp .env.example .env
```

- **Vocareum IDs:** Open the assignment and click the info icon. Copy the course, assignment, and part IDs. These are numeric. The `code=vc_2_0_…` value in a course URL is a different identifier, and the API refuses it.
- **User ID:** `VOCAREUM_USER_ID` is the test student from step 1. Vocareum has no endpoint that returns your own ID.
- **Databricks credentials:** The account ID and service principal values come from the Databricks account console. `.env.example` says where each one is.
- **Workspace:** Leave `DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID` for step 8. The workspace does not exist yet.

[`docs/diagnosing.md`](docs/diagnosing.md#one-time-setup) has the full variable table.

### 3. Add the tooling to the course

In the course's `pyproject.toml`:

```toml
[dependency-groups]
dev = [
    "dbx-vocareum-tools @ git+https://github.com/<owner>/dbx-vocareum.git",
]
```

```bash
uv sync --group dev
```

`uv.lock` pins whichever commit was current when the lock was written. When the tooling changes, bump it:

```bash
uv sync --group dev --upgrade-package dbx-vocareum-tools
```

A runtime fix changes no file in the course. So a course that skips the bump uploads the old runtime, and the upload still verifies.

### 4. Scaffold the hooks

```bash
uv run dbx-vocareum-init lab/
```

This writes the four hooks, `course.env.example`, and a README. `--force` rewrites the hooks later. It never overwrites `course.env`, because that file holds values a person typed.

### 5. Write the course files

[What a course needs](#what-a-course-needs) explains each one.

```bash
cp lab/course.env.example lab/course.env
```

- **`course.env`:** Fill in what the course needs. The file's own comments say what each key costs.
- **Notebooks:** Put them in `lab/` or symlink them in. Name them in `VOC_COURSE_NOTEBOOKS`.
- **`expected.json`:** List what the course builds, at the repository root. [The ship gate](#the-ship-gate) has the format.
- **Own program:** Add `lab/workshop.py` and call it from a hook, if the course builds anything beyond a cluster and notebooks.

### 6. Check offline

These checks make no Vocareum or Databricks call.

```bash
bash -n lab/*.sh                     # parse check, runs nothing
shellcheck lab/*.sh                  # if installed
uv run dbx-vocareum-probe --self-test
uv run dbx-vocareum-upload lab/ --dry-run
```

A script that cannot parse uploads just as cleanly as one that can. The failure then shows up minutes later inside a student session.

### 7. Upload

The upload sends `lab/` as one archive. It then downloads each file back and compares hashes.

```bash
uv run dbx-vocareum-upload lab/ --env-file "$VOC_ENV"
```

- **Exit `0`:** Every file was uploaded and verified.
- **Exit `3`:** A file that came back differs from what was sent. Do not start a lab against it. Upload again until it exits `0`.
- **Whole directory:** Always upload `lab/`, never one changed file. The hooks share state and a runtime, so a partial upload leaves a lab built by one version and torn down by another.
- **No publish step:** The next Start Lab uses whatever is in `/voc/scripts`.

### 8. Build the workspace

The first session on a new part creates the Databricks workspace and runs `workspace_init.sh`. Sign in as the test student and click Start Lab in the Vocareum web UI.

- **Fill in the workspace:** Open the Workspaces page in the Databricks account console. Find the `voc-ws-*` workspace Vocareum created. Copy its URL and numeric ID into `DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID` in `.env`. Pointed anywhere else, every check describes a workspace no student uses.
- **Read the init transcript:** On the Vocareum admin Workspaces page, click the `Init Exit Status` text in the workspace's row. It opens the full transcript of `workspace_init.sh`. Read the last line, not the number. The number has shown `0` for a run that failed.
- **Rerun Init:** Redeploying `/voc/scripts` does not rerun `workspace_init.sh`. The Rerun Init button on the same row does.

### 9. Test a live lab

Check the shared workspace first, then what the student got.

```bash
uv run dbx-vocareum-diagnose --expect expected.json --env-file "$VOC_ENV"
```

`--expect` exits `0` when the workspace holds everything in `expected.json`. If the course builds a pipeline, wait for its update to finish before you trust a failure.

Then check the student's side in the browser:

- **Landing page:** A student who lands on the first notebook means the hooks finished.
- **Diagnostic beacon:** A URL ending in `#voc-diag=<code>&script=<hook>` means a hook failed. The fragment names the error code and the hook.
- **Neither:** A page with no notebook and no beacon means Vocareum never ran the hook. Check the part's lab type and that `user_setup.sh` is in `/voc/scripts`.
- **Session log:** The Vocareum console's Session log carries a failing hook's exit code and output. It is the only place a hook's printed output appears.
- **Launch from the web UI:** A launch from the CLI throws away the redirect, which is the only error channel a hook has.

[`docs/diagnosing.md`](docs/diagnosing.md) covers each channel in full.

### 10. End the session

A session bills compute from the moment it starts, and a failed one keeps billing.

```bash
uv run dbx-vocareum-session --env-file "$VOC_ENV"               # status only
uv run dbx-vocareum-session --end --yes --env-file "$VOC_ENV"
```

This ends the session of `VOCAREUM_USER_ID`. If you launched as a different user, such as the instructor in Student View, end that session in the browser.

### 11. Update

Commit the change. Bump the tooling pin if the tooling changed. Then repeat steps 6, 7, 9, and 10. If `workspace_init.sh` or what it builds changed, press Rerun Init as in step 8.

### 12. Roll back

There is no rollback command. Check out the course's previous commit, upload `lab/` again, then come back. Check out the whole commit, not just `lab/`, because notebooks reached through symlinks live outside it.

```bash
git switch --detach <previous commit>
uv run dbx-vocareum-upload lab/ --env-file "$VOC_ENV"
git switch -
```

The Vocareum API has no delete. A file that the bad upload added stays in `/voc/scripts`. It does nothing unless its name is one of the four hooks or `grade.sh`.

## Running a class

### Before class

```bash
uv run dbx-vocareum-pool --env-file "$VOC_ENV"                    # read only
uv run dbx-vocareum-pool --create --min-idle 5 --env-file "$VOC_ENV"
```

A student waits about 16 minutes for a cluster. An instance pool keeps booted machines on standby, so the wait drops. Idle machines bill from create to delete. [`docs/pre-warm.md`](docs/pre-warm.md) covers the flags and what was measured.

### During class

```bash
uv run dbx-vocareum-diagnose --env-file "$VOC_ENV"                # read the session, start nothing
```

[`docs/diagnosing.md`](docs/diagnosing.md) explains how to read what it reports.

### After class

Order matters. Let the student sessions end first, because a leftover folder appears only once Vocareum deletes that session's identity. A student session ends when its timer runs out or when the student stops the lab. `dbx-vocareum-session --end` ends only the session of `VOCAREUM_USER_ID`, which is your test student.

```bash
uv run dbx-vocareum-session --end --yes --env-file "$VOC_ENV"    # your test session
uv run dbx-vocareum-pool --delete --yes --env-file "$VOC_ENV"
uv run dbx-vocareum-orphans --env-file "$VOC_ENV"                 # list LIVE and ORPHAN folders
uv run dbx-vocareum-orphans --delete --env-file "$VOC_ENV"        # asks before it deletes
```

- **Pool:** Nothing deletes the pool automatically. It bills until you run `--delete`.
- **Home folders:** Vocareum deletes each session's Databricks identity when the session ends. It does not delete the `/Users/<identity>` folder. [`docs/databricks-labs.md`](docs/databricks-labs.md#the-home-folder-outlives-the-identity) has the safety rules.
- **Scripted runs:** `--yes` skips the confirmation on `orphans --delete`.

## Tool reference

Seven commands cover all of it.

| I want to | Run |
| --- | --- |
| Scaffold a course's lab directory | `uv run dbx-vocareum-init <course dir>` |
| Push a change to the lab | `uv run dbx-vocareum-upload <course dir>` |
| Cut how long students wait for compute | `uv run dbx-vocareum-pool --create --min-idle 5` |
| See what a session is doing | `uv run dbx-vocareum-diagnose` |
| Prove a course is ready to ship | `uv run dbx-vocareum-diagnose --expect expected.json` |
| Stop paying for a test session | `uv run dbx-vocareum-session --end --yes` |
| Stop paying for the pool | `uv run dbx-vocareum-pool --delete --yes` |
| Check what a principal may do | `uv run dbx-vocareum-probe --all` |
| Find and reclaim leftover home folders | `uv run dbx-vocareum-orphans` |

### What costs money, and what destroys

- **Spends money:** `dbx-vocareum-diagnose --start` starts a session, which runs `user_setup.sh`, which creates billable compute. `dbx-vocareum-pool --create --min-idle N` holds N booted machines until somebody deletes the pool.
- **Changes every later lab:** `dbx-vocareum-upload` writes to the part that every later Start Lab reads.
- **Destroys:** `dbx-vocareum-session --end`, `dbx-vocareum-pool --delete`, and `dbx-vocareum-orphans --delete` each need that flag to act. Without it, all three only read.
- **Reads only:** Everything else reads and changes nothing.

### Configuration and `--env-file`

Configuration comes from `.env` at the root of this repository. A real environment variable beats `.env`, and a blank value counts as missing. Every tool except `dbx-vocareum-init` takes `--env-file PATH`.

**Type the path carefully.** A path that names no file loads as an empty file. The tool then reports every key missing, not a bad path. A trailing dot did exactly that. The message prints the path it read, so check that first.

`dbx-vocareum-init` makes no API call and reads no configuration.

### Upload flags

- **`--target`:** This is the folder under `/voc` to write into. The default is `scripts`.
- **`--name`:** This renames a single file on the Vocareum side. It cannot be combined with a directory or several paths.
- **`--dry-run`:** This prints the archive and stops. It reads no configuration, so it works before a part is known.
- **`--force`:** This uploads even when every remote file already matches. It proves the write path still works.
- **`--no-verify`:** This skips the download and compare. Use it only when a file is too large to fetch back.

[`docs/deploying.md`](docs/deploying.md) has the full upload runbook, including what is skipped and why.

### Diagnose flags

`dbx-vocareum-diagnose` starts nothing unless you pass `--start`. Every other flag only reads.

- **`--start`:** This starts a session. Pass it only when a launch is worth spending.
- **`--raw-record`:** This prints the whole session record, redacted.
- **`--skip-workspace`:** This reads the Vocareum side and makes no Databricks call.
- **`--principals`:** This compares the account and workspace views of this session's user and group. It needs `DATABRICKS_ACCOUNT_ID`.
- **`--expect MANIFEST`:** This is the ship gate, described next.
- **`--json`:** This also prints the results as JSON.

### Session flags

`dbx-vocareum-session` only reads unless you pass `--end`. `--yes` skips the confirmation. `--action` exists and accepts only `end`. Vocareum documents a second action without saying how it differs, so the tool refuses it with exit `3`.

### The ship gate

```bash
uv run dbx-vocareum-diagnose --expect expected.json
```

`--expect` reads a course's manifest, looks up every object it names, and exits `3` if any is missing. It exists because a hook's own report does not reach the API on a Databricks part. Measured on one part, the hooks ran and the cluster and notebook existed, yet the session record had no `voccustomdata` key. So the gate checks the workspace instead.

The manifest belongs to the course. It is one JSON object. Its keys are `catalogs`, `schemas`, `tables`, `volumes`, `warehouses`, `pipelines`, `genie_spaces`, and `workspace_paths`. Each holds a list of names in the form that kind of object uses.

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

- **Unknown keys:** An unknown key is an error. A misspelled key that was skipped would be a check nobody runs.
- **`PRESENT`:** This is the only result that passes.
- **`ABSENT_OR_HIDDEN`:** This fails. Databricks tells a caller with no permission that the object does not exist, so absent and hidden look the same.
- **`NOT_CHECKED`:** This fails too. A gate that passed when a call did not finish would pass on a revoked credential.
- **Unparsed manifest:** This fails, because empty lists would otherwise read as nothing missing.
- **What it does not cover:** The gate reads the shared workspace. Whether a student got their cluster and notebooks is checked by launching a session, as in step 9.

### Checking permissions

```bash
uv run dbx-vocareum-probe --self-test        # offline, no calls
uv run dbx-vocareum-probe --catalog          # list the measurable operations
uv run dbx-vocareum-probe --all              # every read-only call, as the service principal
uv run dbx-vocareum-probe --source token --probes-file probes/lab-user.json
```

- **`--probe SERVICE:OPERATION`:** This measures one catalog operation. It can repeat.
- **`--probes-file PATH`:** This reads a JSON list of probe objects.
- **`--all`:** This measures every read-only operation. With nothing named, the probe runs only `scim:Me`.
- **`--source`:** This is `service-principal` or `token`. The first uses the OAuth credentials in `.env`. The second uses `DATABRICKS_LAB_USER_TOKEN` and describes the student.
- **`--host` and `--expect-workspace-id`:** These override `.env` for one run. The host must match that workspace ID, or the probe refuses.
- **`--allow-mutating`:** This permits probes that write. Without it, a list holding one is refused before any call.

Every report names which principal it describes. Record results in [`docs/permissions.md`](docs/permissions.md) and nowhere else.

### Exit codes

Every tool returns `0` for success, `1` for a configuration problem, and `2` for an API or guard failure. The third code differs by tool:

| Tool | `2` | `3` |
| --- | --- | --- |
| `dbx-vocareum-init` | not used, it makes no API call | not used |
| `dbx-vocareum-upload` | the API call failed | a file that came back differs from the file that was sent |
| `dbx-vocareum-probe` | a guard refused, so nothing was reported | the offline self-test failed |
| `dbx-vocareum-diagnose` | the diagnostic could not complete | `--expect` found an object missing |
| `dbx-vocareum-session` | the API call failed | the action was refused as unmeasured |
| `dbx-vocareum-pool` | a guard refused, so nothing was created or deleted | not used |
| `dbx-vocareum-orphans` | a guard refused, so nothing was reported or deleted | the deletion was refused |

## The two principals

A Vocareum Databricks lab has two principals, and they are far apart.

- **Service principal:** It holds Account admin, and every tool uses it by default. It answers whether the hooks will work.
- **Lab user:** This is the student. A Vocareum session publishes no Databricks credential for it. The only way to measure a student is a personal access token made inside a live session, passed as `DATABRICKS_LAB_USER_TOKEN`.

[`docs/permissions.md`](docs/permissions.md) has the full account. It also explains why Databricks hides objects rather than refusing access.

## Where to start

| I want to | Read |
| --- | --- |
| Understand the four hooks, what each is handed, and what a session leaves behind | [`docs/databricks-labs.md`](docs/databricks-labs.md) |
| Know what the service principal may do, and what a student may do | [`docs/permissions.md`](docs/permissions.md) |
| Get the hooks up to Vocareum | [`docs/deploying.md`](docs/deploying.md) |
| Get a course notebook in front of a student | [`src/dbx_vocareum_tools/labruntime/README.md`](src/dbx_vocareum_tools/labruntime/README.md) |
| Find out why a lab came up empty | [`docs/diagnosing.md`](docs/diagnosing.md) |
| Cut how long a student waits for compute | [`docs/pre-warm.md`](docs/pre-warm.md) |
| Call the Vocareum REST API from code | [`docs/vocareum-api.md`](docs/vocareum-api.md) |

## Files

| File | What it is |
| --- | --- |
| `docs/` | The reference documents linked above |
| `probes/` | Probe lists for `dbx-vocareum-probe --probes-file`. `lab-user.json` is the set to run inside a live student session |
| `src/dbx_vocareum_tools/` | The installable Python package behind the seven commands |
| `src/dbx_vocareum_tools/labruntime/` | What runs inside Vocareum: the shared runtime `voclib.sh` and `voclab.py`, and the `template/` that `dbx-vocareum-init` copies into a course |
| `tests/` | Offline tests. No test makes a network call |
| `pyproject.toml` | Python dependencies, command entry points, and Ruff and pytest settings |
| `.env.example` | The template for `.env`. Copy it, fill it in, and never commit the result |
| `LICENSE` | The Apache License 2.0 |

The service principal's OAuth secret never goes into git. Keep it in a password manager and let it reach the tools through `.env`, which git ignores. Committed documents write real account, workspace, and metastore IDs as `${PLACEHOLDER}`.

## Quickstart

```bash
cp .env.example .env        # then fill in the Vocareum IDs and the service principal
uv run dbx-vocareum-probe --self-test
uv run dbx-vocareum-upload <course dir> --dry-run
uv run dbx-vocareum-diagnose
```

The tools are a [uv](https://docs.astral.sh/uv/) project, so the first `uv run` sets up the locked environment.

For development:

```bash
uv sync --group dev
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

## License

Apache License 2.0. See [`LICENSE`](LICENSE).
