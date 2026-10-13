# How a Vocareum Databricks lab is built

What Vocareum runs for a Databricks lab, in what order, with what environment, and what a script can hand back. The source is Vocareum's [Databricks scripts](https://help.vocareum.com/en/articles/7045516-databricks-scripts) article, which is the whole of the published contract. Most of what follows is that published contract rather than a measurement, and the difference is stated wherever it matters. Four things on this page are measurements: [Vocareum creates the workspace](#vocareum-creates-the-workspace), the [Session log](#the-session-log-in-the-console-which-is-where-a-script-is-heard), the absence of [`voccustomdata.txt` from the session record](#voccustomdatatxt-which-the-next-hook-reads), and the [home folders a deleted lab identity leaves behind](#the-home-folder-outlives-the-identity).

The companion documents are [`permissions.md`](permissions.md), which covers what each principal is allowed to do inside the workspace, and [`vocareum-api.md`](vocareum-api.md), which covers how to make Vocareum do something from code.

Identifiers appear as `${PLACEHOLDER}`, resolved from `.env`.

## There is no template

An AWS Cloud Lab reads `lab.template` from `/voc/private` and runs CloudFormation with it. A Databricks lab has no equivalent hook. What it has instead is four shell scripts in `/voc/scripts`, executed at four points in the session, and the only way anything gets created in the student's workspace is that one of them creates it.

The practical differences that follow from this are not small:

| | AWS Cloud Lab | Databricks lab |
| --- | --- | --- |
| Provisioning artifact | `lab.template`, declarative | four shell scripts, imperative |
| Isolation | one AWS account per student | one user and one group inside a shared workspace |
| Failure record | CloudFormation stack events, listable for 90 days | the console [Session log](#the-session-log-in-the-console-which-is-where-a-script-is-heard), read by hand, plus whatever the failing script wrote to `voccustomdata.txt` |
| Failure detail | `ResourceStatusReason` per resource | an exit code and captured output in the Session log, and one `error_code` and one `message` if the script wrote them |
| Rollback | CloudFormation rolls the stack back | none. A script that fails halfway leaves half a lab |
| Credential in the session record | temporary AWS access key, secret, and session token | none published |

The last two rows are what shape this repository. A script that fails halfway has to say so itself, because nothing a program can read will say it for you, and [`diagnosing.md`](diagnosing.md) is built around reading the workspace state as a substitute for a stack event log. The Session log is the exception and it is a human channel: somebody has to open the console and look.

## The four hooks

Uploaded as a set to `/voc/scripts` by `dbx-vocareum-upload`. [`deploying.md`](deploying.md) is the runbook.

| Script | When it runs | Required |
| --- | --- | --- |
| `workspace_init.sh` | once, after Vocareum creates a workspace | no |
| `lab_setup.sh` | when a lab is pre-warmed from the fleet dashboard | no |
| `user_setup.sh` | when a student starts the lab, after `lab_setup.sh` if that exists | **yes** |
| `lab_end.sh` | when the lab ends, whether stopped or terminated | no |

A lab with only `user_setup.sh` is a complete lab. `LIFECYCLE_SCRIPTS` in `src/dbx_vocareum_tools/databricks.py` is the machine-readable copy of this table, and `tests/test_selftest_cases.py` holds the order and the required flag to it, so the table and the code cannot drift.

**An absent hook is skipped in silence.** Vocareum reports nothing when a script it would have run is not there, so a lab missing `user_setup.sh` starts successfully and does nothing at all. That failure looks exactly like a lab whose script ran and created nothing, which is why `dbx-vocareum-diagnose` lists what is actually deployed in `/voc/scripts` before it interprets anything else.

Exit status is the whole of the success signal. Zero means the script succeeded, and anything else means it failed. On a non-zero exit Vocareum reads `error_code` and `message` out of `voccustomdata.txt` and shows them, so a script that exits 1 without writing them produces a lab that failed for no stated reason as far as any tool here can tell. It is not entirely mute: the console [Session log](#the-session-log-in-the-console-which-is-where-a-script-is-heard) carries the exit code and the script's captured output whether or not the script wrote anything itself.

### `grade.sh` is not one of the four

`grade.sh` runs when a student clicks Submit, and only when the assignment allows submissions. It is not part of the session lifecycle, it cannot redirect the student, and it receives two extra variables, `vocGradeFile` and `vocReportFile`. It is held apart from the four in code as `GRADE_SCRIPT` for that reason, and `tests/test_selftest_cases.py` asserts it stays apart.

## What each hook is handed

Every variable Vocareum sets is prefixed `VOC`. Which ones exist depends on the hook:

| Variable | `workspace_init` | `lab_setup` | `user_setup` | `lab_end` |
| --- | --- | --- | --- | --- |
| `VOC_DB_WORKSPACE_URL` | yes | yes | yes | yes |
| `VOC_PARTID` | yes | yes | | |
| `VOC_LABID` | yes | yes | | |
| `VOC_RESOURCE_TAGS` | yes | yes | yes | |
| `VOC_CUSTOM_DATA` | | yes | yes | yes |
| `VOC_IPC_DATA_FILE` | | yes | yes | |
| `VOC_DB_USER_EMAIL` | | yes | yes | yes |
| `VOC_DB_GROUP_NAME` | | | yes | yes |
| `VOC_END_LAB_BEHAVIOR` | | | | yes |

Three absences in that table decide where work can go.

**`workspace_init.sh` gets no user and no group.** It runs once for the workspace, before any student exists, so nothing per-student can happen there. What belongs there is the shared work: a secret scope, a cluster policy, a catalog, a SQL warehouse. Thirty students then share one copy rather than making thirty.

**`lab_setup.sh` gets `VOC_DB_USER_EMAIL` but not `VOC_DB_GROUP_NAME`.** Vocareum's table lists the user variable for this hook, which is surprising for a script documented as running when a lab is pre-warmed rather than when a student arrives. Whether it is actually populated at that point is unmeasured, and it is the difference between `lab_setup.sh` being per-lab and being per-user. The template's version of the script records which it turns out to be, as `user_scope=per-user` or `user_scope=per-lab` in its custom data, because the answer decides whether slow, expensive setup can live there instead of in `user_setup.sh`.

**`workspace_init.sh` and `lab_end.sh` get no `VOC_IPC_DATA_FILE`.** Only the two middle hooks can choose what the student sees on entry.

## The channels out of a script

**Vocareum does not expose a lifecycle script's standard output through its REST API.** That negative is settled rather than assumed: about 90 read-only GETs found no log endpoint, no log file anywhere in the exposed file tree, and no per-student dimension on the one endpoint that enumerates files at all, so a hook's own working directory is not addressable from the API in any form. [`vocareum-api.md`](vocareum-api.md#there-is-no-log-endpoint-and-the-file-tree-is-not-the-container-filesystem) records the whole of it, including the trap that a `userid` parameter is accepted and silently ignored.

The output is not discarded, though. It goes somewhere no program can reach.

### The Session log in the console, which is where a script is heard

Every session carries a **Session log** in the Vocareum console. It records the LAUNCH, START, INIT, and END transitions, and for a lifecycle script that exits non-zero it carries the **exit code and the captured output**. It is per session, it is read by hand, and it is the only place a script's own words survive. **So when a lab fails, open the Session log first.** [`diagnosing.md`](diagnosing.md#open-the-session-log-before-you-run-anything) quotes a real one, and covers the separate Init log that `workspace_init.sh` writes to.

### `voccustomdata.txt`, which the next hook reads

A script writes flat JSON to `voccustomdata.txt` in its working directory. Vocareum's contract says it does three things with it: forwards it to the next script in the sequence as `VOC_CUSTOM_DATA`, reads `error_code` and `message` out of it when the script exits non-zero, and carries it in the session record.

It is the hook's own record of its outcome, so `src/dbx_vocareum_tools/labruntime/voclib.sh` writes it with care. `voc_custom_write` writes the file, `voc_custom_field` escapes a value into it, and `voc_fail` writes both failure keys and exits 1 in a single call so that a failure path cannot forget one of them.

**The third of those three was measured not to happen.** On one Databricks part, a live session whose cluster and imported notebook both existed had a session record whose `databricks` block carried six identity fields and no `voccustomdata` key at all. So a hook's `error_code` and `message` do not reach the REST API. A lab is verified by other means: the ship gate, `dbx-vocareum-diagnose --expect`, reads the objects the hooks were meant to build, and the beacon below reaches the student's browser. The forwarding to the next script and the non-zero-exit read are separate mechanisms and are not in doubt.

The chaining is worth using deliberately. The template's `lab_end.sh` echoes the `VOC_CUSTOM_DATA` it received back into its own output, which is what lets a setup run and its matching end run be identified as the same session afterwards.

### `$VOC_IPC_DATA_FILE`, which redirects the student

Writing `{"notebook_url": "..."}` to the path in `VOC_IPC_DATA_FILE` sends the student to that URL when they enter the lab. Without it they land in a bare workspace and have to find the notebook themselves.

Two hooks receive the variable and only `user_setup.sh` writes it here. Two scripts writing the same file is a race whose winner Vocareum does not document, and the cost of losing it is that the whole class lands somewhere unintended.

The URL has to be built from the origin rather than by appending to `VOC_DB_WORKSPACE_URL`, because that variable already carries the `?o=` workspace parameter. Appending produced `https://…/?o=3333333333333333/?o=3333333333333333#workspace/…` on the first local run. `voc_origin` in `voclib.sh` strips the query and fragment first, and it is the shell counterpart of `normalize_host` in `src/dbx_vocareum_tools/databricks.py` for exactly that reason.

### The beacon, which is that same field used as a log

Because `notebook_url` is the only field a script controls that reaches anyone, `voclib.sh` uses it twice. `voc_ipc_beacon` writes a URL whose fragment carries `voc-diag=<state>`, the script name, a UTC timestamp, and the **names** of the `VOC_` variables the hook was handed:

```
https://dbc-….cloud.databricks.com/#voc-diag=MISSING_WORKSPACE_URL&script=user_setup.sh&at=2026-08-05T21:45:53Z&vars=VOC_DB_GROUP_NAME+VOC_DB_USER_EMAIL+VOC_IPC_DATA_FILE
```

`user_setup.sh` writes `voc-diag=entered` before its first guard, `voc_fail` overwrites it with the error code, and the landing URL overwrites it on the success path. Vocareum redirects the student's browser to that URL and does not publish it in the REST session record, so the beacon is read in the browser's address bar. It reads three ways instead of two: a landing URL means the hook finished, a beacon means it ran and stopped and says where, and nothing at all means Vocareum never invoked the hook, which is a fault in the part's configuration rather than in any script. [`diagnosing.md`](diagnosing.md#the-beacon-reaches-the-browser-not-the-api) has the measurement and why a rehearsal is launched from the web UI.

Two constraints on it. The state goes in a **fragment** because a fragment is never sent to a server, so a beacon that does get followed lands on an ordinary page instead of putting diagnostic text into somebody's access log. And `vars=` carries **names only**: the URL is read and quoted by people diagnosing labs, and `VOC_DB_SP_SECRET` is one of the names that list can contain.

## `VOC_END_LAB_BEHAVIOR` decides what cleanup means

The part carries an End lab behavior setting, and `lab_end.sh` receives its value:

| Value | What Vocareum does | What a cleanup script may do |
| --- | --- | --- |
| `stop` | preserves the student's data for their next session | remove billable compute. Removing notebooks destroys work the student expects to find |
| `terminate` | reverts the environment to its original state | anything. Cleanup is free |

Reading this variable is not optional for a script that deletes anything. A `lab_end.sh` that treats every ending as a teardown will delete a student's work between two sessions of the same lab. The template's version branches on it in a `case`, matching case-insensitively and with whitespace removed, so `Terminate` and `terminate\n` reach the terminate branch rather than the default.

**The value is not guaranteed to arrive, and the default is `stop`.** `GET .../parts/{id}` returns no End lab behavior field under any spelling, so nothing on the Vocareum side confirms a value exists to be handed over, and a hook has to be written for the case where it arrives unset or empty. Removing nothing on an unfamiliar value has the cost backwards. A per-student cluster was measured still `RUNNING` after its session record read `state: Terminated`. A cluster left running bills until somebody notices. A cluster terminated in error costs a restart and keeps its configuration and its installed libraries. So unset, empty, and unrecognized all reclaim the compute and delete nothing, `END_LAB_BEHAVIOR_FALLBACK` in `src/dbx_vocareum_tools/databricks.py` names that default once, and `tests/test_lab_end.py` runs the hook for all five cases.

**What arrived is recorded.** `lab_end.sh` writes `end_lab_behavior` into `voccustomdata.txt` as the exact value it was handed, empty included, alongside `end_behavior_status`, which is one of `stop`, `terminate`, `empty`, `unset`, or `unrecognized`, and `cleanup_command`, which is what was actually asked of the workspace. The value and the interpretation are separate fields because the run above could not be diagnosed from a record that carried only the branch taken. Note that `lab_end.sh` receives no `VOC_IPC_DATA_FILE`, so it cannot write a beacon: the custom data is the only channel it has.

## The home folder outlives the identity

The section above is about what a hook may delete. This one is about the object no hook can reach at all.

Vocareum mints a Databricks principal per session and deletes it when the session ends. [`pre-warm.md`](pre-warm.md#what-does-not-work-warming-a-cluster-in-lab_setupsh) records that from the other side, because it is what killed pre-warming: the same student was `labuser10000002_1785948805` on one session and `labuser10000002_1785972938` on the next, since the local part of the address carries the session start epoch. A fresh address every session is what makes the name unusable as an idempotency key. It is also what leaves this behind.

Databricks creates `/Users/<identity>` when it provisions the identity, and `user_setup.sh` imports the course's notebooks into it. **Deleting the identity does not delete the folder.** No hook deletes it either, and `lab_end.sh` is the wrong place to try: when `lab_end.sh` runs the identity is still live and the folder is still the student's, and on `stop` the student's work is preserved for their next session, which is the exact case the table above says must not be cleaned up. The folder becomes an **orphan** one step after the last hook has finished, and no hook is running then.

**Measured in one workspace: nine `/Users/labuser*` folders against exactly one live SCIM identity, so eight of the nine were orphans.** Each held a copy of the course's imported notebooks.

Three things around that reading are inference rather than measurement, and they are worth keeping apart from it:

- **That they accumulate one per session, without end.** The reading is one workspace at one moment. It is consistent with one folder per session and nothing here has watched the count climb across a term.
- **That nothing on the Vocareum side reclaims them.** No reclaim was observed, and there is no published lifecycle to check it against: [`permissions.md`](permissions.md#vocareums-own-roles-three-things-that-are-not-the-same-thing) records how little of Vocareum's identity model is published.
- **That they are harmless in cost.** Notebook source in a workspace folder is not billed compute, and no bill has been read to confirm it. The cost that is not in doubt is legibility. Nine home folders for one student makes the workspace tree unreadable, and finding the live student means decoding the epoch in nine names.

Nobody loses work to this. The identity that could open an orphan no longer exists, and a returning student gets a new address and a new folder, which is the measurement in the first paragraph.

### `dbx-vocareum-orphans`, which reclaims them

```bash
uv run dbx-vocareum-orphans                 # read only. Every /Users/labuser* folder, marked LIVE or ORPHAN
uv run dbx-vocareum-orphans --delete        # delete the orphans, after an interactive confirmation
uv run dbx-vocareum-orphans --delete --yes  # skip the confirmation, for scripted runs
```

**Reading is the default and deleting is the flag.** With no `--delete` the tool lists and changes nothing, so the bare command is safe to run at any time. That is the same shape `dbx-vocareum-pool` and `dbx-vocareum-session` take, and for the same reason: the read is the thing an instructor wants most often, and it should not require remembering which flag is the harmless one.

**SCIM is read first, and it is what decides.** The tool lists the workspace's SCIM identities before it lists the folders, and a folder whose name matches a live identity is marked `LIVE` and is never a candidate for deletion. The order is the safety property rather than an implementation detail. A folder is only a name, and the name of a live student and the name of a student who left last week are the same shape, so the only thing that can tell them apart is the identity plane. Deriving liveness from the folder would delete a running student's notebooks and report a successful cleanup.

Two flags:

- **`--prefix PREFIX`** is which folder names count as lab identities. Default `labuser`. Every lab identity measured in this repository is `labuser<digits>_<epoch>@vocareum.com`, so the default matches Vocareum's own shape and nothing else in the workspace. Widening it is how a real user's home folder gets swept up, which is why the default is the narrow literal rather than something like a match on every folder without a live identity.
- **`--env-file PATH`** points at a different `.env`. Defaults to the repository root, like every other tool here.

**The same three guards as `dbx-vocareum-pool` run before anything is reported:** credential presence, host match, then identity. `DATABRICKS_HOST` is what decides which workspace is read, and Vocareum creates a workspace per part, so a run pointed at the wrong one deletes folders in an environment nobody meant to touch and reports success doing it. The credential guard is there because the SDK's provider chain will otherwise fall through to `DATABRICKS_TOKEN`, `~/.databrickscfg`, or a cached CLI login without saying so. [`permissions.md`](permissions.md#the-guards-and-why-the-probe-refuses-rather-than-reports) has the full argument for refusing rather than reporting.

**Exit codes: `0` ok, `1` config, `2` API, `3` refused.** The `3` matches `dbx-vocareum-session`, the repository's other destructive tool, and means the tool declined to act rather than tried and failed.

**It is instructor-run, and nothing automatic ever calls it.** No hook invokes it: the four hooks run inside Vocareum against a `/voc/scripts` that carries `voclib.sh` and `voclab.py` and no copy of this package, and `voclab.py` is standard library only. Nothing here schedules it either. It belongs between classes, next to `dbx-vocareum-pool --delete`, which is the other thing somebody has to remember to run because nothing runs it for them.

## Vocareum creates the workspace

`workspace_init.sh` is documented as running "once, after Vocareum creates a workspace," which left open whether Vocareum stands a workspace up itself or attaches to one you registered. Measured: it stands one up itself, named `voc-ws-<digits>-<digits>`, using the Account admin role on its service principal. A workspace built by hand through the account console is a different environment, and no lab uses it. What manual setup supplies is the account-level objects every later workspace reuses: the credential configuration, the storage configuration, and the metastore.

Three consequences follow.

**`DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID` have to name the Vocareum-created workspace.** Pointed anywhere else, every probe result, every permission row, and every leftover-resource check describes an environment no student ever enters, and reports success while doing it. Step 5 of `dbx-vocareum-diagnose` exists to catch exactly this: it compares `DATABRICKS_HOST` against the `workspace_url` in the session record and prints a note when they disagree.

**There will be more than one workspace.** Vocareum creates one per part, which is what makes idempotency in `workspace_init.sh` a requirement rather than a precaution.

**`account:ListWorkspaces` is the cheapest way to see the whole picture.** It enumerates every `voc-ws-*` workspace in the account in one call, which answers both how many Vocareum has created and how close the account is to its tier ceiling.

Standing a workspace up is also an identity operation, and Vocareum performs all of them as a single Databricks Account admin while being the SAML identity provider for the entire account. [`permissions.md`](permissions.md#vocareums-own-roles-three-things-that-are-not-the-same-thing) has Vocareum's role and identity model.

## Two Databricks facts that bound the lab

**The account tier caps active workspaces, not users per workspace.** Standard allows 3 active workspaces, Premium 10, Enterprise 50, per Vocareum's [Setting Up Databricks Resources](https://help.vocareum.com/en/articles/12030031-setting-up-databricks-resources). It is a per-account limit on workspaces, not a limit on how many students fit in one workspace.

The per-student ceiling is a separate Vocareum setting on the part, returned as `databricks_maxusers` by `GET .../parts/{id}`. It read 250 on the part measured, so a thirty-student class is nowhere near it.

Which of the two binds follows from the section above: Vocareum creates a workspace per part, so the account-level cap on active workspaces is what limits concurrent labs. That settles what `workspace_init.sh` has to be. It runs once per Vocareum-created workspace, there will be more than one of those, and so everything it creates has to be idempotent rather than assumed unique.

**Outbound non-HTTP ports need egress.** A lab cluster reaching an external service on a port such as 7687 depends on Vocareum's security group allowing it. Vocareum's [2026-04-23 release notes](https://help.vocareum.com/en/articles/14744154-2026-04-23-release-notes) record adding port 7687.

## References

- [Vocareum: Databricks scripts](https://help.vocareum.com/en/articles/7045516-databricks-scripts): the four hooks, their execution order, the environment each receives, and the two output files. This is the whole of the published contract
- [Vocareum: Using Custom Grading Scripts in Vocareum Labs](https://help.vocareum.com/en/articles/14120536-using-custom-grading-scripts-in-vocareum-labs): the `/voc/scripts/grade.sh` entry point, `vocGradeFile`, and `vocReportFile`
- [Vocareum: Setting Up Databricks Resources](https://help.vocareum.com/en/articles/12030031-setting-up-databricks-resources): the AWS and Databricks account configuration this depends on. Also the source for Vocareum being the account-wide SAML IdP, for its one Account admin service principal, and for the active-workspace caps by tier
- [Vocareum: Creating a Databricks assignment](https://help.vocareum.com/en/articles/7045503-creating-a-databricks-assignment): the part configuration, and an "Admin Access to Databricks" section that is the nearest the docs come to naming the lab admin identity without naming it
- [Vocareum: Customer-Managed AWS account for Databricks, VM labs and Compute](https://help.vocareum.com/en/articles/3843032-customer-managed-aws-account-for-databricks-vm-labs-and-compute): why a course runs in a dedicated AWS account
