# Diagnosing a lab that did not come up

A student reports that the lab started and there is nothing in it, or that it did not start at all. You have no access to their session and no console open in front of you. `dbx-vocareum-diagnose` recovers the reason from the Vocareum API and the Databricks workspace.

## Open the Session log before you run anything

The AWS version of this tool reads CloudFormation stack events, which list every resource, its status, and a `ResourceStatusReason` for each failure, and stay listable for 90 days. A Databricks lab has none of that. **Vocareum does not expose a lifecycle script's standard output through its REST API.** That is settled rather than assumed: about 90 read-only GETs found no log endpoint, no log file anywhere in the exposed file tree, and no per-student dimension on the one endpoint that enumerates files at all. [`vocareum-api.md`](vocareum-api.md#there-is-no-log-endpoint-and-the-file-tree-is-not-the-container-filesystem) records the whole negative, including the trap that a `userid` parameter is accepted and silently ignored, so a tool built on it would read the instructor's content and report it as every student's.

**The output is not lost, though. It is somewhere no program can reach.** Every session carries a **Session log** in the Vocareum console. It records the LAUNCH, START, INIT, and END transitions, and for a lifecycle script that exits non-zero it carries the **exit code and the captured output**. Verbatim from a live session:

```
LAUNCH initiated  START successful  INIT successful  END successful (Session Timer Expires)
End Lab script failed [exit-code: 1, output: /voc/scripts/lab_end.sh: line 34: ./voclib.sh: No such file or directory ]
```

That is a complete diagnosis in one line, of a bug this tool could not have found. The session record showed a session that started and ended normally, because from Vocareum's side it did. The log is per session, it is in the console, and it has to be read by hand.

`dbx-vocareum-diagnose` cannot read it and never will. Use it for what happens after: whether the workspace matches what the log says should be in it, and for the failures where no script got far enough to print anything.

## And the workspace Init log, for `workspace_init.sh`

The Session log above is per session, so it covers the three hooks a student's session runs. `workspace_init.sh` runs once when Vocareum creates the workspace, before any student exists, and its output goes somewhere else.

**The admin Workspaces page carries it, and it is a link rather than a label.** Each row's Scripts column reads `Init Exit Status: N`, and clicking that text opens the hook's full transcript, every `voc_log` line in order. That is the whole console narration. It does not contradict the section above: **that was measured against the REST API and remains true there.** No endpoint returns this. It is in the admin UI, it is per workspace rather than per student, and like the Session log it has to be read by hand.

It is worth opening first when a lab comes up empty and the student's own session log says nothing, because `workspace_init.sh` builds the shared objects, such as a catalog, a warehouse or a pipeline, and a student never sees the hook that failed to build them.

**Do not trust the number in `Init Exit Status`.** Measured in the same session, on the same run: `workspace_init.sh` called `voc_fail`, the transcript ended in `FAILED SQL_FAILED`, `voc_fail` ends in `exit 1`, and the page displayed `Init Exit Status: 0`. Whether Vocareum reads a different exit code than the hook returns, or reports the status of the wrapper around it, is unmeasured. What is measured is that a `0` there is not evidence of anything. Open the log and read the last line. `Rerun Init` on the same row re-runs the hook, which is the only way to re-trigger it: redeploying `/voc/scripts` does not.

## The beacon reaches the browser, not the API

The third channel, and the only one a script controls. `voc_ipc_beacon` in `voclib.sh` writes a URL into `$VOC_IPC_DATA_FILE` whose fragment names what went wrong, and `voc_fail` writes one on the way out. From a lab launched in the web UI, the browser landed here:

```
https://${WORKSPACE_HOST}/#voc-diag=NOTEBOOK_IMPORT_FAILED&script=user_setup.sh
  &at=2026-08-08T17:40:49Z&vars=VOC_ASSIGNMENTID+VOC_COURSEID+...
```

**Vocareum consumes `notebook_url` and redirects the student's browser to it, and never publishes that field through the REST API.** Both halves are measured. Readings of the session record found no `notebook_url` and no `voccustomdata`, on a live session whose cluster and imported notebook both existed. The record is the wrong instrument, and the only reader that sees the beacon is a browser.

**So a rehearsal has to be launched from the web UI.** `dbx-vocareum-diagnose --start` runs the same lifecycle and discards the one artifact that says what happened.

**Read it as three states.** A landing URL on a notebook means the hook finished. A `#voc-diag=` fragment means it ran and stopped, and `script=` and the code say where. A page with neither means Vocareum never invoked the hook at all, which points at the part's configuration rather than at anything in the scripts.

**The fragment carries the code and not the message.** It is built from four fixed parts, so `voc_fail`'s `message` is not in it. Take the error code from here and the sentence explaining it from the Session log. A fragment is used because a fragment is never sent to a server, so a diagnostic that gets followed or pasted does not put lab internals into somebody's access log. For the same reason `vars` lists variable names and never values: `VOC_DB_SP_SECRET` is one of the names it can return.

**What `vars` is good for.** It is the only measured statement of what a hook is actually handed, as opposed to what the documentation says it is handed. A fragment like the one above is how `VOC_DB_API_TOKEN` was found to be present in `user_setup.sh`'s environment, along with `VOC_DB_SP_APPLICATION_ID` and `VOC_DB_SP_SECRET`. Names only, so it says a variable was set and nothing about whether the value works.

## What there is to read from code

Three things survive into the API, and the six steps of the diagnostic are built on them:

1. **What the script wrote to `voccustomdata.txt` before exiting.** Vocareum reads `error_code` and `message` out of it on a non-zero exit, and its published contract says the session record carries it. `src/dbx_vocareum_tools/labruntime/voclib.sh` writes both keys and exits in a single call because of that contract. **Measured on one part, the session record does not carry it.** A live session whose cluster and notebook both existed had a record with no `voccustomdata` key at any path. [`databricks-labs.md`](databricks-labs.md#voccustomdatatxt-which-the-next-hook-reads) has the evidence. The beacon above and the ship gate, `--expect`, are how a lab is verified instead.
2. **What the script left behind in the workspace.** A folder that exists, a group the student was never added to, a cluster that was never created. The workspace state is the log.
3. **Whether `vocipcdata.txt` ever received a `notebook_url`.** A student who lands in a bare workspace rather than on the course notebook has a `user_setup.sh` that exited before writing it. `voclib.sh` writes a **beacon** through this field and step 4 decodes it. **Read step 4's verdict on this field with care:** the record does not carry `notebook_url` on a Databricks part even when the hook wrote one, measured twice, so an empty field here is not evidence that the hook failed. The section above has where the beacon does arrive. The step 6 findings that read an empty `notebook_url` overstate what that absence proves.

## Running it

```bash
uv run dbx-vocareum-diagnose                    # read the existing session
uv run dbx-vocareum-diagnose --start            # start the session first, then read it
uv run dbx-vocareum-diagnose --raw-record       # print the whole record, redacted
uv run dbx-vocareum-diagnose --skip-workspace   # Vocareum side only, no Databricks calls
uv run dbx-vocareum-diagnose --principals       # add the account-vs-workspace identity read
uv run dbx-vocareum-diagnose --json             # add the observations as JSON
```

Ending a session is a separate tool, because ending is the one operation here that destroys something:

```bash
uv run dbx-vocareum-session                     # what is this session doing, and is it spending
uv run dbx-vocareum-session --end               # end it, with a typed confirmation
uv run dbx-vocareum-session --end --yes         # end it, for scripted runs
```

**Read it before you end it, and read `--principals` before that.** Ending a session revokes its Databricks principals, so the account-level permission assignments that explain a failed session disappear with it. Capture `dbx-vocareum-diagnose --principals` first, then end.

**A failed session is not a finished session.** `totaltime` on a session recorded as `Failed` was still climbing more than an hour later. `Failed` and `Terminated` are different states and only one of them is free, so the tool reports anything that is not a known ended state as running. `EndLab` is the transitional state between a successful end call and the record settling to `Terminated`, and it is named rather than reported as a failure.

`terminate` is refused. Vocareum documents it beside `end` and does not say how they differ, nothing here has measured it, and the failure mode of guessing is a student's work.

For the diagnostic, exit `0` means it ran, `1` a configuration problem, `2` that the API calls could not complete it. A finding is not a failure of the tool, so a run that identifies eight problems still exits `0`.

**`--start` is off by default, which is the opposite of the AWS tool.** Starting a Databricks session runs `user_setup.sh`, and `user_setup.sh` is what creates billable compute. Reading an existing session costs nothing. Use `--start` when the question is why a fresh start fails, and expect it to be slow: the record poll waits 10 seconds between attempts because a session has to claim a workspace seat, add the user, and run the hook before a notebook URL appears.

Every response body is redacted before printing. The AWS variant of a Vocareum session record carries temporary credentials, a federated console sign-in token, and an unencrypted SSH private key. Vocareum documents the Databricks variant nowhere, so it is treated as though it carries the same. `--raw-record` prints the whole record redacted, which is how the shape got mapped, and it is also why step 4 reports the key path each field was found at rather than just the value.

**`--raw-record` maps the shape, and the shape is now measured.** [`vocareum-api.md`](vocareum-api.md#the-measured-shape) has it. A record from a session that came up carried no `voccustomdata` key, so the channels that report on a hook are the beacon, `--expect`, and the console Session log.

## One-time setup

Copy `.env.example` to `.env` and fill in five values for the Vocareum side and three for the Databricks side. Three more are optional.

| Variable | Where it comes from |
| --- | --- |
| `VOCAREUM_API_KEY` | Vocareum account settings, Personal Access Tokens. Requires Organization Admin, shown once. Its scope must cover the course below: a course-scoped token belonging to a different course fails in a way that reads as a missing course rather than a wrong token, and [`vocareum-api.md`](vocareum-api.md#obstacles-each-of-which-will-recur) has the symptom |
| `VOCAREUM_COURSE_ID` | the info icon on the assignment in the Vocareum UI. Numeric. The `code=vc_2_0_…` value in a course's `editcourse` URL is a different identifier and the API does not accept it |
| `VOCAREUM_ASSIGNMENT_ID` | the same info icon |
| `VOCAREUM_PART_ID` | the same info icon. Must be the part whose labtype is Databricks |
| `VOCAREUM_USER_ID` | a real enrolled student. Vocareum publishes no endpoint returning the caller's own ID |
| `VOCAREUM_ORG_NUMBER` | optional. Step 4B needs it and is skipped without it |
| `DATABRICKS_HOST` | the workspace URL, scheme included, no trailing slash |
| `DATABRICKS_WORKSPACE_ID` | optional. The numeric workspace ID from the account console's Workspaces page. `--principals` resolves it through the SDK when unset, and step 5 uses it to catch a student placed in a different workspace |
| `DATABRICKS_ACCOUNT_ID` | optional. `--principals` needs it for the account-plane reads |
| `DATABRICKS_CLIENT_ID` | the service principal's application ID, labelled UUID in the account console |
| `DATABRICKS_CLIENT_SECRET` | generated once in the account console and not recoverable |

Real environment variables win over `.env`, so the same command runs unchanged in CI. `.env` is git-ignored and must stay that way: the OAuth secret in it is an Account admin credential.

Without `DATABRICKS_HOST` the run still works and reports only the Vocareum side. Step 5 says so rather than failing, because the Vocareum half of the diagnosis is often the half that answers the question.

## The six steps, two of which have a B side

1. **The part.** Reads the part configuration and confirms its labtype is Databricks. A part with the wrong labtype produces a lab that starts and runs nothing, and no other symptom.
2. **`/voc/scripts`.** Lists what is actually deployed and names which lifecycle hooks are present. This runs before anything is interpreted because an absent hook is skipped in silence, so "the script did nothing" and "there is no script" look identical from the outside.
3. **The session.** Skipped unless `--start`.
4. **The session record.** Reports which of the expected fields it carried and the key path each sat at, then pulls out the workspace URL, the lab user email, the group name, and the notebook URL, decoding the last as a beacon when it is one. A field it found but that holds nothing prints as `present but empty`, which is a different fact from `not present` and the one that tells a never-started session apart from a wrong part.
4B. **Vocareum's own workspace record.** `GET /orgs/{org}/databricks_workspaces`, filtered to the configured part. This is where Databricks reports a failure back to Vocareum in words, and `error_info` is the only place those words exist. Needs `VOCAREUM_ORG_NUMBER`, and the step is skipped without it rather than the run refusing to start. The record belongs to the part rather than to a student, so it describes every student on the part, and it carries no timestamp.
5. **The workspace.** With the service principal: does that user exist, which groups is it in, what entitlements does it hold, what compute is named for or owned by it, which secret scopes exist, and what is under `/Users/<email>`.
5B. **The principals, only with `--principals`.** Reads the account-level permission assignments for the workspace, then looks the lab user and the group up in account SCIM and workspace SCIM and compares the two ids. Same id means the workspace is identity federated and the two planes are one principal. Different ids, or one side empty, means they are two, which is the shape that produces `Principal not found in account` verbatim. It then lists every `labuser*` and `Group-lab*` principal on both planes, decodes the unix epoch each Vocareum name ends in, and marks which hold an assignment, so a message about an earlier session can be told from one about this one.
6. **The interpretation.** The gap between what the record says and what the workspace holds, against a table of known causes.

Between steps 4 and 5 the tool compares the workspace URL in the record with `DATABRICKS_HOST` and prints a note when they differ. That case matters more than it looks: it means the student is in a different workspace from the one the tool is pointed at, so everything step 5 reports describes the wrong environment.

### The findings step 6 can produce

| Finding | What it means |
| --- | --- |
| The part is not a Databricks lab | The labtype is something else. An admin has to enable Databricks in the course's Cloud Resources settings before the option appears |
| A required lifecycle script is not deployed | `user_setup.sh` is absent from `/voc/scripts`. Upload the course directory |
| A lifecycle script reported its own failure | `voccustomdata.txt` carries an `error_code` or a `message`. Read this before anything else on the list |
| A lifecycle script started and did not finish | An entry beacon survived in `notebook_url`. Vocareum did invoke the hook, and it reached neither its landing URL nor `voc_fail`, so something between them died under `set -e` |
| A lifecycle script failed and said so through the beacon | A failure beacon survived, carrying the same error code `voc_fail` wrote, through the channel that is proven to reach the record |
| This session has never started | `sessionstart` and `state` are both empty, so nothing below is evidence about the scripts. Start a session with `--start` and diagnose that. **A lab launched from an instructor view prints this row and its hooks did run.** The two are indistinguishable from the record: an instructor-view launch creates the workspace, the lab identity, the cluster and the notebooks, and publishes no readable session record at all, [measured](vocareum-api.md#two-400s-from-this-endpoint-that-are-not-about-the-session). Confirm the launch was by an enrolled student before reading this as nothing having run |
| Vocareum reports this session as failed | `state` reads `Failed`. Every field a running lab fills in is empty because of that, so the findings under it describe the wreckage rather than adding to it |
| No notebook URL was handed back | `user_setup.sh` exited before writing `$VOC_IPC_DATA_FILE`, or wrote something that was not JSON with a `notebook_url` key. Once the beacon is deployed, an empty field here means Vocareum never invoked the hook at all |
| Vocareum recorded a Databricks-side failure on this workspace | `error_info` on Vocareum's own workspace record, which is where Databricks reports back to Vocareum in words. It carries no timestamp and names no principal, so it cannot be attributed to a session from the API alone |
| The lab user does not exist in the workspace | Vocareum adds the student before running `user_setup.sh`, so this is a failure on Vocareum's side of the handoff. Check that the service principal still holds Account admin |
| The lab user is in no group | The student has an identity that can enter the workspace and do nothing in it. Grant against `$VOC_DB_GROUP_NAME`, not against the individual user |
| The lab user has no workspace folder | Nothing was imported. A `mkdirs` or `import` call failed silently, which happens when a script runs calls in sequence and exits 0 regardless |
| No compute belongs to the lab user | Expected when the lab hands out a shared or serverless cluster. A problem when `user_setup.sh` is meant to create one: check the `allow-cluster-create` entitlement on the group and the workspace cluster quota |

Six more come from `--principals` and appear only when it was passed:

| Finding | What it means |
| --- | --- |
| This workspace is not identity federated | The account-level `permissionassignments` endpoint does not answer for this workspace, so no account-plane call can address a workspace-local principal and Vocareum's add-user step cannot work as written |
| The lab user is two principals, not one | The same `userName` resolves to a different id on each plane. This is the shape that produces `Principal not found in account` verbatim |
| The lab user has no account-level counterpart | Account SCIM matched nothing for the address the workspace holds |
| The lab user holds no permission assignment on this workspace | The add-user step did not complete for this student, whatever the workspace directory says |
| The session's group is the principal that was not found | The group resolves in workspace SCIM, not in account SCIM, and holds no assignment |
| The session's group is workspace-local and assigned regardless | Same absence, opposite consequence: the assignment exists anyway, so the message is true and did not stop anything. **This is the measured state of a healthy part.** A lab group not resolving at account level is Vocareum's normal shape rather than a defect |

The last three are gated on `workspace_reached`, so they appear only when the service principal actually authenticated against the workspace. Without that gate they would fire on every run with an unreachable host and report a lab problem that is really a credential problem.

Two more gates work the same way, and all three exist because an unknown must not be reported as an absence:

- **"A required lifecycle script is not deployed" is gated on `scripts_listed`.** Step 2 sets that only when it read a listing it could parse. A listing that failed, or that came back as a [zip-download transaction](vocareum-api.md#listing-a-directory) rather than as file names, leaves it false and this finding is withheld. It is the finding [read as explaining everything at once](#reading-a-result), and its advice is to upload scripts, so producing it from a failed read tells you to overwrite hooks that are already deployed and correct.
- **"This session has never started" replaces "No notebook URL was handed back" rather than joining it.** A session that never ran has no notebook URL and no `voccustomdata.txt`, so the hook-shaped findings are all true and all misattributed: they describe a script that was never invoked. An instructor-view launch produces the same absence for a different reason, which is why the row above says to check who launched it before believing the substitution. When neither `sessionstart` nor `state` is present at all, the tool reports nothing about starting either, because absent fields are not a claim that the session did not start.

A run that matches nothing prints so explicitly: "No known cause matched. That is not the same as the lab being correct." An empty finding list means every check the tool knows how to make came back consistent, which is a narrower claim than a healthy lab, and the two are not worth conflating in a message someone will forward to a student.

## Reading a result

Take the findings in the order printed. They are ordered most decisive first, and the ordering is deliberate: a script that reported its own failure explains everything below it, and a missing `user_setup.sh` explains everything at once.

Two findings are ambiguous by construction and the guidance says which way to read them. "No compute belongs to the lab user" is the expected state for a lab that hands out a shared cluster. "The lab user has no workspace folder" is expected for a lab that imports nothing. Neither can be resolved without knowing what the lab was meant to build, which the tool does not know.

For a permission result, stop here and go to [`permissions.md`](permissions.md). That file is the only place in the repository that states what a principal is allowed to do, and this diagnostic reports as the service principal, which holds Account admin. A call that succeeds in step 5 says nothing about whether a student could make it.

## What this cannot tell you

**Why a script failed, when the script did not say.** A hook that exits 1 without writing `voccustomdata.txt` produces a lab that failed for no reason this tool can state, and no API call recovers what it printed. Go to the [Session log](#open-the-session-log-before-you-run-anything), which carries the exit code and the output whether or not the script wrote anything itself. The fix on the code side is on the writing side rather than the reading side, which is why `voc_fail` in `src/dbx_vocareum_tools/labruntime/voclib.sh` writes all four of its fields and exits in one call: a failure path cannot forget one of them.

**Whether a script ran at all, as distinct from ran and did nothing.** Step 2 narrows this by listing what is deployed, but a deployed hook that exits 0 immediately leaves the same trace as one Vocareum never invoked. A script that always names itself in its custom data closes the gap, and the template hooks do: both the success path and `voc_fail` write `script` and `ran_at`, so a record carrying `script` proves the hook ran and a record carrying nothing proves it did not. The one failure this cannot survive is a hook that dies before it can source `voclib.sh`, which is why each hook carries its own `MISSING_VOCLIB` branch written in plain `printf`.

**What a student's own credential permits.** The diagnostic uses the service principal because that is the only credential there is. Measuring the student's ceiling needs `DATABRICKS_LAB_USER_TOKEN`, a personal access token generated from inside a live session, and `dbx-vocareum-probe --source token` is the tool that reads it.
