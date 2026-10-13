# Driving the Vocareum REST API from code

Every call this repository makes against `api.vocareum.com`, the request shapes that work, and the ones that look like they should and do not.

**Two kinds of row live here.** The transport-level behavior was measured against an AWS Cloud Lab course on the same API with the same kind of token. That covers the base URL, the auth header, the Cloudflare front, the path conventions, the body shapes, and the transaction protocol. It carries over unchanged, because the API does not know what labtype a part has until it reads the part. Everything specific to Databricks was measured against a real Databricks part. What is still **unmeasured** is what a start call returns for a Databricks part, and the section on starting a session says so.

This document covers the Vocareum side: how to make Vocareum do something. The companions are [`databricks-labs.md`](databricks-labs.md), which covers what Vocareum then runs, and [`permissions.md`](permissions.md), which covers what each principal is permitted to do inside the workspace.

Identifiers appear as `${PLACEHOLDER}`, resolved from `.env`.

## Basics

| Property | Value |
| --- | --- |
| Base URL | `https://api.vocareum.com/api/v2` |
| Auth header | `Authorization: Token <personal access token>` |
| Token source | Vocareum account settings, Personal Access Token tab. Requires Organization Admin. Displayed once at creation and never again |
| Token scope | Chosen in the Generate New Token dialog from a `global`/`course` dropdown that defaults to Global. A course-scoped token reaches that one course and nothing else. Fixed at creation: the token list offers copy and delete and no way to re-scope, so widening a token means generating a replacement. The scope appears in no successful response, so it is invisible until a call fails. See [Obstacles](#obstacles-each-of-which-will-recur) |
| Token permissions | A third axis, set by checkbox in the same dialog and also fixed at creation. Grouped by resource, with `list`, `create`, and `update` under `courses`, `assignments`, and `parts`, and `list`, `enroll`, and `drop` under `users`. So a token can be correctly scoped and still be refused one endpoint. The upload path needs `parts: update`, not merely `list` |
| Rate limit | 800 requests per minute, not configurable |
| Content type | `application/json` on every request carrying a body |

Successful writes are frequently asynchronous. They answer `202` with a `transactionid` rather than `200`, and the work has not happened yet when the response arrives. See [Transactions](#transactions).

## Obstacles, each of which will recur

| Obstacle | Behavior | Resolution |
| --- | --- | --- |
| Cloudflare User-Agent ban | Cloudflare fronts `api.vocareum.com` and bans the default `Python-urllib/x.y` User-Agent, answering `HTTP 403` with a full marketing HTML page and Cloudflare error 1010 before the request reaches the API. The token is never evaluated, so it reads as an authentication failure and is not one | Send any other User-Agent value. `src/dbx_vocareum_tools/client.py` sets one on every call |
| Trailing slashes | `/courses/{id}/assignments/` returns `Invalid Request`. `/courses/{id}/assignments` returns 200 | Never append a trailing slash to a v2 path |
| `include_dropped=1` | Rejected. The parameter takes `true` or `false` | Pass `true` or `false` |
| Bodyless `HTTP 500` | A malformed request body produces `500` with a zero-length response body and no error text at all. It is a request shape problem masquerading as a server fault | Vary the body shape, not the endpoint. The [upload](#uploading-the-lifecycle-scripts) section is a worked example |
| No self-identity endpoint | The v2 surface has no call that returns the caller's own identity, and the course roster returned `total_records: 0` for student, teacher, grader, and instructor alike, leaving the instructor's own user ID undiscoverable. The cause is that Vocareum does not list the course owner under Add Teachers | [Enrol a diagnostic student](#enrolling-a-diagnostic-student) through `POST /courses/${VOCAREUM_COURSE_ID}/users` to obtain a usable user ID, and keep that account. `VOCAREUM_USER_ID` needs a real user, and that section has the body shape, which is not the one the neighbouring endpoints take |
| Course-scoped token, reported as an empty org | A token restricted to one course answers `403 Access Forbidden (Token scope is restricted to another course)` on every other course path, and `400 Invalid Request, token is restricted to a course` on `/orgs/{orgid}/…`. `GET /courses` then returns exactly the courses in scope. For a single-course token that is one course, and it is indistinguishable from an org that genuinely contains one course | Read the `/courses` listing as a report of the token's scope, never of the org's contents. Confirm the diagnosis by requesting any other course ID: a scope refusal answers `403` for every ID including ones that cannot exist, where a real lookup would tell the two apart. Then generate a token whose scope covers the course you mean |
| A course code is not a course ID | The UI addresses a course by an opaque code in its `editcourse` URL, `code=vc_2_0_…`, while the API addresses courses only by numeric ID. `GET /courses/{code}` answers `400 Invalid Request`. `GET /courses?code={code}` is worse: the parameter is **ignored**, and the call answers `200` with the in-scope course list, so it appears to have resolved the code and has not | Take the numeric ID from the info icon in the UI, or from `GET /courses` under a token that can see the course. Never infer a course's identity from a `?code=` response |
| A missing `list=true` reads as an empty directory | One path serves two operations. `…/files?dir=/voc/scripts&list=true` lists the directory; the same call without `list=true` is the download-as-zip variant, which answers `200` with `{"status": "success", "message": "Started", "transactionid": …}`, names no files, and starts a packaging transaction as a side effect. A reader parsing that for file names concludes the directory is empty, and every run leaves an unpolled transaction behind. Measured against a part carrying all six files | Always send `list=true`, and treat the transaction shape as "the listing did not happen" rather than as "nothing is deployed": the two produce opposite advice, and the second tells you to overwrite files that are already correct. The [listing](#listing-a-directory) section has both response shapes. `dbx-vocareum-diagnose` shipped with this fault and reported all four hooks absent against a part carrying all four |
| An expired course vanishes from the listing rather than reporting itself expired | Once `end_date` passes, the course stops appearing in `GET /courses` altogether, while `GET /courses/{id}` still answers `200` with the whole record. Nothing anywhere says "expired". The first symptom is a course that looks deleted, and the listing cannot explain an absence. Measured on a course whose `end_date` had passed that morning | Ask for the course by ID before concluding anything, and read the dates on the record it returns. The one endpoint that names the cause is the session resource, which answers `400 "Course expired"`. The gate is `end_date`, not `access_end_date`: the same course refused sessions with most of a day of `access_end_date` left. Extending `end_date` restores the listing and the sessions together |
| `files` holds bare strings, not objects | The listing returns `"files": ["user_setup.sh"]`, so no file name is behind a key named anything. Searching a flattened payload for a key containing `name`, which is the right way to read a session record, finds nothing at all here and again reads as an empty directory | Read `files` as the array of strings it is. Objects carrying a `name` have also come back from this endpoint, so accept both shapes |

The User-Agent ban catches only the `Python-urllib` signature, so a descriptive name is enough and there is no reason to impersonate a browser. Verified against `GET /courses/{id}` with a valid token:

| User-Agent | Response |
| --- | --- |
| `Python-urllib/3.13` | 403, blocked by Cloudflare |
| `Python-requests/2.32` | 200 |
| `curl/8.7.1` | 200 |
| `vocareum-lab-diagnostic/1.0` | 200 |
| none sent | 200 |

`requests` is unaffected only because its own default User-Agent is not the banned one.

## The workspace file tree

Every path below is under one part. The instructor workspace contains:

```
/voc/course      /voc/data       /voc/docs      /voc/private
/voc/scripts     /voc/solution   /voc/startercode
```

That list is the AWS-course reading. Walking the Databricks part returned the same set without `solution`, so treat `solution` as present on some parts and not others rather than as guaranteed. [There is no log endpoint](#there-is-no-log-endpoint-and-the-file-tree-is-not-the-container-filesystem) has the full walk, including the two roots above `/voc` that this list does not mention.

For a Databricks part, `/voc/scripts` is the directory that matters. Vocareum reads `workspace_init.sh`, `lab_setup.sh`, `user_setup.sh`, `lab_end.sh`, and `grade.sh` from it and executes nothing anywhere else. There is no `/voc/private/lab.template` equivalent, because a Databricks lab has no declarative provisioning artifact at all.

**The same endpoint uses two different path conventions, and mixing them fails.** Directory listing takes an absolute path rooted at `/voc`. File addressing takes a path relative to it, with the `/voc/` prefix removed:

| Parameter | Correct form | Wrong form and its error |
| --- | --- | --- |
| `dir=` | `/voc/scripts` | `scripts` returns `scripts doesn't exist` |
| `filename=` | `scripts/user_setup.sh` | `/voc/scripts/user_setup.sh` returns `specified source does not exist /voc/scripts/user_setup.sh` |

Both errors are `HTTP 400` and both name a path, so they read as "the file is missing" when the file is present and the convention is wrong. The error strings above are the AWS-course measurement with the folder name substituted.

### Listing a directory

```
GET /courses/{courseId}/assignments/{assignmentId}/parts/{partId}/files?dir=/voc/scripts&list=true
```

```json
{"assignmentid": "${VOCAREUM_ASSIGNMENT_ID}", "courseid": "${VOCAREUM_COURSE_ID}",
 "directory": "/voc/scripts", "files": ["user_setup.sh"], "partid": "${VOCAREUM_PART_ID}"}
```

The listing gives names only, with no size, hash, or modified time, so it cannot tell you whether the file up there is the version you meant to upload. Download it and compare hashes.

It does answer one question decisively, which is why `dbx-vocareum-diagnose` calls it in step 2: whether a hook exists at all. Vocareum skips an absent hook in silence, so a lab with no `user_setup.sh` starts successfully and does nothing, and this listing is the only thing that distinguishes that from a hook that ran and created nothing.

**`list=true` is not a refinement of this call, it is the call.** One path serves two operations, chosen by that parameter, and the other one answers `200` with no file names in it:

```json
{"status": "success", "message": "Started", "transactionid": "461890", "objid": "4168946"}
```

That is the download-the-directory-as-zip variant. It reports success, names nothing, and starts a packaging transaction as a side effect, so a reader that parses it for file names concludes the directory is empty and a caller that polls nothing leaves a transaction behind on every run. A tool built on this endpoint has to treat that shape as "the listing did not happen" and not as "nothing is deployed": the two produce opposite advice, and the second one tells you to upload scripts that are already there. `dbx-vocareum-diagnose` once shipped with exactly that fault and reported all four hooks absent against a part carrying all four.

**Names arrive as bare strings.** `files` is an array of strings rather than of objects, so nothing in the payload is keyed by a field name. Searching a flattened payload for a key containing `name` is a reasonable way to read the session record and finds nothing whatsoever here. Read `files` as the array it is. Objects carrying a `name` have also been observed from this endpoint, so accepting both shapes costs little.

### Downloading a file

```
GET /courses/{courseId}/assignments/{assignmentId}/parts/{partId}/files?filename=scripts/user_setup.sh
```

Returns a `download_url` on `labs.vocareum.com`, not the file. The URL carries an `expires` epoch and a `signature`, and it needs no `Authorization` header. Fetch it as a second request.

The `file` parameter inside that URL is `asnlib.0/<folder>/<name>`, so these folders are stored under an `asnlib` root internally. That internal name is not accepted by the `filename` parameter, which was the basis for two of the failed upload attempts below.

### Uploading the lifecycle scripts

`dbx-vocareum-upload` does everything in this section, and [`deploying.md`](deploying.md) is the runbook for it. Reach for the command first, and read the rest when it breaks or when you need to write something other than the scripts.

**There is no upload endpoint.** All three documented `files` operations are `GET`. The only write path for workspace content is the part update:

```
PUT /courses/{courseId}/assignments/{assignmentId}/parts/{partId}
```

`content` must be a **JSON array**. The object form returns a bodyless `500`. This is the working request:

```json
{"content": [{"target": "scripts", "zipcontent": "<base64 of a zip>"}]}
```

`zipcontent` is base64 of a zip archive. Archive members are written into the folder named by `target`, so a member named `user_setup.sh` with `target: "scripts"` lands at `/voc/scripts/user_setup.sh`. Existing files of the same name are replaced. A member may carry its own directory prefix; `target` may not. See [Nesting](#nesting) below, which corrects what rows 6 and 7 of the table appeared to say.

Two packaging details matter, and [`deploying.md`](deploying.md#two-things-the-packaging-does-on-purpose) has the reasoning:

- **Set the mode from the shebang.** Vocareum executes these files. A script that arrives mode `0644` is a lab that fails at its first hook.
- **Pin every member's modification time.** Zip entries carry a timestamp, so the same file zipped twice produces two different archives and two different payload hashes. The tool pins every member to 1980-01-01, the zero of the DOS timestamp the format uses. It has no effect on what Vocareum stores and it makes two uploads comparable.

Seven body shapes were tried against the same 3675-byte zip on the AWS course, with `target: "private"`. Only one was accepted, and the `target` value is the only thing that differs for a Databricks part:

| Body | Result |
| --- | --- |
| `{"content": [{"target": "<folder>", "zipcontent": ...}]}` | **202**, `{"status": "success", "message": "Started", "transactionid": "450659", "objid": "${VOCAREUM_PART_ID}"}` |
| `{"content": {"target": "<folder>", "zipcontent": ...}}` | 500, empty body |
| `{"content": {"target": "<folder>", "zipcontent": ..., "reset": 0}}` | 500, empty body |
| `{"content": {"target": "/voc/<folder>", "zipcontent": ...}}` | 500, empty body |
| `{"content": {"target": "<folder>", "zipcontent": ...}, "update": 1}` | 500, empty body |
| `{"content": {"target": "asnlib", "zipcontent": ...}}`, member `<folder>/<name>` | 500, empty body |
| `{"content": {"target": "asnlib/<folder>", "zipcontent": ...}}` | 500, empty body |
| `{"content": {"zipcontent": ...}}` | 500, empty body |

Rows 1 and 2 differ by the array wrapper and nothing else, which isolates the wrapper as the cause. The `target` was correct from the first attempt and the `500` said nothing about it, which is what made this expensive to find. That is also why `dbx-vocareum-upload` warns before the request when `--target` is not one of `docs`, `scripts`, `startercode`, or `private`: after the request there is nothing to read.

Two fields are documented and remain untested. `url` is an alternative to `zipcontent` and presumably fetches a zip Vocareum can reach. `reset` is documented as part of the same block and its scope is unknown. Assume `reset` can delete the folder until someone proves otherwise, which for `/voc/scripts` means assume it can delete the whole lab.

The write is scoped to `target`. On the AWS course, `/voc/scripts` and `/voc/startercode` were listed before and after a write to `/voc/private` and were unchanged.

#### Nesting

Measured on the Databricks course, part `${VOCAREUM_PART_ID}`, three single-member uploads to `target: "private"` with a control first. The answers differ by which half of the request the slash is in.

| Case | `target` | Member name | Result |
| --- | --- | --- | --- |
| control | `private` | `probe_flat.txt` | **202**, transaction `success`, downloads with the bytes sent |
| nested member | `private` | `probe_dir/probe_nested_member.txt` | **202**, transaction `success`, downloads with the bytes sent |
| nested target | `private/probe_target` | `probe_in_nested.txt` | **400** at the `PUT`, named |

**A nested archive member works.** It lands at `/voc/private/probe_dir/probe_nested_member.txt`, the files listing returns it under that exact path, and the download carries the bytes that went up. So courseware can travel in one archive with its directory structure intact and does not have to be flattened into one folder of 27 names.

**This corrects the earlier reading of the table above.** Rows 6 and 7 were taken as evidence that a nested member is rejected. Both of those rows also used the *object* form of `content`, which returns a bodyless `500` on its own, so the nesting was never isolated. Under the array form it is accepted.

**A nested target is refused, and the refusal is the most useful error on this API.** It names the field and enumerates the whole valid set, which no documentation does:

```json
{"error": {"message": "Invalid target (private/probe_target) for content update. Valid targets: [course, data, docs, scripts, startercode, private]"}}
```

Six targets, two of them (`course` and `data`) not previously seen by these tools. `validate_path_segment` in `upload.py` refusing a slash in `--target` was a guess when it was written and is now the measured behaviour.

**An upload merges into the target, it does not replace it.** `probe_flat.txt` was still present and still downloadable after a second upload to `private` carrying only `probe_dir/probe_nested_member.txt`. This is why [`deploying.md`](deploying.md) insists on uploading the whole course directory rather than the one file that changed: a partial write leaves the previous version's files in place beside the new ones, and nothing in the response says so.

Two probe files remain at `/voc/private/probe_flat.txt` and `/voc/private/probe_dir/probe_nested_member.txt`. `/voc/private` is not executed and nothing reads it, so they are inert. There is no documented delete on this API.

#### Verifying an upload by hand

The listing cannot confirm content, and the `202` only confirms the request was queued. Confirm by hash. `dbx-vocareum-upload` does this automatically and exits `3` when the hashes disagree. By hand it is three steps:

```bash
# 1. PUT the part, then poll the transaction to state=success
# 2. GET the download_url for scripts/user_setup.sh
# 3. curl it and compare
shasum downloaded.sh <course dir>/user_setup.sh
```

`shasum` with no arguments prints SHA-1. The upload tool prints SHA-256, so the two sets of digests are not comparable with each other.

#### `success` is not the same as addressable

The transaction reaching `state=success` does not mean every path it wrote can be read back yet. On the first nested deploy of the Databricks course, 37 members, the transaction reported `success` and 21 of the 37 answered `400 does not exist` on the very next read. All 37 were addressable and byte-correct minutes later, confirmed read-only with no second `PUT`. With the eight flat files this course carried before, the window had never been wide enough to see.

The consequence is a false `EXIT_MISMATCH`. The files endpoint answers "not there" and "not there yet" identically, so `remote_digest` returns `None` for both and the verify step read a correct deployment as a broken one. `verify` now retries only the paths that answered `None`, backing off `2, 4, 8` seconds, and only those paths rather than the whole set. A path that answers with the *wrong bytes* is reported at once and never retried: that is the real mismatch the exit code exists for, and re-reading it until it agrees is how a bad deploy would talk its way through the gate.

`show_current_files` reads the same paths before the upload and deliberately does not retry. There `None` means the file has genuinely never been written, which is what an ordinary first upload looks like.

## There is no log endpoint, and the file tree is not the container filesystem

About 90 read-only GETs against one Databricks part looked for a lifecycle script's standard output or standard error anywhere on the v2 surface. It is not reachable. This is written down as settled so that nobody spends another 90 calls on it. The shape of the negative also tells you where to look instead, which is the [Session log in the console](diagnosing.md#open-the-session-log-before-you-run-anything).

**Enumeration works through `dir=`, and only through `dir=`.** `filename=` addresses exactly one file and cannot list a directory: `voc`, `voc/`, `voc/scripts`, `voc/scripts/`, and an empty value all answer `400`. It is also rooted **at** `/voc` with the prefix stripped, as [the two path conventions](#the-workspace-file-tree) above record: `filename=scripts/user_setup.sh` answers `200` with a `download_url`, while both `voc/scripts/user_setup.sh` and `/voc/scripts/user_setup.sh` answer `400 specified source does not exist`.

The listing parameter is a different thing entirely:

```
GET /courses/{c}/assignments/{a}/parts/{p}/files?dir=<absolute path>&list=true
```

```json
{"courseid": "…", "assignmentid": "…", "partid": "…", "directory": "…", "files": ["…"]}
```

It takes an absolute path and, unlike `filename=`, it is **not confined to `/voc`**. That is how the whole exposed tree gets enumerated, and it is the call `dbx-vocareum-diagnose` already makes in `read_scripts` in `src/dbx_vocareum_tools/diagnose.py`.

Walked to depth 6, the entire tree was this. It is an older reading, from before `voclab.py` and `course.env` were part of the upload, which now adds both to `/voc/scripts`. Braces hold a directory's own children, and nothing was found below what is shown:

```
/                  resource  voc  work
/resource          asnlib{private, public{docs}, publicdata}  lib{public}  scripts  startercode
/voc               course{public}  data  docs  private  scripts  startercode
/voc/scripts       grade.sh  lab_end.sh  lab_setup.sh  user_setup.sh  voclib.sh  workspace_init.sh
/resource/scripts  grade.sh  lab_end.sh  lab_setup.sh  user_setup.sh  voclib.sh  workspace_init.sh
/work              empty
```

No log file and no log directory anywhere in it. `/work` is empty, which is the one place a hook's own working directory might plausibly have surfaced. And `dir=/tmp`, `dir=/home`, and `dir=/var/log` all answer `400`, so this tree is Vocareum's content store rather than the filesystem a script actually ran on. A file a hook wrote next to itself is not in here because this is not that machine.

**Decisive: the endpoint has no per-student dimension.** `userid`, `user_id`, `userId`, and `user` are all **silently ignored**. Each returns a byte-identical, part-scoped payload. There is no error, no warning, and no difference in the response to tell you the parameter did nothing.

That is the danger worth naming before someone writes the tool. A diagnostic built on `&userid=<student>` would look like it was reading a given student's filesystem, would in fact be reading the instructor's part content, and would keep doing that forever without a single failed call. Every student would produce the same answer and the answer would be about none of them. A hook's output written into its own working directory sits on the far side of a boundary this endpoint does not cross, and no parameter crosses it.

The remaining candidate paths were tried and are dead:

| Path | Response |
| --- | --- |
| `…/parts/{p}/logs`, `…/parts/{p}/log` | `400 Invalid Request` |
| `…/parts/{p}/resources/{userId}/logs` | `400 Invalid Request` |
| `…/parts/{p}/batch_script`, `…/parts/{p}/batch_script/0` | `400 No record found for this batch script id` |

The batch-script pair is worth separating from the other two. Its error names the record it could not find rather than rejecting the request, so that endpoint exists and presumes a batch job was submitted. It is not a session log, and per [Endpoints this repository uses](#endpoints-this-repository-uses) it presumes an environment that already came up.

## Transactions

Asynchronous writes answer `202` with a `transactionid`. Poll it:

```
GET /transaction/{transactionid}
```

```json
{"status": "success", "state": "success", "objid": "${VOCAREUM_PART_ID}", "message": "Part updated"}
```

`state` is `pending` until the work finishes. `message` is the only place some errors ever appear, so print it in full rather than truncating it.

## Enrolling a diagnostic student

Everything that reads a session needs a `userId`, and the v2 surface has no call returning the caller's own identity, so the instructor account cannot supply one. Enrol a student and keep it.

```
POST /courses/{courseId}/users
```

**The body is a flat object.** Not an array, and not wrapped in a `users` key, despite the response coming back under one:

```json
{"email": "you+student1@example.com", "role": "student"}
```

```json
{"status": "success", "users": [{"id": "5000100", "courseid": "200001",
 "role": "student", "name": "Test Student 1", "email": "you+student1@example.com",
 "client_key": "", "client_userid": "", "lti_user_id": null, "dropped": false,
 "organization_terms_agreed": "", "organization_terms_agreed_date_utc": ""}]}
```

Worth knowing before you write the call, because the shape is not guessable from the neighbouring endpoints and a wrong body here answers with a [bodyless `500`](#obstacles-each-of-which-will-recur) that names nothing. The part-update endpoint requires its `content` as an array and several endpoints take a named-collection wrapper such as `{"rubrics": [...]}`. This one takes neither.

| Field | Required | Notes |
| --- | --- | --- |
| `email` | yes, or `client_key` plus `client_userid` instead | The login, and the value to keep in `VOCAREUM_USER_EMAIL` |
| `role` | no | `student`, `teacher`, or `grader`. Omitted, it enrols a student |
| `name` | no | Ignored when the organization already holds this email. See below |
| `section_name` | no | Unmeasured |
| `external_id` | no | Unmeasured |

**`role` accepts those three values and no others, so there is no API path to an admin.** Org admin and every custom role are granted through the UI only, from Edit Org, by somebody who is already an org admin. Nothing on the v2 surface reaches them. [`permissions.md`](permissions.md#vocareums-own-roles-three-things-that-are-not-the-same-thing) has the role model and where each grant is made.

Two behaviours measured enrolling `student1@example.com` onto course 200001:

- **A user ID is per user per organization, not per course.** The call returned `5000100`, the same ID that account already held on a different course in org 999. So one diagnostic student serves every course in the org, and a user ID found on one course is worth trying on another.
- **An existing organization user wins over the request body.** The call supplied `name: "Student One"` and the response came back with `Test Student 1`, the name already on file. Enrolment onto a second course is a membership change rather than a user creation, so treat `name` as effective on first creation only.

Measured enrolling a previously unknown address onto course `${VOCAREUM_COURSE_ID}`, which is the other half of that second behaviour: **an email the organization has never seen comes back with `name` set to the email string itself.** Vocareum defaults the field rather than leaving it empty or honouring the request. That makes the response diagnostic. A `name` that differs from what you sent means the address was already an org user and you have been handed its existing ID; a `name` equal to the email means a new org user was created. Neither the response nor the roster says outright which happened, and knowing matters because a reused ID may carry sessions on other courses that a diagnostic run would then read as its own.

The call answered `200` with the record inline. **No `202` and no `transactionid`,** so enrolment is synchronous and the [transaction poll](#transactions) never fires here. Write the poll anyway if you are reusing `client.py`, since it costs nothing when the response is already terminal, but do not wait on a transaction that will not arrive.

Confirm it with the roster rather than with the response, because the response is what the write claims and the roster is what the API will answer later:

```
GET /courses/{courseId}/users?role=student
```

Drop it with `DELETE /courses/{courseId}/users/{userId}`.

## Starting a lab session

```
PUT /courses/{courseId}/assignments/{assignmentId}/parts/{partId}/resources/{userId}
```

Documented as: starts resources, extends the session if already started, launches CloudFormation templates, and issues temporary credentials. The last two clauses describe an AWS Cloud Lab. For a Databricks part, what this call sets in motion is the workspace seat, the user and group, and then `user_setup.sh`.

**Starting a Databricks session is expensive in a way starting an AWS one is not.** It runs `user_setup.sh`, which is where billable compute gets created. That is why `--start` is opt-in on `dbx-vocareum-diagnose` and default-on in its AWS counterpart.

**Do not trust the first reading after a start.** On the AWS course this call sometimes returned the full resource record with `state: "Starting"` and no `transactionid`, leaving nothing to poll, and a diagnostic that inspected immediately reported the previous run's result as the current one. The stack appeared about 90 seconds later. Whether a Databricks part returns a transaction id here is unmeasured, so the tools are written for the worse case: `dbx-vocareum-diagnose --start` polls the session record on a 10-second interval and waits for the record to carry what it is looking for, rather than reading once.

The lesson generalizes past CloudFormation. Wait on the far side, meaning the thing that was supposed to be created, rather than on the Vocareum side. For a Databricks lab the far side is a `notebook_url` in the record or a folder in the workspace.

## Reading the Databricks workspace

```
GET /orgs/{orgid}/databricks_workspaces
```

**This is the authoritative source for `DATABRICKS_HOST`, and it is better than the account console.** The console tells you which workspaces exist. This tells you which workspace Vocareum built for a given part, so a value taken from here cannot disagree with the one Vocareum uses, which is the exact mismatch step 5 of `dbx-vocareum-diagnose` exists to catch. A measured record:

```json
{"id": "10001", "orgid": "999", "courseid": "200001",
 "assignmentid": "5000001", "partid": "5000002", "num_users": "1",
 "workspace_id": "2222222222222222",
 "workspace_url": "https://dbc-22222222-2222.cloud.databricks.com",
 "workspace_name": "voc-ws-10001-1111111111111111", "status": "RUNNING",
 "error_info": "Databricks (add user to workspace) Principal not found in account. Error code: RESOURCE_DOES_NOT_EXIST",
 "region": "us-west-2", "databricks_cloud_type": "AWS",
 "init_script_id": "", "destroy_script_id": ""}
```

Five things this settles that nothing else does:

- **The workspace is keyed by part.** `courseid`, `assignmentid`, and `partid` are on the record, which confirms from Vocareum's own side that a workspace belongs to a part rather than to a student.
- **`workspace_id` arrives as a field.** It no longer has to be parsed out of an `o=` parameter, so `workspace_id_from_url` becomes a cross-check rather than the only route to the value.
- **`error_info` carries the last Databricks-side failure, and it persists while `status` reads `RUNNING`.** A workspace can be healthy and still have failed at adding a user. Read both fields: `status` alone will tell you everything is fine. The `Principal not found in account` value above is not a privilege problem, because Vocareum's own service principal holds Databricks Account admin. [`permissions.md`](permissions.md#principal-not-found-in-account-is-not-a-privilege-problem) works through what it turned out to be, which is a true statement about the lab group that stopped nothing.
- **`region` and `databricks_cloud_type`** give the cloud and region without an AWS call.
- **`init_script_id` and `destroy_script_id`** are Vocareum's own hooks on the workspace, distinct from the four lifecycle scripts in `/voc/scripts`. Both were empty here and their relationship to the lifecycle hooks is unmeasured.

The org path is the only shape that works. `/databricks_workspaces`, `/courses/{c}/databricks_workspaces`, and the part-scoped equivalent all answer `400 Invalid Request`, so this call needs `VOCAREUM_ORG_NUMBER` and there is no per-course way to ask. `GET /orgs/{orgid}/databricks_pools` is the companion and returned an empty list.

### What a second reading of the same record showed

Re-read after a session had failed on the part, and this is now step 4B of `dbx-vocareum-diagnose`, so every run reports it.

- **`error_info` was byte-identical to the value above**, which was captured before that session ran.
- **`num_users` had moved from `1` to `2`**, and `status` still read `RUNNING`.

So the record does change between readings, and `error_info` did not change with it. That is consistent with a **last-error field on the workspace** rather than a per-session one, and it is the reason the message cannot be attributed to any particular session from the API. There is no timestamp on it and no principal id in it. Settling which session it belongs to needs Vocareum support. [`permissions.md`](permissions.md#principal-not-found-in-account-is-not-a-privilege-problem) has what the message turned out to mean.

One caveat on `num_users`: it is not a count of active sessions. Both lab users from both sessions still exist in the workspace, and the failed session was still open when this was read.

### What happened when the workspace was deleted

Workspace `10001` above was deleted from the Databricks side, by hand, after its course expired. Reading this endpoint afterwards returned `total_records: "1"` holding only the replacement, `10002`, built for the new course. So **deleting the workspace in Databricks also removes Vocareum's `databricks_workspaces` record for it.** The two are not independent, and there is no orphaned Vocareum row left to clean up.

That makes this endpoint a poor place to learn a workspace's history and a good place to learn its present. Everything the record said about `10001`, including which storage configuration and network configuration it was built on, went with it. On the Databricks account side the corresponding metastore assignment and permission assignments **survive** and still name the dead workspace ID, so the residue is there rather than here. Read what you need out of this record before deleting anything.

The replacement's `error_info` was empty, against the `Principal not found in account` that `10001` carried from the start. Both workspaces were built by the same Vocareum service principal for the same repository's part, so that message is not a standing property of this setup.

**A cloned course gets a new record only once its lab is first run.** Cloning course, assignment and part carries the part settings and the whole of `/voc/scripts` across, measured: `labtype`, `databricks_maxusers` and `session_length` matched, and all eight files listed on the new part with no upload. What cloning does not carry is the roster, which is empty on the clone, and no workspace exists for the new part until a session starts. So a freshly cloned course reads as fully deployed and completely unusable at the same time, and the roster is the part to check.

## Reading the session record

```
GET /courses/{courseId}/assignments/{assignmentId}/parts/{partId}/resources/{userId}
```

For an AWS part this is where temporary credentials, a federated console sign-in URL with a live `SigninToken`, and an unencrypted SSH private key sit. **The shape for a Databricks part is undocumented by Vocareum, and no Databricks credential has been found published in one.** It has now been measured, though, which it had not been when this section was written.

### The measured shape

Read from one Databricks part after the session had ended. Seventeen key paths, containers included, and these are the whole of them:

```
status
resources[0].{courseid, assignmentid, partid, userid, sessionstart, sessionend,
              totaltime, type, state,
              databricks.{workspaceid, groupid, groupname, useremail, userid}}
```

Three things to take from that.

**`databricks` is an object, not a list.** `cloud` on an AWS record is a list and is read as `cloud[0]`, so a reader that carries the AWS habit across will index into a dict and find nothing. This is the one place the two record shapes diverge in a way that breaks code rather than returning less.

**There is no output field of any kind.** Nothing in the record matches `log`, `out`, `err`, `message`, `trace`, or `debug`. The part record is the same: 28 key paths and no log field among them. Taken with [There is no log endpoint](#there-is-no-log-endpoint-and-the-file-tree-is-not-the-container-filesystem), a script's output is not on this API at any depth.

**No `customdata` key appears anywhere in the record.** Vocareum's contract says `voccustomdata.txt` is carried in the session record. This reading was taken after the session had ended. A later reading of a **live** session on a part, whose cluster and imported notebook both existed, found six fields in `databricks` and still no such key. So, measured on one part, the record does not carry it. [`databricks-labs.md`](databricks-labs.md#voccustomdatatxt-which-the-next-hook-reads) has what that means for the hooks.

The consequence is larger than one missing field. `voc_fail`'s `error_code` and `message` ride in the same file, so they do not reach the REST API either. A lab is verified instead by the ship gate, `dbx-vocareum-diagnose --expect`, by the beacon the student's browser lands on, and by the console [Session log](diagnosing.md#open-the-session-log-before-you-run-anything), read by hand.

### Two `400`s from this endpoint that are not about the session

Both read like a missing session while being nothing of the kind. This endpoint answers `400` for reasons that live on the course, so a reader diagnosing a lab can spend a long time looking at the lab.

**`"Course expired"`** means the course's `end_date` has passed. It has nothing to do with the part, the student, or whether a session ever ran. Its companion symptom is stranger and comes first: **an expired course drops out of `GET /courses` entirely while `GET /courses/{id}` keeps answering `200` with the full record.** So the first thing anyone notices is a course that appears deleted, and the listing gives no reason because it gives no row. The gate is `end_date` and not `access_end_date`, measured: a course whose `end_date` had passed that morning refused sessions while its `access_end_date` was still most of a day away. Extend `end_date` and both symptoms clear together, the listing included.

**`"The user is not enrolled as a student in the course"`** is an enrolment gate, not a session gate. Measured both ways on the same part within one hour: before enrolment every user ID answered that `400`, including one that had genuinely run a session; after enrolling one student, that student's ID answered `200` with `sessionstart`, `sessionend` and `state` empty and `databricks` an empty array. So an enrolled student who has never started anything is readable, and the `400` says only that the roster does not hold them. Reading it as "no session yet" sends you looking for a session that was never the problem.

That second finding has a consequence worth stating on its own. **A lab launched from an instructor view runs the hooks and produces no readable session record.** Measured: a launch by a Vocareum user who was on no roster created the Databricks workspace, the lab identity, the cluster and the notebook, and yet both that user ID and every other answered the enrolment `400`. The run was real and its record was unreachable. Enrol a student and launch as that student, or the run measures nothing that has to come back through Vocareum.

There is no self-identity endpoint, so the way that user ID was recovered was from the Databricks side: the cluster's base64 `custom_tags` value decodes to `{"orgid":..., "courseid":..., "partid":..., "userid":..., "mylabid":0}`. Worth knowing, because it is the only route found so far from a running lab back to the Vocareum user that started it.

### How the tools read it

They search the record by field-name fragment instead of by path. `SESSION_RECORD_FIELDS` in `src/dbx_vocareum_tools/databricks.py` lists what to look for, currently `workspace_url`, `user_email`, `group_name`, `notebook_url`, `totalspend`, and `allowedspend`, and `find_field` reports the key path each value was found at as well as the value. The path is half the answer: a value with no path cannot be looked up again. `--raw-record` prints the whole record for mapping it.

And they redact everything before printing it. The undocumented shape is treated as though it carries what the AWS shape carries, because the cost of being wrong in the other direction is a live credential in a terminal transcript.

Two fields are worth pulling out by hand on the first live run. `workspace_url` should carry an `o=` parameter naming the workspace, and `workspace_id_from_url` reads it, which is how a student who landed in a different workspace from the one the tools point at gets caught. `notebook_url` has not been seen in the record on a Databricks part, even when `user_setup.sh` wrote it. Vocareum redirects the browser to it instead, and [`diagnosing.md`](diagnosing.md#the-beacon-reaches-the-browser-not-the-api) has the measurement.

### Spend

On the AWS course the same record carried the session's cost as strings in US dollars, cumulative for one user on one part:

```json
"totalspend": "0.000244",
"allowedspend": "200"
```

`allowedspend` matched `total_dollar` on the part, alongside `monthly_dollar`, `total_time`, `monthly_time`, and a `session_length` of 240 minutes. Whether the same fields appear on a Databricks record is unmeasured, but the budget itself demonstrably applies to Databricks labs: Vocareum's [2026-04-23 release notes](https://help.vocareum.com/en/articles/14744154-2026-04-23-release-notes) list "[Databricks] Show error message when starting lab after course end or over budget". A lab that will not start is therefore a budget question before it is a script question, and it is worth checking that before reading anything else.

The v2 endpoint reference describes this call as returning session times and cloud data and names no spend field at all, so the counters have to be found by reading a response.

## Ending a session

```
DELETE /courses/{courseId}/assignments/{assignmentId}/parts/{partId}/resources/{userId}
{"action": "end"}
```

The reference documents this in one line, "End or terminate resources", with a body of `{"action":"end|terminate"}` and no account of how the two differ. Only `end` has been exercised, on the AWS course. Treat `terminate` as unknown and destructive until someone measures it.

**This call is synchronous.** It answers `200` with `{"status": "success", "message": "Resources ended/terminated"}` and no `transactionid`, so there is nothing to poll. The part update is asynchronous and this is not, which means a client that assumes every write returns a transaction will wait forever on a field that never arrives.

On the AWS course, ending a session moved `state` to `Terminated`, `cloud[0].status` to `inactive`, and rewrote both `sessionend` and `expected_termination_time` to the moment of the call. `sessionend` is rewritten rather than left at its original value, so it records when the session actually ended and cannot be used to recover the scheduled length.

### What a Databricks end actually looks like

Two sessions were ended on one part and both behaved identically.

**`EndLab` is a transitional state and it is not in the reference.** The read-back immediately after the `200` reads `state: EndLab`. Roughly a minute later the same record reads `Terminated`. A client that treats anything other than `Terminated` as "still running" will report a successful end as a failure, which is why `dbx-vocareum-session` names `EndLab` separately instead.

**`sessionend` before the end call is a schedule, not a fact.** On a running session it held start plus four hours. The end call rewrote it to the actual moment, matching the AWS behavior. So a `sessionend` in the future is the allotted window and says nothing about whether the session is over.

**`totaltime` appears to change units on termination.** While running it is a seconds counter that climbs on a failed session as readily as on a healthy one: `4198`, then `4547`, then `4566` across successive reads of one failed session. After the end call the same field read `75`, against a wall clock of 76 minutes. The obvious reading is that a live record reports elapsed seconds and a terminated one reports billed minutes, but only two sessions have been measured and Vocareum documents neither, so do not compute cost from this field without checking the unit against the wall clock.

**The record is scrubbed.** After termination `useremail` and `workspaceid` both read empty. Anything you need from a session record has to be captured while the session is live.

**Ending a session revokes its Databricks principals.** Account-level permission assignments on the workspace went from 7 while a session was live to 5 after the end call, losing exactly that session's lab user and lab group. This matters for diagnosis more than for cleanup: the principal evidence a failed session leaves behind is destroyed by ending it, so run `dbx-vocareum-diagnose --principals` *before* the end call.

The tool for all of this is `dbx-vocareum-session`, which reads by default and requires `--end` plus a typed confirmation to end anything. It refuses `terminate` outright rather than measuring it on a live lab.

Two differences from the AWS behavior matter here, and both come from Databricks labs sharing one long-lived workspace rather than handing out disposable accounts.

**There is no account pool and no reallocation.** An AWS part holds a pool of AWS accounts and moves the allocation between sessions, so ending a session gives up the account and anything left in it becomes unreachable. A Databricks part has one workspace that persists across every session of the course. Nothing is reallocated, and nothing becomes unreachable, which also means nothing is wiped for you.

Confirmed on a real part. It resolves to a `voc-ws-<digits>-<digits>` workspace that Vocareum created itself rather than one registered by hand, and the workspace name carries what looks like a part-scoped identifier. So "one workspace per part" is the right unit: not one per course and not one per student. [`databricks-labs.md`](databricks-labs.md#vocareum-creates-the-workspace) has the evidence and what follows from it.

**Cleanup is the part's setting, not the API call's.** What ending a session actually does to the student's environment is decided by the End lab behavior configured on the part, which reaches `lab_end.sh` as `VOC_END_LAB_BEHAVIOR` with the value `stop` or `terminate`. [`databricks-labs.md`](databricks-labs.md#voc_end_lab_behavior-decides-what-cleanup-means) has the table. A `stop` preserves the student's data for their next session, so a `lab_end.sh` that deletes unconditionally destroys work the student expects to find.

## Endpoints this repository uses

| Method and path | Used for | Caller |
| --- | --- | --- |
| `GET /courses/{c}/assignments/{a}/parts/{p}` | Read part configuration: `labtype`, `session_length`, budgets | `diagnose.py`, step 1 |
| `PUT /courses/{c}/assignments/{a}/parts/{p}` | Upload workspace content through `content` | `upload.py` |
| `GET .../parts/{p}/files?dir=/voc/scripts&list=true` | List which lifecycle hooks are deployed | `diagnose.py` step 2, `upload.py` before a write |
| `GET .../parts/{p}/files?filename=scripts/user_setup.sh` | Get a signed download URL for one file | `upload.py`, to verify by hash |
| `PUT .../parts/{p}/resources/{userId}` | Start or extend a lab session | `diagnose.py --start` only |
| `GET .../parts/{p}/resources/{userId}` | Read the session record: workspace URL, lab user, group, notebook URL, spend | `diagnose.py`, step 4 |
| `DELETE .../parts/{p}/resources/{userId}` | End a session early, with `{"action": "end"}` | `session.py`, with `--end` |
| `GET /transaction/{id}` | Poll an asynchronous write or session start | `client.py`, for both tools |
| `POST /courses/{c}/users` | [Enrol the diagnostic student](#enrolling-a-diagnostic-student), which is the only way to obtain a `userId` | by hand, and the source of `VOCAREUM_USER_ID` |
| `GET /courses/{c}/users?role=student` | Confirm an enrolment from the roster rather than from the write's own response | by hand |
| `GET /orgs/{orgid}/databricks_workspaces` | The workspace URL, workspace ID, region, status, and last Databricks-side error, per part | by hand, and the source of `DATABRICKS_HOST` |
| `GET /courses` | Find a course's numeric ID, and check whether the token is global or course-scoped | by hand |

Three other paths look promising, and only one of them is worth chasing:

- **Org AWS usage**, `GET /orgs/{orgid}/aws_usage`, is documented as returning unused, used, closed, discarded, and fraud counts, a payer count, and a total cost. It is the only published route to a figure spanning more than one user. Still unmeasured, and the reason has now moved twice: under a course-scoped token it answered `400 Invalid Request, token is restricted to a course`, and under a global token with most permission boxes checked it answers `403 Access Forbidden (permission denied)`. So the remaining blocker is a permission checkbox rather than scope. For per-user spend, the session record above is the direct answer and needs neither.
- **Data Pipeline**, `GET /orgs/{orgid}/s3_labs`, is aggregate reporting: daily CSVs of sessions and cost, refreshed every 6 hours, with CloudFormation-shaped event strings whose meaning for a Databricks lab is unknown.

  **It answers `200` with live AWS STS credentials in the response body**, an `aws_access_key_id`, an `aws_secret_access_key`, and an `aws_session_token`, which are presumably how the caller is meant to fetch the CSVs from S3. Treat this endpoint the way the session record is treated: never print its body. Anything that reads it needs redaction before output, and a body already printed to a terminal should be treated as a credential disclosure rather than as log noise.
- **Batch script**, `GET .../parts/{p}/batch_script/{id}`, returns base64 execution output across student environments. It presumes an environment that already came up, which is the opposite of the case [`diagnosing.md`](diagnosing.md) exists for. Tried, and it answers `400 No record found for this batch script id`, both with an id and without one, so it presumes a batch job was submitted as well. It is the closest thing on this API to a route to script output and it is [still not one](#there-is-no-log-endpoint-and-the-file-tree-is-not-the-container-filesystem).

## References

- [Vocareum REST API](https://help.vocareum.com/en/articles/3658972-vocareum-rest-api): Personal Access Token generation, scopes, rate limit, status codes
- [Vocareum REST API v2 Endpoint Reference](https://help.vocareum.com/en/articles/14542929-vocareum-rest-api-v2-endpoint-reference): all 66 endpoints. The `PUT Part - Update` entry names `content (target, url/zipcontent, reset)` in one line and documents no example, which is the whole of the published guidance on uploading a file
- [Vocareum API v2 Postman documentation](https://documenter.getpostman.com/view/6736336/S11Exg4b): request format and pagination
- [Vocareum: Databricks scripts](https://help.vocareum.com/en/articles/7045516-databricks-scripts): what Vocareum runs from `/voc/scripts` once the files are there
- [Vocareum: Data Pipeline](https://help.vocareum.com/en/articles/6010883-data-pipeline): session log fields and the `s3_labs` export
- [Vocareum: 2026-04-23 release notes](https://help.vocareum.com/en/articles/14744154-2026-04-23-release-notes): the Databricks entries for budget errors and workspace API tokens
