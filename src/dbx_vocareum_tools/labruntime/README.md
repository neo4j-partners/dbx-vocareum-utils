# labruntime/

The canonical account of what runs inside Vocareum, and why each part of it is shaped the way it is. Read this before touching a hook.

Nothing here is a course. A course's own directory is scaffolded from `template/` by `dbx-vocareum-init` and holds four hooks, a `course.env`, and its notebooks. That directory uploads as a set to `/voc/scripts`:

```bash
uv run dbx-vocareum-upload <course dir>
```

and `dbx-vocareum-upload` adds `voclib.sh` and `voclab.py` to the archive from this package on the way out.

This file is not uploaded, and neither is `template/README.md`. `dbx-vocareum-upload` skips `README.md`, `course.env.example` and `.DS_Store`, so notes for the reader live beside the scripts without reaching the workspace.

## Two owners, one directory

| File | Owner | Upgraded by |
| --- | --- | --- |
| `voclib.sh` | this package, forever | bumping the dependency |
| `voclab.py` | this package, forever | bumping the dependency |
| `template/*.sh` | the course, from the first `dbx-vocareum-init` | editing the course's copy |
| `template/course.env.example` | this package | seeded once into the course's `course.env` |

The split is not filing. It is what makes "one owner per file" true at deployment rather than only in a design document. A course holding its own `voclab.py` uploads perfectly, verifies by hash, and provisions with whatever runtime it was scaffolded with, and nothing in the transcript says which copy went up. So `upload.py` refuses a local file of either runtime name outright rather than preferring one.

The hooks go the other way because a hook is where a course states what it wants: which subcommands run, in what order, and what reaches `voccustomdata.txt`. An uneditable hook would push every course-specific decision into an environment variable, which is how a course-agnostic tool acquires one course's vocabulary.

## What runs, and when

| File | Runs | Required |
| --- | --- | --- |
| `workspace_init.sh` | once, after Vocareum creates a workspace | no |
| `lab_setup.sh` | when a lab is pre-warmed from the fleet dashboard | no |
| `user_setup.sh` | when a student starts the lab | yes |
| `lab_end.sh` | when the lab ends, stopped or terminated | no |
| `voclib.sh` | never. Sourced by the four above | no |
| `voclab.py` | never. Invoked by the four above through `voc_voclab` | no |
| `course.env` | never. Sourced by `voclib.sh` at source time | no |
| the course's notebooks | never. Read by `voclab.py notebook-import` | no |

`dbx-vocareum-upload` warns that nothing executes `voclib.sh`, `voclab.py`, `course.env` or the notebooks. The warning is correct every time. Vocareum runs only the four named hooks plus `grade.sh`, and a file in `/voc/scripts` that is not one of those names is inert unless another script reaches for it. All of these are reached for, and a misspelled hook name looks exactly the same in the listing, which is why the warning is printed rather than suppressed.

## How a course states its values

`course.env` is the seam. `voclib.sh` finds it with the same three-candidate search it uses for itself, sources it inside `set -a` so every value is exported, and does so at source time rather than on a call, because all four hooks need it and a hook that forgot the call would come up with defaults and report a perfectly healthy cluster belonging to no course. A file that will not source is `BAD_COURSE_ENV` and stops the hook: carrying on would provision something the course did not ask for, under a record that reports success.

Exported and not merely set, because `voclab.py` is a separate process and reads `VOC_COURSE_*` out of its own environment. A plain shell variable would be visible to the hook and invisible to the program the hook exists to run.

On the Python side those values are read when the subcommand runs, never as an argparse default. An argparse default is evaluated while the parser is built, which is before the environment matters, so every course option defaults to `None` and `resolve_course_value` consults the variable afterwards. A flag still wins over the environment, which is what makes these commands runnable from a laptop against a workspace whose `course.env` is not on this machine.

`VOC_COURSE_*` is excluded from `voc_report_env` by prefix. That measurement exists to record what *Vocareum* hands a hook, and a value the course put there itself would make it answer a different question than the one it is asked.

A course that sets nothing is a real course. It gets a working single-node cluster with no libraries and no notebooks, and `notebook-import` records `notebook_action=none` rather than failing, because content that reaches a student some other way is not a broken lab.

Sourcing it is not one line. Each hook searches three candidates in order, beside `$0`, then `/voc/scripts/voclib.sh`, then the working directory, because Vocareum guarantees neither the working directory a hook runs in nor the form of `$0`. When none of the three holds the file, the hook writes `error_code` `MISSING_VOCLIB` with plain `printf` and exits 1, spelled out inline rather than delegated, because `voc_fail` is defined in the file that went missing. [`diagnosing.md`](../../../docs/diagnosing.md#open-the-session-log-before-you-run-anything) quotes the Session log line that forced this.

An absent hook is skipped in silence rather than reported, so a lab missing `lab_end.sh` starts normally and simply never cleans up. `dbx-vocareum-diagnose` lists what is actually deployed for exactly this reason.

## What this version does

It measures its own environment, provisions one compute cluster per student, imports whatever notebooks the course named into that student's workspace folder, and lands them on the first one. A course that also runs SQL gets a serverless warehouse from `workspace_init.sh`.

`workspace_init.sh` can also resize the one warehouse no course asked for. Databricks creates a starter warehouse with every new workspace, and Vocareum creates a workspace per part, so a size corrected by hand in the console lasts exactly one class. Measured in one workspace, it arrived as `Small`, serverless, auto-stop 10, created by the service principal, and carrying a `users` `CAN_USE` grant, which is every student in the workspace. `Small` is four times the compute of `2X-Small`, so a student who opens the SQL editor and takes the default gets the expensive one. `voc_voclab starter-warehouse` puts it on `VOC_COURSE_STARTER_WAREHOUSE_SIZE` through `POST /api/2.0/sql/warehouses/{id}/edit`, which is documented as an incremental edit, so only `cluster_size` travels and this step never becomes the owner of fields it did not set. It resizes rather than deletes, because a deleted starter warehouse is a student staring at a SQL editor with nothing to run on and the size is the whole cost story. A course that names no size gets no edit, which is the right default for a course that never looked. The name is matched against `Serverless Starter Warehouse` and then `Starter Warehouse`, the workspace's spelling before the documentation's, and finding neither is recorded as `starter_warehouse_action=absent` rather than failed: Databricks documents that it creates one and does not promise to keep doing so, and taking a class down over an object no course asked for would save nothing. A size that is not one of the ten the API accepts fails before any call, as `INVALID_WAREHOUSE_SIZE` naming the course variable, because a `400` from Databricks names the field and not the `course.env` line holding the typo, and the spellings are case sensitive.

The cluster is single node and `SINGLE_USER`, named after the student's lab identity with the domain stripped, so `student1_200000@vocareum.com` gets a cluster called `student1_200000`. Single node because a lab that reads from and writes to an external system rather than shuffling across workers would leave a worker idling while being billed, and because a room of thirty students is thirty clusters. `SINGLE_USER` because that access mode is what allows a Maven jar to be installed straight from coordinates; on a `USER_ISOLATION` cluster the same jar first has to be added to the metastore artifact allowlist, which is a prerequisite outside these scripts and one a course cannot satisfy from a hook.

The name is the idempotency key, and that is what makes pre-warming an optimization rather than a second code path. `lab_setup.sh` runs `cluster-ensure` when Vocareum has already associated a student, and `user_setup.sh` runs the same call when the student arrives. Whichever runs first creates the cluster; the other finds it by name and starts it if it has since terminated. A lab nobody pre-warmed still works, just more slowly, and the two hooks cannot duplicate or undo each other's work. That property is bounded by one session, for the reason set out under "Student identities are per session" below.

The node type is `i3.xlarge`. Measured, `GET /api/2.0/clusters/list-node-types` returns 629 node types on a lab workspace, `m5.large` among them. The five node types the UI offers, `i3.xlarge`, `i3.2xlarge`, `i3.4xlarge`, `i3en.xlarge` and `g4dn.xlarge`, are the `node_type_id` allowlist of the built-in Personal Compute cluster policy, with `i3.xlarge` as its `defaultValue`. `GET /api/2.0/policies/clusters/list` confirms it. Nothing here sets `policy_id`, so the allowlist never applies to these clusters. `i3.xlarge` is kept deliberately rather than by constraint: it is what Personal Compute would hand a student who built their own cluster, so a provisioned cluster and a hand-made one behave alike. The `DEFAULT_NODE_TYPE_ID` comment in `voclab.py` is the fuller account.

The runtime is pinned rather than resolved, and the default is `17.3.x-scala2.13`. Pinned because a course tested on one runtime must not quietly run on another, and a plain runtime rather than an ML one because a course states what it needs in `VOC_COURSE_LIBRARIES` and gets it installed, so paying ML's boot time for a library set the course did not choose buys nothing. Scala 2.13 because a Maven coordinate is built against a Scala version and 2.13 is the current one. A course that does want the ML runtime sets `VOC_COURSE_SPARK_VERSION`.

Measured on one workspace, `GET /api/2.0/clusters/spark-versions` offers 56 versions and `17.3.x-cpu-ml-scala2.13` is an exact key among them, named "17.3 LTS ML (includes Apache Spark 4.0.0, Scala 2.13)". No policy on that workspace constrains `spark_version` at all. The four built-in policies other than Personal Compute leave `node_type_id` unlimited as well. The first live session ran on the ML key and the default moved afterwards, so the plain key is pinned but not yet observed in a session.

Both pinned values are checked against the workspace immediately before `clusters/create`, and only there. `preflight_cluster_settings` reads `clusters/spark-versions` and `clusters/list-node-types` through `available_keys`, so a runtime or node type the workspace does not offer fails as `UNAVAILABLE_SPARK_VERSION` or `UNAVAILABLE_NODE_TYPE` naming the options that are on offer, rather than surfacing a raw API error. `describe_options` bounds that sample to ten and says how many were left out, and for runtimes `spark_version_family` narrows it to the keys sharing the ML, GPU and Scala traits a substitute would have to share. A mismatch fails rather than substituting the nearest thing, because a course tested on one runtime must not quietly run on another. A student arriving at a pre-warmed cluster pays nothing for any of this, since the existing-cluster branch never reaches it. If the preflight read itself fails, `available_keys` returns `None`, which logs and proceeds, because `clusters/create` is the authority on what it will accept and a workspace that will not list its node types must not become an unprovisionable lab.

A cluster takes its box from an instance pool when the workspace holds one, and neither the node type nor the preflight above applies in that case. `cluster-ensure` lists `/api/2.0/instance-pools/list` and matches on name, `DEFAULT_POOL_NAME` unless `VOC_POOL_NAME` or `--pool-name` names another, because an `instance_pool_id` means nothing outside the workspace that issued it and Vocareum makes a workspace per part. Name is the join key here for the same reason it already is for clusters. Measured, a pooled create body is a different body rather than the same body with one key swapped: `node_type_id`, `aws_attributes` and `enable_elastic_disk` are all pool-owned, and sending any of them beside `instance_pool_id` is a `400` naming the field. The `first_on_demand: 1` that gets dropped with `aws_attributes` is not a setting to lose, so `dbx-vocareum-pool --create` puts the pool itself on `ON_DEMAND`; a pool made by hand with defaults is `SPOT`, and on a single node cluster the driver is the whole cluster. The node-type preflight is replaced rather than kept, because the pool owns the node type, and what takes its place is a comparison of the pool's `preloaded_spark_versions` against the pinned runtime, recorded as `pool_preload=hit` or `pool_preload=miss`. It records rather than fails because measured, a cluster asking for a runtime the pool did not preload is accepted silently, boots, and saves nothing, so writing it down is the only way anyone learns of it. No pool is not an error either: absent, not `ACTIVE`, or a listing that would not read at all each fall back to `node_type_id` and today's body, recording `pool=absent`, `pool=inactive` or `pool=unreadable`. A student's session is the wrong place to enforce instructor setup. All of this is on the create branch only, so a student who found their cluster already there pays for none of it and their record carries no `pool` field at all. [`../../../docs/pre-warm.md`](../../../docs/pre-warm.md) is the account of what a pool buys, which is the 10 minutes spent waiting on AWS and not the 6 the libraries take.

The credential a hook runs under is settled. The service principal `.env` names through `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET` authenticates to the workspace over OAuth machine-to-machine and holds `allow-cluster-create`. A running Vocareum hook is also handed `VOC_DB_API_TOKEN`. [`../../../docs/permissions.md`](../../../docs/permissions.md#the-credential-a-hook-runs-under) holds what that token authenticates as, and this file states no verdicts. What settled it was the `Init Exit Status` transcript on the admin Workspaces page, which carries a hook's stderr and needs no student, no roster and no session record.

`voc_detect_credential` still measures and records what it can reach on every run. A variable observed once on one part is not a guarantee about the next, and the record is what makes an absence diagnosable rather than mysterious. When it is absent, `voclab.py` says so as `MISSING_API_TOKEN` instead of failing obscurely.

`lab_end.sh` reclaims the cluster at the depth `$VOC_END_LAB_BEHAVIOR` calls for: terminated on a `stop`, keeping the configuration, and deleted outright on a `terminate`. The reasoning written into the `stop` branch is that the next session starts the same cluster by name without resolving the Maven connector a second time, and the identity finding below undermines that reasoning. Failing to reclaim is recorded in `voccustomdata.txt` rather than raised as a failed hook, because a lab end that reports failure is not something a student can act on. They have already left, and the thing a later reader needs is a leftover cluster traceable to the run that failed to remove it.

Every other value takes the `stop` depth, and that default is a measurement rather than a preference. A per-student cluster was measured still `RUNNING` after its session record read `state: Terminated`. The part record carries no End lab behavior field under any spelling, so a value arriving unset or empty is the case to design for. Reclaiming compute in error costs a restart and keeps the cluster's configuration and libraries; deleting in error destroys what the session was keeping; doing nothing bills until somebody notices. So unset, empty, and an unrecognized word all terminate the cluster and delete nothing, and the match on the two words ignores case and surrounding whitespace so that `Terminate` is not treated as unknown. What arrived goes into `voccustomdata.txt` verbatim as `end_lab_behavior`, beside `end_behavior_status` and `cleanup_command`, because a record that names only the branch taken cannot diagnose a run like that one, and this hook gets no `VOC_IPC_DATA_FILE` so it cannot write a beacon either.

## How a course's notebooks reach a student

Nothing in Vocareum puts them there, and `/voc/startercode` does not work. A notebook was uploaded there, verified byte-identical, and the student's Databricks folder `/Users/<lab identity>` was still empty in Student View. So `startercode` reaches the Vocareum environment beside Databricks and not the Databricks workspace.

The notebooks therefore ship in the course's own directory, beside the hooks, and `user_setup.sh` runs `voclab.py notebook-import` after `cluster-ensure` and before it writes the landing page. The course names them in `VOC_COURSE_NOTEBOOKS`, comma separated, and a name may be a directory, in which case every importable file under it goes in. Each name is found beside `$0`, then in `/voc/scripts`, then in the working directory, which is the same three candidates and the same reason as `voclib.sh`. A course that names none imports nothing and records `notebook_action=none` rather than failing, because content that reaches a student some other way is not a broken lab.

Each file is `POST`ed to `/api/2.0/workspace/import`. A `.ipynb` goes as `format=JUPYTER`; a `.py` goes as `format=AUTO` with no `language`; `.sql`, `.scala` and `.r` go as `format=SOURCE` with the `language` the extension implies, because the source format carries no language of its own and Databricks will not guess. The landing URL is the first notebook the course named, so the order in `VOC_COURSE_NOTEBOOKS` is the order a student meets them. The extension comes off every target path except `.py`, because a Databricks notebook object carries none and importing to `...ipynb` names the student's notebook after the file rather than after the lab.

**`.py` is the exception, and it is one change in two places.** Measured against a live workspace, after a course shipped a `data_utils.py` its notebooks do `from data_utils import ...` on and every student got `NotebookImportException: Unable to import module data_utils. The following file appears to be a notebook`. `SOURCE` with a `language` always produces a notebook, and Python's import machinery refuses to import one. `AUTO` decides from the path first and the content second: a `.py` with no `# Databricks notebook source` first line arrives as a workspace file, a `.py` carrying that line arrives as a notebook, and **anything at a path with no extension arrives as a workspace file whatever it holds**. That last rule is why `AUTO` alone is not the fix and is not safe alone either. Sending `AUTO` while still stripping the extension would turn every notebook in every course into a file, silently.

Stripping stays for everything else for the cosmetic reason above, and keeping `.py` is safe because of where the extension survives. Measured in the same run: `JUPYTER` stores at the path as written whatever the path ends in, `AUTO` deciding file stores as written, and `AUTO` deciding notebook is the one case where Databricks strips the extension itself. A headerless module takes the middle case, so the object lands at exactly the path `voclab.py` built and the `get-status` check below asks about something that exists. That matters more than it looks: were the write and the check ever to name different paths, the check would 404 on every run, `user_setup.sh` would re-import on every start including the one after a `stop`, and the student's work would be overwritten. A course that wants a `.py` delivered as a notebook says so in the file, with the first line Databricks' own export already writes, and it should not also be in `NOTEBOOK_KEEP_EXTENSIONS`.

A named directory that holds nothing importable is a failure rather than a silent zero, because the course asked for something it did not get.

**It skips rather than overwrites.** `GET /api/2.0/workspace/get-status` runs first, and an object already at the path ends the command as `kept`. `user_setup.sh` runs again on every start, including the one after a `stop`, and a `stop` is the ending that preserves the student's work for the session they are coming back to. Overwriting would hand them a blank copy of the notebook they spent the first session filling in, which is the same mistake the `$VOC_END_LAB_BEHAVIOR` branch in `lab_end.sh` exists to avoid. A `RESOURCE_ALREADY_EXISTS` from the import itself is read the same way, which covers both a hook that got there first and this request's own retry landing after a 5xx that had in fact succeeded. `overwrite` is `false` in the body, so a retry cannot produce a second copy, and that is why this is the one write here that is safe to retry.

`lab_end.sh` removes nothing for it. The notebook costs nothing to leave, a `stop` must leave it, and a `terminate` reverts the workspace anyway.

## Student identities are per session

Vocareum mints a fresh student principal for each session and removes it when the session ends. SCIM showed `labuser10000002_1785971423@vocareum.com` at 23:10 UTC. Minutes later, after that session had ended, `GET /api/2.0/preview/scim/v2/Users` returned `totalResults` 1, holding only `adminuser1000000@vocareum.com`. A cluster left `RUNNING` in the same workspace belongs to `labuser10000002_1785948805@vocareum.com`, an identity that no longer exists. The numeric suffixes decode as epoch seconds, 1785948805 being 16:53 UTC and 1785971423 being 23:10 UTC, so those two names are two sessions of what a roster would call one student.

Three things follow, and none of them is repaired here.

The cluster name changes between sessions, because it is the local part of an identity that is itself per session. Idempotency on the name therefore holds within a session, which is the `lab_setup.sh` to `user_setup.sh` case it was designed for and where it still works exactly as described above. Across sessions it does not hold, because the next session looks for a name the previous one never used.

`lab_end.sh`'s `stop` branch terminates the cluster on the reasoning that the next session will restart it by name. That reasoning does not survive this finding. What a `stop` should do instead is an open design question. Settling it means deciding whether a per-session cluster name is the right key at all.

The `CAN_RESTART` grant target is transient. It has to come from the live session's `VOC_DB_USER_EMAIL` and must never be cached or carried forward, because a grant naming an earlier session's principal names a principal the account has already removed.

The cost hazard this creates is measured rather than hypothetical. A cluster created through the UI under Personal Compute was found `RUNNING` with `autotermination_minutes` 0, owned by a deleted identity, after its Vocareum session had ended. Clusters these scripts create carry `DEFAULT_AUTOTERMINATION_MINUTES` 90, so that backstop covers those and nothing else. This is the concrete form of the rule that Vocareum will not remove what it did not create.

## What a hook hands back

Vocareum does not expose a lifecycle script's standard output through its REST API. A line written with `voc_log` reaches whoever is watching the Vocareum console and nobody else.

`voccustomdata.txt` is the hook's own record of its outcome. Vocareum forwards it to the next script in the sequence as `VOC_CUSTOM_DATA`, and reads `error_code` and `message` out of it when a script exits non-zero. Its contract also says the session record carries it. Measured on one Databricks part, the record does not, so the ship gate, `dbx-vocareum-diagnose --expect`, and the `voc-diag` beacon are how a lab is verified. [`../../../docs/databricks-labs.md`](../../../docs/databricks-labs.md#voccustomdatatxt-which-the-next-hook-reads) has the measurement. Anything a later hook needs still goes through `voc_custom_write`, and a script that fails does so through `voc_fail`, which writes four fields, `ran_at`, `script`, `error_code`, and `message`, in that order, before exiting 1. A script that exits 1 without the last two produces a lab that failed for no stated reason. The first two are there because `voc_custom_write` writes with `>` and so truncates: a hook that failed early used to leave a record naming no script at all, indistinguishable from a hook Vocareum never ran. With `script` present, a record proves the hook ran and reached that point.

The other outbound channel is `$VOC_IPC_DATA_FILE`. Writing `{"notebook_url": "..."}` there redirects the student to that URL on entry. Only `lab_setup.sh` and `user_setup.sh` are given that variable, and only `user_setup.sh` writes it here: two scripts writing the same file is a race whose winner is not documented.

## The shell and Python boundary

The Databricks calls live in `voclab.py` because they are not calls worth writing in shell, and the hooks reach them through `voc_voclab`. The contract between the two halves is narrow on purpose.

`voclab.py` narrates to stderr, so its log interleaves with `voc_log` in whatever console is watching, and returns `key=value` lines on stdout, one per line, keys drawn from `[a-z0-9_]`. On failure it emits `error_code` and `message` and exits non-zero. `voc_voclab` turns each line into a `voclab_<key>` variable, so a hook reads `$voclab_cluster_name` on success and hands `$voclab_error_code` with `$voclab_message` to `voc_fail` on failure. Stale variables from an earlier call are cleared first, so a caller cannot read this run's result through the last one's values. `key=value` rather than JSON because the shell side then needs no JSON parser, and there is no `jq` to assume.

Both output files stay single-writer. `voclab.py` writes neither `voccustomdata.txt` nor `$VOC_IPC_DATA_FILE`; the calling hook does, from those variables.

**A course runs a program of its own the same way.** `voc_voclab` is a one-line wrapper over `voc_python <program.py> [args...]`, which is the general form. `voclab.py` is the runtime's and a course does not edit it, so a course that provisions something the runtime does not know about, a catalog of its own shape, a volume, a pipeline, uploads its own program beside the hooks and calls it. Everything above holds unchanged: the program is found with the same three-candidate search, `python3` is guarded the same way, and it answers on the same `key=value` contract.

What differs is the variable prefix, and it differs on purpose. The prefix is the program's name without `.py`, so `workshop.py` answers as `$workshop_catalog` and a missing one fails as `MISSING_WORKSHOP_PY`. Sharing `voclab_` across two programs would let a hook read a field the other program set, and it would read as a success rather than as a failure. The clearing of stale variables stops at the program's own prefix for the same reason, so one call cannot erase another's results.

A course's program is standard library only for exactly the reasons `voclab.py` is, and it may `import voclab` to reuse the HTTP layer: both files land in `/voc/scripts`, so that is a lookup in the directory it is already running from.

This is the part worth not simplifying. A Python program that exits non-zero having written nothing to `voccustomdata.txt` produces a lab that failed for no stated reason, which is the exact thing `voc_fail` exists to prevent, so `voclab.py` catches even the exceptions it did not anticipate and reports them as `UNEXPECTED_ERROR` with the traceback on stderr. It also does not `pip install` anything. It is standard library only, against Python 3.9, because a `pip install` on a hook's critical path is a PyPI round trip a student waits through and fails outright on a host with a PEP 668 managed Python. `voc_require_python3` guards the interpreter itself and fails as `MISSING_PYTHON3` when there is none.

## Where a course's own provisioning goes

A course owns its four hooks, so anything the runtime does not do goes into them. The question is which hook, and the answer is scope rather than convenience.

Shared work belongs in `workspace_init.sh`: a secret scope, a cluster policy, a catalog, the SQL warehouse. It runs once per workspace, so 30 students do not produce 30 copies.

Slow work belongs in `lab_setup.sh`, which runs before any student arrives. The cluster is already started here, and library installation is several minutes of cluster time that a pre-warm is the right place to spend.

Per-student work belongs in `user_setup.sh`. The cluster, the libraries and the notebooks are done by the runtime; a course adds whatever else is one student's, and the landing page still points at whichever notebook a student should open first.

Anything billable that `user_setup.sh` creates has to be removed in `lab_end.sh`, and that script has to read `$VOC_END_LAB_BEHAVIOR` first. On `stop` the student's data is preserved for their next session, so deleting their notebooks destroys work they expect to find. On `terminate` the environment reverts and cleanup is free.

Grant permissions to the group, not the user. `$VOC_DB_GROUP_NAME` is what Vocareum manages and what it tears down, and a grant made directly to a student identity outlives the session that needed it.

The per-student cluster is the deliberate exception, and it is worth being explicit about why. `CAN_RESTART` on it is granted to the student's own `user_name`, because the object is one student's cluster and granting it to the group would let every student in the workshop restart every other student's compute. The rule above is about grants that outlive their session; this grant cannot, because `lab_end.sh` removes the object it is attached to and the principal it names ceases to exist when the session does. The `user_name` has to be read from the live session for that reason, never from anything written down earlier. Note that this is also the call most likely to fail on a pre-warm, with the `Principal not found in account` error `docs/permissions.md` describes, since a Vocareum identity may not have propagated to the account that early. That is an ordering problem rather than a privilege one, and it recovers by itself: `user_setup.sh` runs the same `cluster-ensure` again with the identity in place.
