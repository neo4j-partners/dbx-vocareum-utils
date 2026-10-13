# Pre-warming a lab

## The cost being attacked

A student waits about 16 minutes for a usable cluster. Measured on one per-student cluster:

| Phase | Time | What is happening |
| --- | --- | --- |
| Create to `RUNNING` | 10 min | Databricks asks AWS for an EC2 box, boots it, joins it to Spark |
| `RUNNING` to all `INSTALLED` | 6 min | Maven resolves a Spark connector, pip installs ten packages |

The cluster reports `RUNNING` for the last 6 of those minutes, so a student who trusts the state indicator gets import errors.

## What does not work: warming a cluster in `lab_setup.sh`

`lab_setup.sh` used to call `cluster-ensure` whenever `VOC_DB_USER_EMAIL` arrived. That path is deleted. Two measurements killed it.

**The name changes between sessions.** Vocareum mints a Databricks principal per session and deletes it afterward, and the local part of the address carries the session start epoch. The same student was `labuser10000002_1785948805` on one session and `labuser10000002_1785972938` on the next. `cluster_name_for` in `voclab.py` keys idempotency on that local part, so a pre-warmed cluster and the cluster `user_setup.sh` guarantees carry different names. Warming produced a second cluster, not a head start.

**The second cluster is invisible to cleanup.** `cluster_release` keys on the same name. `lab_end.sh` runs against the student's own session and names the student's own cluster, so a cluster warmed under any other name is never reclaimed. Only `DEFAULT_AUTOTERMINATION_MINUTES` ends it, which is 90 minutes of billing for nothing.

Fixing the permission grant that failed during pre-warm buys none of this back. The blocker is the name, not the grant.

## What can work: an instance pool

An instance pool is a Databricks object, not an AWS object. It keeps N booted EC2 boxes on standby. A cluster that names the pool grabs a ready box instead of waiting for AWS. That attacks the 10 minutes. It does nothing to the 6.

The pool needs no student identity, which is why it dodges the bug above.

## Measurements

Run against a Vocareum-created workspace as the service principal. Every pool was created with `min_idle_instances` 0, so none launched EC2, and every cluster accepted was permanent-deleted in the same run. The workspace was re-listed afterward and held zero pools.

What the service principal may do here is a permissions result and belongs in [`permissions.md`](permissions.md), not on this page.

**A pool accepts the ML runtime.** `preloaded_spark_versions: ["17.3.x-cpu-ml-scala2.13"]` was accepted and read back unchanged. The pool reached `ACTIVE`.

**A single-node `SINGLE_USER` cluster composes with a pool.** `clusters/create` accepted `instance_pool_id` together with `num_workers` 0, the singleNode `spark_conf` trio, `ResourceClass=SingleNode`, and a dedicated principal. The cluster reached `PENDING` and reported `node_type_id` `i3.xlarge` inherited from the pool.

**Three keys have to move to the pool, not one.** The first version of this measurement sent a hand-written subset of the create body and concluded that `instance_pool_id` simply replaces `node_type_id`. That was wrong, because the subset omitted keys the real `cluster_settings` sends. Re-run with the exact body:

| Sent alongside `instance_pool_id` | Result |
| --- | --- |
| the full `cluster_settings` body | `400 INVALID_PARAMETER_VALUE`, `The field 'enable_elastic_disk' cannot be supplied when an instance pool ID is provided.` |
| `aws_attributes` only | `400 INVALID_PARAMETER_VALUE`, `The field 'aws_attributes.zone_id' cannot be supplied when an instance pool ID is provided.` |
| `enable_elastic_disk` only | `400 INVALID_PARAMETER_VALUE`, same message |
| `node_type_id` only | `400 INVALID_PARAMETER_VALUE`, `The field 'node_type_id' cannot be supplied when an instance pool ID is provided.` |
| none of the four | accepted |

So `node_type_id`, `aws_attributes` and `enable_elastic_disk` are all pool-owned. A pooled create body is a different body, not the same body with one key swapped.

**A pool created with defaults puts the whole cluster on spot.** This is the finding that matters most and it is a correctness result rather than a shape one. A pool created without `aws_attributes` reports `availability: SPOT` with `spot_bid_price_percent: 100`, and the cluster that came off it reported `first_on_demand: 0`. `cluster_settings` sets `first_on_demand: 1` for a stated reason: it keeps the driver off the spot market, and on a single-node cluster the driver is the entire cluster. A default pool therefore hands every student a cluster a spot reclaim can kill mid-lab.

The intent moves to the pool rather than disappearing. `aws_attributes: {"availability": "ON_DEMAND"}` on the pool was accepted and read back.

**`zone_id: auto` does not survive the move.** A pool accepts `auto` and resolves it once, at creation, to a concrete zone. Both test pools came back pinned to `us-west-2c`. On a cluster `auto` means pick at boot; on a pool it means pick now and never again. The comment in `cluster_settings` wanted `auto` for the case of thirty clusters requested on a workshop morning, and a pool gives that up. It is a cost of pooling, not a defect.

**A runtime the pool did not preload is accepted silently.** A pool preloading only `17.3.x-cpu-ml-scala2.13` accepted a cluster asking for `17.3.x-scala2.13`. The cluster reached `PENDING` and reported the requested version. Nothing warned. This is the failure that costs money and reports success: boxes sit idle, students still wait for a runtime to download, and no error names it.

**Pool defaults when the create call omits them.**

| Field | Default |
| --- | --- |
| `max_capacity` | absent, meaning no ceiling |
| `idle_instance_autotermination_minutes` | 60 |
| `enable_elastic_disk` | `False` |
| `aws_attributes` | `{"availability": "SPOT", "zone_id": "us-west-2c", "spot_bid_price_percent": 100}` |

An absent `max_capacity` is what turns one mistake into an open-ended bill, so the CLI sets it. `max_capacity` 25 and `idle_instance_autotermination_minutes` 30 were both accepted and read back.

## Where the pool connects to a workspace

A pool lives in exactly one workspace and its `instance_pool_id` means nothing anywhere else. Vocareum creates a workspace per part, so one pool serves one part. A second part needs a second pool.

`DATABRICKS_HOST` decides which workspace gets it. Pointed at any workspace other than the Vocareum-created one, the pool bills in a place no student enters and reports success while doing it. [`databricks-labs.md`](databricks-labs.md#vocareum-creates-the-workspace) records why the Vocareum-created workspace is the one that counts.

Clusters therefore find the pool by **name**, not by ID. Name is the join key, the same way it already is for clusters. The name comes from `VOC_POOL_NAME` with `DEFAULT_POOL_NAME`, `workshop-pool`, as its default, so one workspace can host two courses without them fighting over the same pool.

## Running the pool

```bash
uv run dbx-vocareum-pool                          # read only: is there one, and in which workspace
uv run dbx-vocareum-pool --create --min-idle 5    # before the class
uv run dbx-vocareum-pool --delete --yes           # after the class
```

With neither `--create` nor `--delete` the tool only reads, so the bare command is safe to run at any time. `--delete` asks for confirmation, and `--yes` skips it for scripted runs. `--create` and `--delete` together are refused, because they name two different intentions.

What the pool is made of, when you want something other than the default:

- **`--pool-name`** picks the pool to act on. It falls back to `VOC_POOL_NAME`, then to `workshop-pool`.
- **`--min-idle`** is how many boxes stay booted and waiting. Default `0`, which launches nothing and costs nothing.
- **`--max-capacity`** is the ceiling on boxes, idle and in use. Default `25`. The platform default is no ceiling at all, which is why this is always sent.
- **`--idle-minutes`** is how long a released box waits before going back to AWS. Default `30`. The platform default is `60`.
- **`--node-type`** is the node type the pool boots. Default `i3.xlarge`.
- **`--spark-version`** is the runtime to preload. It must match the course's `VOC_COURSE_SPARK_VERSION`, or the pool saves nothing. With no value, a create falls back to `17.3.x-scala2.13`, and a read lists the preloads and passes no verdict.
- **`--env-file`** points at a different `.env`.

Every pool is created `ON_DEMAND`, for the reason in [Measurements](#measurements).

Three things to know:

- **Idle boxes cost real AWS money, from create to delete.** Nothing deletes the pool automatically. That is the admin's job, and it is why `--min-idle` defaults to `0`.
- **No pool at all is not an error.** The lab scripts fall back to naming a node type, and the class runs as it would without one.
- **The tool runs three guards first,** on the credential, the host, and the identity. A pool created against the wrong host bills where no student goes.

## Still open

- **How much time a pool really saves.** Pools preload runtimes, not libraries, so the 6 minutes of installs stay. The drop from 16 minutes to about 7 is arithmetic, not an observation.
- **Whether `min_idle_instances` holds when a whole class starts at once.** The pool sits in one availability zone, so this is a question about capacity in that zone rather than in the region.
- **Whether Vocareum reuses a part's workspace between classes.** A pool dies with its workspace, so the pool is per class rather than per course until this is measured.
