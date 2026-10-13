# What each principal in a Vocareum Databricks lab will and will not allow

The permission ceiling inside the Databricks workspace Vocareum drives, what is verified about it, and how it is measured.

**This file is the only place in the repository that states a permission result.** Everything else links here. If you measure a call, add a row to the table for the principal you measured it as, and change nothing else.

**The service principal has been measured and the lab user has not.** Every row of the lab-user table is still `Untested`, because filling it needs a personal access token minted inside an enrolled student's live session. A session launched from an instructor view mints a Databricks lab identity but publishes no student session record and no token, and from the Databricks side the two look identical. The gap between the two tables is the whole point of the document.

Identifiers appear as `${PLACEHOLDER}`, resolved from `.env`. [`.env.example`](../.env.example) says where each value comes from.

## The two principals are not close to equivalent

| Principal | How it authenticates | What it is |
| --- | --- | --- |
| Service principal `vocareum-sp` | OAuth M2M, client ID `${DATABRICKS_CLIENT_ID}` and a secret | Holds the Databricks `Account admin` role, which is what Vocareum requires of it. It creates the workspaces, assigns the users, and drives the account console |
| Lab user | a personal access token generated inside a live session | A workspace identity in a Vocareum-managed group, holding whatever that group was granted and nothing else |

The service principal is an account administrator. Anything measured with it describes what an administrator may do in this workspace. It says nothing whatsoever about what a student may do, and the gap between the two columns below is the only interesting number in this document.

**Vocareum's session record for a Databricks part publishes no Databricks credential.** An AWS Cloud Lab session hands back a temporary access key, secret, and session token, and the student role can be measured with them. There is no equivalent here. The only way to reach the lab user is a personal access token minted from inside a live student session and passed as `DATABRICKS_LAB_USER_TOKEN`, which `dbx-vocareum-probe --source token` reads and nothing else does. It expires with the session that made it, so it is a measurement credential rather than a stored one.

Every probe report prints a banner naming the principal it describes. That is not decoration. A permissions table that measured an administrator and reads as though it measured a student is the specific mistake this repository exists to avoid making twice, and on Databricks the two credentials differ by one flag rather than by a whole code path.

## Databricks hides rather than refuses

This is the single behavior that makes a Databricks permission measurement harder to read than an AWS one.

A principal with no permission on an object is told the object does not exist. A list call returns only the objects the caller can see, so it succeeds and comes back short rather than failing. Both of these look like success:

| Observation | On AWS | Here |
| --- | --- | --- |
| `ResourceNotFoundException` on a resource that does not exist | authorization passed, so the action is Allowed | proves nothing. A resource the caller cannot see answers identically |
| An empty list from a list call | the collection is empty | the collection is empty, or the caller can see none of it |

So `RESOURCE_DOES_NOT_EXIST` is recorded as `NOT_FOUND_OR_HIDDEN`, whose tracker status is `Inconclusive`, and an empty list is recorded as `ALLOWED_EMPTY`, whose status is `Allowed` with the qualification stated in the next-step text. `tests/test_selftest_cases.py` asserts that not-found never maps to `Allowed`, because that mapping is exactly the one the AWS classifier is right to make and this one is not.

The catalog exploits the behavior where it can. `clusters:Get` deliberately names a cluster ID that does not exist, so the answer separates a principal that may read clusters from one that may not, without creating anything.

## Three admins grant three different things under one error

`PERMISSION_DENIED` covers grants made by three different people in three different consoles. Only the message body says which:

| Verdict | Status | Who fixes it | Where |
| --- | --- | --- | --- |
| `PERMISSION_DENIED` | Denied (ACL) | workspace admin | the object's Permissions dialog in the workspace |
| `UNITY_CATALOG_DENIED` | Denied (UC) | metastore admin | a `GRANT` on the securable in Unity Catalog |
| `ENTITLEMENT_MISSING` | Denied (entitlement) | account admin | the group's entitlements in the account console |
| `METASTORE_NOT_ASSIGNED` | Blocked | account admin | the workspace has no metastore attached at all, so no `GRANT` can help |

Collapsing these into one Denied would turn an actionable result into a shrug, which is why `classify()` in `src/dbx_vocareum_tools/probe.py` reads the message rather than only the code. `DENIAL_CASES` in `selftest.py` holds the wording each verdict is recognized by, and the pytest suite drives the same table, so a change to the classifier that breaks one of them fails both runners.

The metastore predicate is the one that has already been wrong once. It tested for the literal `"not assigned"`, and the actual wording is "has no Unity Catalog metastore assigned", where the negation sits several words away from the participle it governs. The self-test caught it before any live run did.

## Vocareum's own roles: three things that are not the same thing

Everything above this heading is Databricks-side. This section is Vocareum-side, read out of `help.vocareum.com`. None of it is measured against the live org: it is what Vocareum publishes, plus two places where Vocareum publishes nothing and that absence is itself the finding.

Three models get run together in conversation and running them together misleads, so they are kept apart. Only the first has a published permission model. The second is undocumented entirely. The third is measured and lives in another file.

### Platform roles, which are held by humans

Course-level roles are **Student**, **Teacher**, and **Grader**, granted on the course Enrollment page or through LTI. Org-level roles are granted only by somebody who is already an org admin, from Edit Org.

**There is no built-in teaching assistant role.** A TA is either a Grader or a custom role, and which one you choose is a real decision rather than a naming preference, because Grader is a fixed bundle and a custom role is not.

Custom roles are the only part of this with a published permission model, and only an org admin can create one:

| Role type | The permissions it can grant |
| --- | --- |
| course | `submissions: view/grade`, `workarea: view/submit` |
| org | `course: clone`, `course: teacher access`, `resource dashboard: view`, `tickets: create`, `lab budget: edit time / edit cost` |

`lab budget: edit time / edit cost` is the one to notice for a Databricks course, because a Databricks lab that will not start is [a budget question before it is a script question](vocareum-api.md#spend), and this is the permission that lets somebody other than an org admin fix it.

**There is no article on adding an org admin.** The mechanism is one sentence buried inside [Org-level settings](https://help.vocareum.com/en/articles/6399583-org-level-settings): "Admin Roles: View and add admins to your organization. Admins have privileges to edit courses beyond Instructors." That article carries `noindex` and is absent from the sitemap, so it will not surface in a search of the help site and it did not surface in one. No privilege matrix is published for the built-in org admin role either, so what an org admin may actually do is knowable only by holding the role and trying, which is the same epistemic position as the Databricks tables above and without the instrument.

### The `adminuser<digits>@vocareum.com` lab identity, which is undocumented

Searching the help site for `adminuser` or `labuser` returns **zero hits** across roughly 170 articles. The nearest published thing is an "Admin Access to Databricks" section in [Creating a Databricks assignment](https://help.vocareum.com/en/articles/7045503-creating-a-databricks-assignment), which says org admins and instructors "can enter the Databricks workspace as an admin by clicking on the workspace URL in the workspaces table on the Databricks Workspaces page".

That sentence establishes only that privileged UI access exists. It names no identity. The docs never give the naming pattern, never say whether one such identity exists per workspace or per org, and never state its Databricks privileges.

**Inference, and it is labelled as one because nothing published supports it.** The most likely reading is that `adminuser<digits>@vocareum.com` is the identity Vocareum's SSO mints when a privileged user clicks that workspace URL, which would explain it appearing as the creator of a cluster nobody remembers creating by hand. Nothing contradicts that reading and nothing confirms it. Do not build anything on it, and in particular do not assume its privileges: an identity that enters "as an admin" through a UI path is not necessarily a Databricks workspace admin, and the two are being conflated by that sentence rather than distinguished.

### API token permissions, which are the third axis

Covered in [`vocareum-api.md`](vocareum-api.md#basics) and not re-derived here. One consequence belongs on this page: the v2 `role` field accepts `student`, `teacher`, or `grader` and nothing else, so **there is no API path to grant org admin or a custom role**. Admin granting is UI only. It cannot be scripted, it cannot be audited from the API, and it has to be performed by a human who already holds the role.

### `Principal not found in account` is not a privilege problem

A Vocareum workspace record can carry `error_info: "Databricks (add user to workspace) Principal not found in account. Error code: RESOURCE_DOES_NOT_EXIST"` while its `status` reads `RUNNING`. Vocareum performs every identity operation as a Databricks Account admin, and an account admin can add any principal to any workspace in the account. So the error is about ordering: a Vocareum identity may not have propagated to the account yet when it is used. The lab group was measured to be workspace-local, with no account-level counterpart, on healthy sessions and failed ones alike. The message was also measured to persist unchanged across a healthy session, so it is workspace state rather than a session outcome. Do not read it as a cause.

**Ending a session revokes its principals.** The workspace's assignment list loses exactly that session's user and group at the end call. Capture `dbx-vocareum-diagnose --principals` before ending anything, or the evidence goes with the session.

## The ceiling, as measured

Two tables, one per principal, deliberately not merged. Merging them would destroy the distinction the tables exist to preserve.

Run `uv run dbx-vocareum-probe --all` to fill either one. Each result carries a verdict and a tracker status, and `TRACKER_STATUS` in `probe.py` is the mapping between them, so the Status column here is generated rather than judged.

### Service principal, holding Account admin

Measured as `${DATABRICKS_CLIENT_ID}` against workspace `${DATABRICKS_WORKSPACE_ID}`, with the workspace guard confirming the host before any result is recorded.

Measured by `dbx-vocareum-probe --all`, 24 operations, all three guards passing first. The same 24 operations returned the same verdicts against a replacement workspace after the first was deleted: 20 `OK`, 3 `ALLOWED_EMPTY` and 1 `INCONCLUSIVE_BAD_INPUT`. The service principal's ceiling is a property of the account role it holds rather than of any one workspace, so a workspace being replaced does not invalidate what was measured in its predecessor.

| Operation | Status | Measured by |
| --- | --- | --- |
| `account:ListGroups` | Allowed | `OK` |
| `account:ListMetastores` | Allowed | `OK` |
| `account:ListUsers` | Allowed | `OK` |
| `account:ListWorkspaces` | Allowed | `OK` |
| `clusters:Get` | Inconclusive | `INCONCLUSIVE_BAD_INPUT`. See below |
| `clusters:List` | Allowed | `OK` |
| `clusters:ListNodeTypes` | Allowed | `OK` |
| `clusters:SparkVersions` | Allowed | `OK` |
| `instance-pools:List` | Allowed | `ALLOWED_EMPTY` |
| `jobs:List` | Allowed | `ALLOWED_EMPTY` |
| `libraries:AllClusterStatuses` | Allowed | `OK` |
| `policies:List` | Allowed | `OK` |
| `scim:ListGroups` | Allowed | `OK` |
| `scim:ListUsers` | Allowed | `OK` |
| `scim:Me` | Allowed | `OK` |
| `secrets:ListScopes` | Allowed | `ALLOWED_EMPTY` |
| `token:List` | Allowed | `OK` |
| `unity-catalog:CurrentMetastore` | Allowed | `OK` |
| `unity-catalog:ListCatalogs` | Allowed | `OK` |
| `unity-catalog:ListExternalLocations` | Allowed | `OK` |
| `warehouses:List` | Allowed | `OK` |
| `workspace-conf:Get` | Allowed | `OK` |
| `workspace:GetStatus` | Allowed | `OK` |
| `workspace:List` | Allowed | `OK` |

Nothing was denied, which is the expected result for an account administrator and is the reason this table cannot stand in for the one below it. Three readings need care:

- **The three `ALLOWED_EMPTY` results are not measurements of an empty workspace.** `instance-pools:List`, `jobs:List`, and `secrets:ListScopes` authorized and returned nothing. Per [Databricks hides rather than refuses](#databricks-hides-rather-than-refuses), an empty list is not evidence the collection is empty. What it does establish is that the call is permitted, which is the question this table asks.
- **`clusters:Get` measured nothing at all.** It answered `INVALID_PARAMETER_VALUE`, `Cluster 0000-000000-doesnotexist does not exist`, because the catalog probes it with a deliberately absent cluster ID. Databricks validates the request shape before it authorizes, so a rejected parameter says nothing either way about permission. It is recorded as Inconclusive rather than Allowed for that reason, and it is the one row in this table that a real cluster ID would change.
- **`token:List` succeeded, so personal access tokens are enabled on this workspace.** That is the prerequisite for the lab-user table below ever being filled, and it is a property of the workspace rather than of this principal.

#### Instance pools, measured by hand rather than by the probe

Creating and deleting a pool are writes, so they are not in the probe catalog and never will be: `--all` walks the whole catalog, and a write in there would spend money on someone's behalf. They are recorded here because [`pre-warm.md`](pre-warm.md) depends on them and a permission result belongs on this page and nowhere else.

Measured as the service principal against workspace `${DATABRICKS_WORKSPACE_ID}`. Every pool was created with `min_idle_instances` 0, so no EC2 was launched, and every object created was deleted in the same run. The workspace was listed afterward and held zero pools.

| Operation | Status | Measured by |
| --- | --- | --- |
| `instance-pools:Create` | Allowed | Pool created and read back `ACTIVE` |
| `instance-pools:Get` | Allowed | Read back the fields it was created with |
| `instance-pools:Delete` | Allowed | Pool deleted, listing then empty |
| `clusters:Create` with `instance_pool_id` | Allowed | Cluster reached `PENDING` and inherited the pool's `node_type_id` |

So an instructor holding these credentials can run the `dbx-vocareum-pool` runbook without asking anyone for anything. Nothing here says whether the token a hook runs under may do the same, and nothing needs it to: `cluster-ensure` only lists pools and names one it found, which is the `instance-pools:List` row above.

The workspace guard was tested on purpose, with `--probe scim:Me --expect-workspace-id 0`. It refused, exited 2, and reported no probe results: "Host guard refused the run. Expected workspace 0, reached ${DATABRICKS_WORKSPACE_ID}." A tool that measured the wrong workspace and printed results anyway would be worse than one that failed, so this refusal matters as much as the table above it.

#### The credential a hook runs under

Vocareum hands each hook `VOC_DB_API_TOKEN`. Measured from inside a running `workspace_init.sh`, it authenticates, and `scim/v2/Me` returns the service principal's identity, `${DATABRICKS_CLIENT_ID}`. Eight read-only calls were allowed: SCIM `Me` and groups, clusters, instance pools, SQL warehouses, Unity Catalog catalogs, the workspace tree, and the token list. `DATABRICKS_TOKEN` was not set, so these verdicts belong to that token and not to a fallback credential.

**Same identity implies the same ceiling, and that step is an inference.** Databricks authorizes by principal, so the 24-operation table above should describe this credential too. Eight calls were measured and twenty-four were not. Measure any specific operation a hook needs rather than reading it off that table. The beacon's `vars` list shows `user_setup.sh` is handed a variable of the same name. Whether its value carries the same identity there was not measured, which is why `voc_detect_credential` runs in each hook.

### Lab user, inside a live session

Measured as `${DATABRICKS_LAB_USER_EMAIL}` with a personal access token generated inside that student's session, through `dbx-vocareum-probe --source token`.

| Operation | Status | Measured by |
| --- | --- | --- |
| Every operation in the catalog | Untested | No enrolled student's session has yet minted a token |

### What the catalog asks, and why each entry is there

`uv run dbx-vocareum-probe --catalog` prints this. Every entry is a read, and `tests/test_selftest_cases.py` asserts the catalog holds no writes, because `--all` walks the whole of it and a write in there would start billable compute on someone's behalf.

| Group | Operations | Why a lab needs it |
| --- | --- | --- |
| Identity | `scim:Me`, `scim:ListUsers`, `scim:ListGroups` | `user_setup.sh` has to resolve `VOC_DB_USER_EMAIL` to a workspace user and `VOC_DB_GROUP_NAME` to a group to grant against |
| Workspace tree | `workspace:List`, `workspace:GetStatus` | where the student's notebooks go |
| Compute | `clusters:List`, `clusters:Get`, `clusters:SparkVersions`, `clusters:ListNodeTypes`, `policies:List`, `instance-pools:List` | a lab that cannot start a cluster cannot run. `policies:List` matters separately: a cluster policy is how a lab caps what a student can spend |
| Libraries | `libraries:AllClusterStatuses` | a course's connectors and packages reach a cluster as libraries |
| Secrets | `secrets:ListScopes` | where external service credentials belong, rather than in notebook text |
| Jobs and SQL | `jobs:List`, `warehouses:List` | for labs that run on a schedule or through a warehouse rather than interactively |
| Unity Catalog | `unity-catalog:CurrentMetastore`, `unity-catalog:ListCatalogs`, `unity-catalog:ListExternalLocations` | granted in a different place by a different admin, which is why its denials get their own verdict |
| Tokens | `token:List`, `workspace-conf:Get` | whether a student can mint the credential this tool needs to measure a student at all. If personal access tokens are disabled for the workspace, the lab-user column above can never be filled |
| Account scope | `account:ListWorkspaces`, `account:ListMetastores`, `account:ListUsers`, `account:ListGroups` | a different host, and the plane Vocareum itself drives when it creates workspaces and assigns users |

`clusters:SparkVersions` is worth its own note. The runtime versions a workspace offers bound which build of a Spark connector can be attached, so it is a compatibility question that happens to be answerable by a permission probe.

## The guards, and why the probe refuses rather than reports

The AWS equivalent learned this the expensive way: a probe whose credentials had quietly expired fell through boto3's provider chain, picked up the operator's own ambient credentials, and reported confidently on an entirely different AWS account. The Databricks SDK has the same provider chain and the same failure mode.

So `guard_credentials` and the workspace guard both refuse to let a run start rather than letting it report:

- **A blank credential is refused, not treated as absent.** A credential present in name and empty in fact is the dangerous case, because the SDK reads a blank as "not supplied" and falls through to whatever is ambient. `SP_GUARD_CASES` covers the blank client ID, the blank secret, and the blank lab-user token.
- **The host must be the expected workspace.** `normalize_host` reduces a URL copied out of a browser to its origin so that two hosts can be compared for equality and have that mean something, and the guard compares the result against `DATABRICKS_WORKSPACE_ID`. A diagnostic that reads the wrong workspace is exactly as wrong as a probe that measures the wrong principal.
- **Plain http is refused outright.** A Databricks workspace is https, and a host that arrives as http is a configuration error rather than a downgrade to accept.
- **A value that is not a hostname is refused where it is read.** `urlsplit` treats everything before the first slash as the netloc, spaces included, so `not a url` would normalize into `https://not a url`, satisfy the host guard by comparing equal to itself, and surface several calls later as an SDK connection error naming no cause. `normalize_host` checks the netloc shape and both entry points turn the refusal into a configuration error, so a malformed `DATABRICKS_HOST` fails before the first call rather than during one.
- **A mutating method needs `--allow-mutating`.** `is_read_only` treats everything other than `GET` and `HEAD` as changing something. Databricks has read-only `POST` endpoints, so this is deliberately conservative: treating a read-only `POST` as mutating costs one flag, and treating a real write as read-only costs a cluster nobody meant to start.

`uv run dbx-vocareum-probe --self-test` exercises all of this offline and makes no Databricks call. Run it before trusting a result from a live run.

## Open questions for Vocareum support

- **What does `enable_tokens` in the part configuration do?** `VOC_DB_API_TOKEN` is known to arrive, to authenticate, and to carry the service principal, but not whether that setting is what puts it there, nor what turning it off would remove.
- **Is `VOC_DB_USER_EMAIL` populated in `lab_setup.sh`?** Vocareum's table lists it for a hook documented as running when a lab is pre-warmed, which is before a student is known. The answer decides whether expensive setup can live there.
- **Is the group Vocareum creates per student or per lab?** `VOC_DB_GROUP_NAME` is what a lifecycle script grants against, and whether it holds one student or thirty changes what granting against it means.
- **How is the `adminuser…@vocareum.com` principal created at the Databricks account level, and in what order relative to workspace assignment?** Vocareum publishes nothing about the sequence.

## References

- [Databricks: Authentication for Databricks automation](https://docs.databricks.com/aws/en/dev-tools/auth/): the OAuth M2M flow the service principal uses and the SDK's credential provider chain
- [Databricks: Unity Catalog privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges): the `GRANT` model behind the `UNITY_CATALOG_DENIED` verdict
- [Databricks: Manage users, service principals, and groups](https://docs.databricks.com/aws/en/admin/users-groups/): entitlements, which are the third of the three grants
- [Vocareum: Setting Up Databricks Resources](https://help.vocareum.com/en/articles/12030031-setting-up-databricks-resources): that Vocareum is the SAML IdP for the whole Databricks account, that its service principal holds Account admin, and the active-workspace caps by tier. The source for [the `Principal not found` reading](#principal-not-found-in-account-is-not-a-privilege-problem)
- [Vocareum: Custom roles](https://help.vocareum.com/en/articles/5858581-custom-roles): the only published Vocareum permission model, and the two role types with what each can grant
- [Vocareum: Org-level settings](https://help.vocareum.com/en/articles/6399583-org-level-settings): one sentence on Admin Roles is the whole of the published mechanism for adding an org admin. The article is `noindex` and absent from the sitemap
- [Vocareum: Getting started, administrators](https://help.vocareum.com/en/articles/8923577-getting-started-administrators) and [For administrators](https://help.vocareum.com/en/collections/3374725-for-administrators): the org admin's own documentation, which describes what to do and not what the role permits
- [Vocareum: Creating a Databricks assignment](https://help.vocareum.com/en/articles/7045503-creating-a-databricks-assignment): the "Admin Access to Databricks" section, which is the closest the docs come to the `adminuser…@vocareum.com` identity without naming it
- [Vocareum: Access codes and signup links](https://help.vocareum.com/en/articles/15367675-access-codes-signup-links): how a human arrives with a course-level role in the first place
