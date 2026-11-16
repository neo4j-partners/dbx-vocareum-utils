# Neo4j runs on Aura, not inside the workspace

## The decision

The graph side of a Neo4j workshop is a Neo4j Aura instance. The lab connects out to it over Bolt. Nothing runs Neo4j inside the Databricks workspace: no cluster hosts a database, no init script installs a server, and no Docker image is built.

This depends on the network. Vocareum's [2026-04-23 release notes](https://help.vocareum.com/en/articles/14744154-2026-04-23-release-notes) record "[Databricks] Add port 7687 to security group". Port 7687 is the Bolt port. **Documented, not measured here.** No run recorded in this repository has opened a Bolt connection from a Vocareum cluster. [What to check on the first live run](#what-to-check-on-the-first-live-run) says how to measure it.

Aura accepts TLS only, so every connection string in workshop content has this shape:

```
neo4j+s://<instance-id>.databases.neo4j.io
```

The instance ID, the username, the generated password and the database name come from the credentials file Aura offers when the instance is created. Store all four. On AuraDB Free the database is not always `neo4j`. A course that assumes the default connects fine, and `verify_connectivity` passes, but the first real query fails.

## What Aura adds to the lab

Databricks holds the data and the compute. The Delta tables, the Unity Catalog governance, and the Spark cluster all stay in Databricks. Aura adds a store where relationships are first-class. A `SIMILAR_TO` edge written from a Spark job becomes something a Cypher query can traverse. A notebook reads a Delta table, writes the entities and relationships it derives into Aura, then asks the graph a question that would be a multi-way self-join over the table.

The Neo4j Connector for Apache Spark carries the data across. It reaches a cluster as a Maven library. A course attaches it to the per-student cluster through a cluster policy, together with its Python libraries. [`permissions.md`](permissions.md) records that attaching libraries this way is allowed for the service principal, and [`pre-warm.md`](pre-warm.md) records how long the install takes.

## What follows from running Neo4j outside

- **No workspace resource hosts a database.** Nothing has to be created for it in `workspace_init.sh`, and nothing has to be torn down in `lab_end.sh`.
- **Aura sits outside `VOC_END_LAB_BEHAVIOR`.** A `terminate` reverts the Databricks workspace and leaves the graph as it was. A lab that needs a clean graph per session has to reset it with a Cypher statement the notebook runs, because Vocareum's cleanup cannot reach Aura.
- **Region is a separate choice.** Aura picks its own cloud and region at sign-up, independent of the Databricks workspace's region. The two should be close enough that Bolt latency does not dominate a notebook cell.
- **The credentials are the one thing that has to reach the workspace.** The URI, username and password have to be readable from a notebook. That is a Databricks secrets question, not a Neo4j one.

## Where the credentials belong

In a secret scope, read by the notebooks through `dbutils.secrets.get`. Not in notebook text, and not in a cluster environment variable. Both are visible to anyone who can read the object, and both end up committed when the notebook is exported. The databricks-neo4j-workshop course makes one exception, described below.

Three arrangements are possible. They differ in who creates the scope and who can read the password.

| | One shared scope | One scope per student, from a hook | One scope per student, from a notebook |
| --- | --- | --- | --- |
| Created by | `workspace_init.sh`, once | `user_setup.sh`, per session | the student, in the first lab notebook |
| Aura instances | one, shared by the class | one per student | one per student, which the student creates |
| Who else can read it | everyone with `READ` on the scope | only with an ACL the hook writes for the student | the student who made it |
| Cleanup | none | `lab_end.sh`, on `terminate` | none from a hook |

**Not measured.** The "Who else can read it" row ignores workspace admins, who can read every scope, and probably the hook's service principal too. No hook in this repository writes the ACL the second column needs. Whether a `terminate` takes a student-made scope with it is not measured either.

The databricks-neo4j-workshop course takes the third column. **From the notebook code, not measured.** Each student creates an AuraDB Free instance on the course site's Setup page, before Lab 1. The first Lab 1 notebook then stores the credentials. The student types the URI, username, password and database into a notebook cell. A later cell writes them as four keys into the student's own scope through `WorkspaceClient().secrets`, and later notebooks read them back. So the password does pass through notebook text once, which is the exception to the rule above.

**Open.** Whether a hook may create a secret scope. A lifecycle script does hold a Databricks credential, `VOC_DB_API_TOKEN`, which [`permissions.md`](permissions.md#the-credential-a-hook-runs-under) measures. `secrets:ListScopes` is measured there as allowed, but from a laptop as the service principal, not with the hook's token. Creating a scope is not measured, because creating one is a write and the probe catalog holds no writes. [`permissions.md`](permissions.md) is where that answer goes when someone measures it.

`dbx-vocareum-diagnose` lists the secret scopes in step 5. That is how to check that the scope a notebook expects is there.

## What to check on the first live run

Confirm from a notebook cell in a real lab, before a class depends on it, that Bolt reaches Aura:

```python
from neo4j import GraphDatabase

with GraphDatabase.driver(uri, auth=(user, password)) as driver:
    driver.verify_connectivity()
```

Three failures are worth telling apart in front of a class, because each has a different fix: a blocked port, a wrong password, and a `bolt://` or `neo4j://` scheme without `+s`. **Open.** How each one surfaces from a Vocareum cluster is not measured. Record the exception each one raises on the first live run, so a class can be told which is which.

## References

- [Vocareum: Databricks scripts](https://help.vocareum.com/en/articles/7045516-databricks-scripts): the hooks a shared setup would live in.
- [Vocareum: 2026-04-23 release notes](https://help.vocareum.com/en/articles/14744154-2026-04-23-release-notes): "[Databricks] Add port 7687 to security group".
- [Neo4j Connector for Apache Spark](https://neo4j.com/docs/spark/current/): the library that moves data between Delta tables and the graph.
