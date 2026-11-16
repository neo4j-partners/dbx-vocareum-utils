# Next steps: Vocareum Databricks workspace

Updated 2026-11-01. This is a working tracker, not a committed doc. It names real Vocareum course IDs, so keep it out of git. Account and workspace IDs are written as placeholders and live in `.env`.

## Goal

Get a Vocareum session to start on a Databricks part and build a workspace.

## Where things stand

- **Root cause found:** Vocareum held a bad service principal secret. Its workspace record said `no databricks service principal token`. Saving the current secret in the Vocareum Settings page fixed it.
- **Course in use:** 223679 "voc databricks test". Assignment 5648602, part 5648603, student 5263762. `.env` points here.
- **Why not 224346:** That course is a "Clone by Copy" of 223679. Session calls on it failed with `HTTP 400: The user is not enrolled as a student in the course`.
- **Course end date:** Moved to Nov-21-2026 by hand. Confirmed through the API.
- **Workspace:** Vocareum built `voc-ws-27654-...` and it is `RUNNING`. `workspace_init.sh` ran, since the warehouse and catalog exist.
- **Session record:** It went `Failed` → `WorkspaceSetup` → `Failed`. It failed after the workspace came up, with no lab user and no cluster. The cause is not in the REST record.
- **Start call:** It answered `HTTP 400 Workspace is scheduled to be created` while the build ran.
- **Stale host in `.env`:** `DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID` still name a workspace that is gone. They need the new values (see N2).
- **Test workspace:** `agent2-test-workspace` is `RUNNING` in the account. Nothing links it to the part. Keep or delete is undecided.
- **Old failed records:** Two `VOC_FAILED` workspace records remain, `voc-ws-27491-...` and `voc-ws-27492-...`. They look harmless.

## What was run, and what came back

| # | Command or action | Result |
| --- | --- | --- |
| 1 | `uv run pytest` | Pass. Offline. |
| 2 | `uv run dbx-vocareum-probe --self-test` | Pass. |
| 3 | `uv run ruff check .` and `uv run ruff format --check .` | Pass. |
| 4 | `uv run python debug/switch_course.py --dry-run`, then without the flag (user ran it) | `.env` rewritten to 223679 / 5648602 / 5648603 / 5263762. Old values are in `.env.bak`. |
| 5 | `uv run dbx-vocareum-upload` of the workshop `lab/` directory (59 files) to part 5648603 | Uploaded. Hash verified. Used temporary env vars, not `.env`. |
| 6 | `uv run dbx-vocareum-diagnose --start` (billable, first run) | Steps 1 and 2 passed. Step 3 failed with `Workspace is scheduled to be created`. The record read `Failed`. |
| 7 | `uv run dbx-vocareum-session --end --yes`, twice | Both returned success. The record still read `Failed`. |
| 8 | DNS check on the `.env` host | Does not resolve. This caused the earlier 5 minute timeout in diagnose. |
| 9 | Account API, list workspaces (service principal, new OAuth secret) | `management-workspace` is `FAILED` ("temporary access to your AWS account has expired"). `agent2-test-workspace` is `RUNNING`. |
| 10 | Agent created `agent2-test-workspace` with the `vocareum-db` credential config and the us-west-2 storage config | Reached `RUNNING` in about 90 seconds. No errors. No compute created. Nothing deleted. |
| 11 | Account API, list the service principal's OAuth secrets (dates only, no values) | Two secrets, both `ACTIVE`. One created 2026-10-30, expires 2027-04-28. One created 2026-10-01, expires 2028-09-30. Neither is expired. |
| 12 | Vocareum docs: Setting Up Databricks Resources | Vocareum stores its own copy of the service principal secret, expiry date, ID and application ID. |
| 13 | `uv run dbx-vocareum-diagnose` (read only), step 4B | Two `VOC_FAILED` workspace records with `Databricks (create workspace us-west-2): no databricks service principal token`. |
| 14 | Vocareum console, Settings, Databricks Account Configuration: pasted the current secret, **Save Account Configuration** (user did this) | Banner: "Databricks account configuration saved." Application ID matches `DATABRICKS_CLIENT_ID`. Principal ID matches `vocareum-sp`. |
| 15 | `uv run dbx-vocareum-diagnose --start` (billable, second run) | Same `HTTP 400 Workspace is scheduled to be created`. |
| 16 | `uv run dbx-vocareum-diagnose` (read only), two reads a minute apart | New workspace record `voc-ws-27654-...` with a workspace ID and URL, `WORKSPACE_INIT`, no error. Session state `WorkspaceSetup`. |
| 17 | Background poll of the session and workspace records, every 30 seconds | Its workspace parse was wrong, and it stopped after one read. Replaced by row 18. |
| 18 | `uv run dbx-vocareum-diagnose --raw-record` with `DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID` set in the shell for this run only (`.env` untouched) | Workspace `voc-ws-27654-...` is `RUNNING`. The host and identity guards passed as `vocareum-sp`. The session record reads `Failed`, `workspaceid` is `null`, and `totaltime` keeps rising (81, 336, 624, 679). No lab user email, no group. Clusters: 0. Secret scopes: none. |
| 19 | Read only look in the new workspace as the service principal | `shared_warehouse` is `RUNNING`. Catalog `databricks-neo4j-workshop` exists. Groups include `ws-27654-users`. Users: only the two Vocareum admins. No lab user, no cluster, no instance pool. |

## What the results mean

- **The fix was the stored secret.** Before the save, Vocareum made no workspace. After the save, it made one. Both are measured.
- **AWS link works.** Vocareum's own build got past the credential and storage configs.
- **The old test failures were not about AWS.** `agent2-test-workspace` proved the AWS side. The real gap was Vocareum's token.
- **Not proven:** That nothing was billing from the first `Failed` session. The record never reached `Terminated`.
- **Open:** Whether a token bad on 2026-10-29 was an expired secret or a replaced one. Vocareum's record has no timestamps.

## Poll result

- **Workspace:** The build finished. `voc-ws-27654-...` is `RUNNING`.
- **`workspace_init.sh` ran.** The workspace holds the `shared_warehouse` warehouse and the `databricks-neo4j-workshop` catalog, which that hook creates.
- **The session still failed.** It failed after the workspace came up. No lab user and no cluster exist, so it stopped before or inside `user_setup.sh`.
- **Why is unknown.** The REST record carries no error and no log. Vocareum's console may show the hook output.
- **Billing:** `shared_warehouse` is running and stops itself after 10 minutes idle. The `Failed` session's `totaltime` is still rising, so end it.

## Tracking list

### Done

- [x] **A1. Re-read the session record.** It moved from `Failed` to `WorkspaceSetup`.
- [x] **A2. Read the Vocareum workspace record.** It named the cause: `no databricks service principal token`.
- [x] **B1. Secret Vocareum holds.** Replaced on the Settings page. The page cannot show which secret is stored.
- [x] **C1. Retry `--start`.** Answered `scheduled to be created`, then the workspace build began.

### Next

- [x] **N1. Wait for the build.** Done. The workspace is `RUNNING`.
- [ ] **N1b. End the `Failed` session.** `uv run dbx-vocareum-session --end --yes`. Its `totaltime` is still rising. Needs your yes.
- [ ] **N2. Copy `.env`, then update the host.** `cp .env .env.bak2`. Change only `DATABRICKS_HOST` and `DATABRICKS_WORKSPACE_ID`.
- [ ] **N3. Read the record with `uv run dbx-vocareum-diagnose`.** Step 5 should now match `workspace_url`.
- [ ] **N4. Launch the lab from the Vocareum console** and test it. Read the hook output there if the session fails again, because the REST record shows none.
- [ ] **N5. Run `uv run dbx-vocareum-session --end --yes`** when the test is done. An open session bills.
- [ ] **N6. Read `/Users` for orphans.** `uv run dbx-vocareum-orphans` (read only).

### Still open

- [ ] **A3. Check the account SSO setup.** Vocareum must be the SAML identity provider on the Databricks account. The certificate printed in Vocareum's setup article expired in January 2025, so confirm the account holds a current one. This needs the account console.
- [ ] **A4. Check the `vocareumvm` role.** Vocareum builds the VPC, subnets and NAT gateway through it. Use `rtk proxy aws iam get-role --role-name vocareumvm`. The current build may already prove it works.
- [ ] **A5. Check the trust policies of `vocareum-db` and `vocareum-uc-db`.** The external ID must equal `${DATABRICKS_ACCOUNT_ID}`.
- [ ] **B2. Ask Vocareum to clear the two `VOC_FAILED` records.** Only needed if they cause trouble.
- [ ] **B4. Ask Vocareum how a clone gets enrolled.** This explains why course 224346 fails with "not enrolled".
- [ ] **B5. Ask Vocareum to warn before a secret expires.** The failure gave no sign of why it happened.

### Cleanup and decisions

- [ ] **D1. Decide on `agent2-test-workspace`.** Keep it or delete it. Nothing deletes it without your yes.
- [ ] **D2. Fix the `.env` comments.** The comments above the four ID lines still describe course 224346.
- [ ] **D3. Remove `management-workspace`.** It is `FAILED` and unrelated to this work. Your call.
- [ ] **D4. Remove the stale `00_cluster_smoke_test.ipynb` and `grade.sh`** from `/voc/scripts` if you want a clean part.
- [ ] **D5. Settle the date mismatch.** `.env.example` says 223679 ended Aug-07. The API said Aug-10 before the change.
- [ ] **D6. Put the findings in `docs/`.** Add the `no databricks service principal token` error and the Settings page to `docs/vocareum-api.md` and `docs/diagnosing.md`. Do not close an open question without a measurement.
- [ ] **D7. Account limit.** The tier caps active workspaces, and `agent2-test-workspace` plus `management-workspace` plus the Vocareum one count toward it.

## Suggested order

- **First:** N1, then N2 and N3.
- **Then:** N4 and N5.
- **Last:** the Still open and Cleanup lists.
