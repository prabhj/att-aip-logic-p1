# AIP Logic on Databricks: object -> LLM -> Actions API -> object (demo)

Palantir AIP Logic archetype rebuilt with Databricks primitives, deployed as a Declarative Automation Bundle.
The Actions API (FastAPI on Databricks Apps) is a **placeholder** for the real Actions/Ontology service; it exists
to show the API pattern, not as a production service.

| AIP Logic concept | Databricks primitive |
| --- | --- |
| Object type / object | UC Delta table row (`demo_documents`) with `<property>_updated_by/_at` columns |
| Logic (LLM block) | Governed UC SQL function calling `ai_query` (`logic_summarize_document`), one per config file |
| Action type | UC procedure, `SQL SECURITY DEFINER` (`action_update_text_summary`); callers need EXECUTE only, not MODIFY |
| Actions API | Databricks App `POST /api/actions/<action>`; SQL runs with the caller's forwarded token |
| Action log | `action_audit` table, written only by the app's service principal |
| Automation / batch | Lakeflow Job `logic_batch` (propose -> send_actions -> verify), run as a service principal |
| On-demand run | App page `/` and `POST /api/logic/<logic>/run`, executed as the signed-in user |
| Config | `src/config/actions.yml` (action inventory) + `src/config/logics/*.yml` (one file per Logic) |

A new Logic of the same shape = a new YAML in `src/config/logics/`, then `bundle deploy` + `bundle run logic_setup`.

## Layout

* `db_yaml/databricks.yml` - the bundle root (kept out of the repo root on purpose): variables, targets,
  top-level permissions; includes `../resources/*.yml` and syncs the parent folder. Run all `bundle` commands from `db_yaml/`.
  dev uses `sync_mode: overwrite` (files copied to `.bundle/...`), because the run-as SP cannot read your git folder.
* `resources/actions_api.app.yml` - the app (source = `src/`, so it reads the same config and library as the jobs)
* `resources/logic.jobs.yml` - `logic_setup` and `logic_batch`
* `src/aip_logic.py` - shared library: config loading, identifier validation, DDL/SQL generation
* `src/app.py` - Actions API + on-demand page
* `src/jobs/` - `setup.py`, `create_connection.py`, `propose.py`, `send_actions.py`, `verify.py`
* `scripts/bootstrap_service_principal.py` - one-time: batch SP, OAuth secret, secret scope

## Run it

1. One time (workspace admin): `python scripts/bootstrap_service_principal.py` (or call `bootstrap()` in a notebook).
2. Grant yourself **Service principal: User** on `aip-logic-summary-p1-batch` (Settings > Identity and access >
   Service principals > the SP > Permissions). Needed because `logic_batch` runs as that SP.
3. `cd db_yaml && databricks bundle deploy -t dev`
4. `databricks bundle run actions_api -t dev` (starts the app and deploys its source)
5. `databricks bundle run logic_setup -t dev` (tables, seed data, functions, procedures, grants, connection, smoke test)
6. On demand: open the app URL, pick an object, Run. The result shows who ran the LLM step and the edit.
7. Batch: `databricks bundle run logic_batch -t dev` (params: `logic_name`, `mode=missing|all`, `call_mode`).

## What has been verified in this workspace

* `logic_setup` succeeds: 12 seed objects, UC function + DEFINER procedure generated from config, 13/13 grants,
  LLM smoke test on DOC-0001 returns a summary, UC HTTP connection created and the proxy test reaches the app.
* `logic_batch` (run 409782019054629, `mode=all`, `call_mode=connection`) succeeds: 12/12 proposals sent through the
  UC connection proxy and applied; verify passes all 5 checks, including PERMISSION_DENIED when the batch SP
  tries to UPDATE the table directly (it has SELECT only; writes go through the Action procedure).
* Independent query: all 12 rows have summaries, `text_summary_updated_by` = batch SP, 12 audit rows with
  executed_as = caller = SP and `auth_mode = caller_token`.
* Action path as the batch SP (OAuth M2M -> app): `/api/whoami` shows the forwarded token and executed_as = SP;
  `POST /api/actions/updateTextSummary` returns `applied`, the row's `text_summary_updated_by` = SP, and an
  audit row (`caller_token`, same `request_id`) is written.
* A payload with an extra `object_table` field is rejected (400) and still audited.

## Shortfalls (accepted for the demo)

* **Mock Actions API.** FastAPI app with a minimal HTML page; no OpenAPI auth docs, rate limiting, versioning,
  idempotency store, or HA guarantees. In production this is the real Actions service.
* **Metastore privilege needed.** `CREATE CONNECTION` is metastore-level (connections cannot be scoped to a
  catalog or schema). Without it, set `call_mode: direct` (OAuth M2M with the SP secret from the secret scope);
  `create_connection` then prints SKIPPED instead of failing.
* **Manual prerequisites.** The SP, its secret, and the deployer's `Service principal: User` role are outside the
  bundle (bootstrap script + UI).
* **On-demand path as a human user not yet exercised end to end**; it needs a browser (OAuth U2M) session on the app.
* **Config duplication.** Catalog/schema are passed to the app as env vars and to jobs as parameters; the action
  and logic configs themselves are single-sourced (the app reads `src/config`).
* **Long-lived secret.** `direct` mode stores an SP OAuth secret in a secret scope; rotate with
  `bootstrap_service_principal.py --rotate`. Prefer the UC connection when available.
* **Model limits.** `databricks-gpt-5-5` rejects `temperature`, so outputs are not deterministic; LLM failures are
  recorded as `LLM_ERROR` in the outbox rather than retried with backoff.
* **Tag policy.** The workspace's `data_classification` tag policy rejects `synthetic`, so table tagging is skipped.
* **Audit is best effort.** The edit and the audit insert are separate statements; if the audit insert fails the
  response says `audit_written: false` but the edit is not rolled back.
* **Network.** Apps and UC-connection traffic paths differ for Private Link / IP access lists; not addressed here.
* **Leftover scratch schema** `pj_demo_att_catalog.zz_scratch_aip_p1` from capability probes; drop it manually
  (`DROP SCHEMA ... CASCADE`).
