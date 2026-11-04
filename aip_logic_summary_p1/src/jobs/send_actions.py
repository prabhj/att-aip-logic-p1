"""logic_batch / send_actions: send each staged proposal to the Actions API and record the outcome per proposal.

call_mode=connection (default): through the UC HTTP connection proxy
    POST {workspace}/api/2.0/unity-catalog/connections/<conn>/proxy/api/actions/<action>
    The job authenticates to the proxy as its run-as service principal (needs USE CONNECTION); the proxy injects
    the connection's OAuth M2M token (also the batch service principal) when it calls the app.
call_mode=direct (fallback): OAuth M2M token from the secret scope, plain HTTPS to the app URL.

The SQL `http_request` function and the SDK's serving_endpoints.http_request are rate limited and meant for
interactive use, so this uses the proxy endpoint with bounded concurrency and retries.
"""
import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

parser = argparse.ArgumentParser()
for arg in ["--src", "--catalog", "--schema", "--logic-name", "--run-id", "--job-id", "--trigger-type", "--triggered-by",
            "--call-mode", "--connection-name", "--app-name", "--secret-scope", "--concurrency"]:
    parser.add_argument(arg, default="")
A = parser.parse_args()
sys.path.insert(0, A.src)

import aip_logic as L  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.core import Config  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()
host = w.config.host.rstrip("/")
C, S = A.catalog, A.schema
actions, logics = L.load_all(f"{A.src}/config")
cfg = logics[A.logic_name]
action = cfg["action"]["name"]
outbox = L.fq(C, S, L.OUTBOX_TABLE)
run_as = spark.sql("SELECT current_user()").first()[0]

# ---- who triggered this run (the job itself always runs as the service principal)
triggered_by = A.triggered_by.strip()
if not triggered_by:
    try:
        creator = w.jobs.get_run(int(A.run_id)).creator_user_name
    except Exception as e:  # the run-as principal may lack permission to read the run
        creator = None
        print("could not read run creator:", str(e)[:200])
    triggered_by = f"{creator or 'unknown'} ({A.trigger_type or 'unknown'} trigger, job {A.job_id} run {A.run_id})"
print(f"run_as: {run_as} | triggered_by: {triggered_by} | call_mode: {A.call_mode}")

# ---- transport
if A.call_mode == "connection":
    url = f"{host}/api/2.0/unity-catalog/connections/{A.connection_name}/proxy/api/actions/{action}"
    auth = w.config.authenticate                    # the job's own identity -> UC proxy
elif A.call_mode == "direct":
    from databricks.sdk.runtime import dbutils
    url = f"{w.apps.get(A.app_name).url.rstrip('/')}/api/actions/{action}"
    sp = Config(host=host, client_id=dbutils.secrets.get(A.secret_scope, "sp_client_id"),
                client_secret=dbutils.secrets.get(A.secret_scope, "sp_client_secret"))
    auth = sp.authenticate                          # OAuth M2M token for the batch SP -> app
else:
    raise ValueError("call_mode must be 'connection' or 'direct'")
print("endpoint:", url)

RETRY = {429, 500, 502, 503, 504}


def send(row):
    payload = {"object_id": row.object_id, "value": row.proposed_value, "logic_name": A.logic_name,
               "run_id": A.run_id, "triggered_by": triggered_by, "request_id": row.proposal_id}
    code, body = None, ""
    for attempt in range(4):
        try:
            r = requests.post(url, headers={**auth(), "Content-Type": "application/json"}, data=json.dumps(payload), timeout=120)
            code, body = r.status_code, r.text
            if code not in RETRY:
                break
        except requests.RequestException as e:
            code, body = None, f"{type(e).__name__}: {e}"
        time.sleep(2 ** attempt)
    try:
        res = json.loads(body)
    except ValueError:
        res = {}
    api_status = res.get("status") if isinstance(res, dict) else None
    status = {"applied": "SENT", "not_found": "NOT_FOUND"}.get(api_status, "FAILED")
    return (row.proposal_id, status, code, body[:4000], res.get("executed_as") if isinstance(res, dict) else None,
            datetime.now(timezone.utc).replace(tzinfo=None))


rows = spark.sql(f"SELECT proposal_id, object_id, proposed_value FROM {outbox} "
                 f"WHERE run_id = :run_id AND logic_name = :logic AND status IN ('PENDING', 'FAILED')",
                 args={"run_id": A.run_id, "logic": A.logic_name}).collect()
with ThreadPoolExecutor(max_workers=max(1, int(A.concurrency or 4))) as pool:
    results = list(pool.map(send, rows))

if results:
    spark.createDataFrame(results, "proposal_id STRING, status STRING, http_status INT, response STRING, "
                                   "executed_as STRING, sent_at TIMESTAMP").createOrReplaceTempView("send_results")
    spark.sql(f"""MERGE INTO {outbox} t USING send_results s ON t.proposal_id = s.proposal_id
      WHEN MATCHED THEN UPDATE SET status = s.status, http_status = s.http_status, response = s.response,
                                   executed_as = s.executed_as, sent_at = s.sent_at""")
counts = {k: sum(r[1] == k for r in results) for k in ("SENT", "NOT_FOUND", "FAILED")}
print(f"sent to {action}: {len(results)} | " + " | ".join(f"{k}: {v}" for k, v in counts.items()))
for r in results:
    print(f"  {r[0][:8]}  {r[1]:<9} HTTP {r[2]}  executed_as={r[4]}")
if counts["FAILED"] or counts["NOT_FOUND"]:
    print("first problem response:", next(r[3] for r in results if r[1] != "SENT")[:1000])
