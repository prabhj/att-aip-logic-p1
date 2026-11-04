"""logic_setup / create_connection: UC HTTP connection to the Actions API app (OAuth M2M as the batch service principal).

Bundles cannot declare UC connections, so this task creates it. The OAuth client id/secret are referenced with
secret(scope, key); they never appear in code, YAML or job parameters. Only the batch service principal gets
USE CONNECTION, because anyone who can use the connection calls the Actions API as that principal.
Ends with a connectivity test through the UC connections proxy (GET /api/whoami on the app).

CREATE CONNECTION is a metastore-level privilege. If the deployer lacks it and the bundle's call_mode is not
'connection', the task prints SKIPPED and succeeds (the batch then uses call_mode=direct); otherwise it fails.
"""
import argparse
import re
import sys

import requests

parser = argparse.ArgumentParser()
for arg in ["--src", "--app-name", "--connection-name", "--secret-scope", "--batch-sp", "--call-mode"]:
    parser.add_argument(arg, default="")
A = parser.parse_args()
sys.path.insert(0, A.src)

import aip_logic as L  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()
host = w.config.host.rstrip("/")

app = w.apps.get(A.app_name)
if not app.url:
    raise RuntimeError("The app has no URL yet. Run `databricks bundle run actions_api` first, then rerun logic_setup.")
app_url = app.url.rstrip("/")
if not re.fullmatch(r"https://[A-Za-z0-9.\-]+", app_url):
    raise ValueError(f"unexpected app url {app_url}")
if not re.fullmatch(r"[A-Za-z0-9_\-]{1,128}", A.secret_scope):
    raise ValueError("invalid secret scope name")
conn = L.ident(A.connection_name)

# DROP + CREATE keeps it idempotent (the app URL or secrets may have changed); the grant is re-applied below.
created = True
try:
    spark.sql(f"DROP CONNECTION IF EXISTS {conn}")
    spark.sql(f"""CREATE CONNECTION {conn} TYPE HTTP OPTIONS (
      host {L.lit(app_url)}, port '443', base_path '/',
      client_id secret({L.lit(A.secret_scope)}, 'sp_client_id'),
      client_secret secret({L.lit(A.secret_scope)}, 'sp_client_secret'),
      oauth_scope 'all-apis',
      token_endpoint {L.lit(host + '/oidc/v1/token')})""")
except Exception as e:
    if "CREATE CONNECTION" in str(e) and A.call_mode != "connection":
        print(f"SKIPPED: the deployer lacks CREATE CONNECTION on the metastore; call_mode={A.call_mode}, so the "
              "batch calls the Actions API directly (OAuth M2M from the secret scope). Ask a metastore admin for "
              "CREATE CONNECTION and set call_mode=connection to use the UC connection proxy.")
        created = False
    else:
        raise

if created:
    spark.sql(f"COMMENT ON CONNECTION {conn} IS " + L.lit(
        f"Actions API ({A.app_name}); OAuth M2M as batch SP {A.batch_sp}. Managed by bundle aip_logic_summary_p1 / logic_setup."))
    spark.sql(f"GRANT USE CONNECTION ON CONNECTION {conn} TO {L.principal(A.batch_sp)}")
    print(f"connection {A.connection_name} -> {app_url} (OAuth M2M, secrets from scope {A.secret_scope}); "
          f"USE CONNECTION -> {A.batch_sp}")
    # Connectivity test through the UC connections proxy: the app should see the batch service principal as caller.
    proxy = f"{host}/api/2.0/unity-catalog/connections/{A.connection_name}/proxy/api/whoami"
    r = requests.get(proxy, headers=w.config.authenticate(), timeout=60)
    print(f"proxy test: HTTP {r.status_code} {r.text[:1000]}")
    if r.status_code != 200:
        raise RuntimeError("The UC connection could not reach the Actions API; see the response above")
