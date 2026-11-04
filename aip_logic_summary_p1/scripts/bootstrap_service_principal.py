"""One-time bootstrap. Run BEFORE the first `databricks bundle deploy`. Idempotent; needs a workspace admin.

Creates what a bundle cannot (the job's run_as and the app permissions reference the principal at deploy time,
and secret VALUES must never be in YAML):
  1. the batch service principal (display name = bundle variable batch_sp_name)
  2. an OAuth secret for it (M2M), stored in a secret scope as sp_client_id / sp_client_secret
  3. READ on the scope for the principal (used by call_mode=direct); the UC connection references the same keys
The secret is never printed. --rotate creates a new OAuth secret and overwrites the stored one.

Run locally:   python scripts/bootstrap_service_principal.py --profile <profile>
or paste into a notebook cell and call bootstrap().
"""
import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, workspace

DEFAULT_NAME = "aip-logic-summary-p1-batch"
DEFAULT_SCOPE = "aip_logic_summary_p1"


def bootstrap(name=DEFAULT_NAME, scope=DEFAULT_SCOPE, rotate=False, w=None):
    w = w or WorkspaceClient()
    found = list(w.service_principals.list(filter=f'displayName eq "{name}"'))
    if found:
        sp = found[0]
        print(f"service principal exists: {sp.display_name} ({sp.application_id})")
    else:
        sp = w.service_principals.create(display_name=name, active=True, entitlements=[
            iam.ComplexValue(value="workspace-access"), iam.ComplexValue(value="databricks-sql-access")])
        print(f"service principal created: {sp.display_name} ({sp.application_id})")

    if scope not in {s.name for s in w.secrets.list_scopes()}:
        w.secrets.create_scope(scope=scope)
        print(f"secret scope created: {scope}")
    keys = {k.key for k in w.secrets.list_secrets(scope=scope)}
    if rotate or not {"sp_client_id", "sp_client_secret"} <= keys:
        secret = w.service_principal_secrets_proxy.create(service_principal_id=sp.id)
        w.secrets.put_secret(scope=scope, key="sp_client_id", string_value=sp.application_id)
        w.secrets.put_secret(scope=scope, key="sp_client_secret", string_value=secret.secret)
        print(f"OAuth secret created for the service principal and stored in {scope} (sp_client_id, sp_client_secret)")
    else:
        print(f"OAuth secret already stored in {scope}; use --rotate to replace it")
    w.secrets.put_acl(scope=scope, principal=sp.application_id, permission=workspace.AclPermission.READ)
    print(f"READ on scope {scope} -> {sp.application_id}")
    print(f"\nDone. The bundle resolves the principal by display name (variable batch_sp_name = {name}).")
    return sp.application_id


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--name", default=DEFAULT_NAME)
    p.add_argument("--scope", default=DEFAULT_SCOPE)
    p.add_argument("--profile", default=None)
    p.add_argument("--rotate", action="store_true")
    a = p.parse_args()
    bootstrap(a.name, a.scope, a.rotate, WorkspaceClient(profile=a.profile) if a.profile else None)
