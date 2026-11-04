"""logic_batch / verify: prove the pattern held for this run, fail the job otherwise.

1. every proposal of the run was sent and applied by the Actions API
2. every send was audited by the API (audit row with the same request_id, status applied)
3. the object table holds exactly the sent values
4. *_updated_by on each object is the identity the API reported as executed_as
5. the run-as identity cannot write the object table directly (only through the Action)
"""
import argparse
import sys

parser = argparse.ArgumentParser()
for arg in ["--src", "--catalog", "--schema", "--logic-name", "--run-id"]:
    parser.add_argument(arg, default="")
A = parser.parse_args()
sys.path.insert(0, A.src)

import aip_logic as L  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
C, S = A.catalog, A.schema
actions, logics = L.load_all(f"{A.src}/config")
cfg = logics[A.logic_name]
o = cfg["object"]
obj_t, outbox, audit = L.fq(C, S, o["table"]), L.fq(C, S, L.OUTBOX_TABLE), L.fq(C, S, L.AUDIT_TABLE)
idc, tgt = L.ident(o["id_column"]), L.ident(o["target_property"])
by = L.ident(o["target_property"] + "_updated_by")
args = {"run_id": A.run_id, "logic": A.logic_name}
run = "x.run_id = :run_id AND x.logic_name = :logic"


def q(stmt):
    return spark.sql(stmt, args=args).first()[0]


proposals = q(f"SELECT count(*) FROM {outbox} x WHERE {run}")
sent = q(f"SELECT count(*) FROM {outbox} x WHERE {run} AND x.status = 'SENT'")
audited = q(f"""SELECT count(DISTINCT a.request_id) FROM {audit} a JOIN {outbox} x ON a.request_id = x.proposal_id
               WHERE {run} AND x.status = 'SENT' AND a.status = 'applied' AND a.run_id = :run_id""")
written = q(f"""SELECT count(*) FROM {obj_t} t JOIN {outbox} x ON t.{idc} = x.object_id
               WHERE {run} AND x.status = 'SENT' AND t.{tgt} = x.proposed_value""")
attributed = q(f"""SELECT count(*) FROM {obj_t} t JOIN {outbox} x ON t.{idc} = x.object_id
                  WHERE {run} AND x.status = 'SENT' AND t.{by} = x.executed_as""")
run_as = spark.sql("SELECT current_user()").first()[0]
try:   # analysis-time permission check; WHERE FALSE so nothing could change even if it were allowed
    spark.sql(f"UPDATE {obj_t} SET {tgt} = {tgt} WHERE FALSE")
    bypass_blocked, bypass_msg = False, "direct UPDATE was allowed"
except Exception as e:
    msg = str(e)
    bypass_blocked = any(k in msg for k in ("PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS", "does not have MODIFY"))
    bypass_msg = msg.splitlines()[0][:160]

print(f"run {A.run_id} | logic {A.logic_name} | run_as {run_as} | proposals staged: {proposals}")
if proposals == 0:
    print("INFO no proposals (every object already has a value); rerun with mode=all")
checks = [
    ("every proposal sent and applied", sent == proposals, f"{sent}/{proposals}"),
    ("every send audited by the API", audited == sent, f"{audited}/{sent}"),
    ("object table holds exactly the sent values", written == sent, f"{written}/{sent}"),
    ("updated_by = executed_as reported by the API", attributed == sent, f"{attributed}/{sent}"),
    ("run-as identity cannot write the table directly", bypass_blocked, bypass_msg),
]
for name, ok, detail in checks:
    print(f"{'OK  ' if ok else 'FAIL'} {name}: {detail}")

print("\naudit rows for this run:")
spark.sql(f"""SELECT action, object_table, object_id, property, caller, executed_as, auth_mode, triggered_by, logic_name,
                     run_id, status, error, at FROM {audit} WHERE run_id = :run_id AND logic_name = :logic ORDER BY object_id, at""",
          args=args).show(100, truncate=False)
if not all(ok for _, ok, _ in checks):
    raise Exception("verification failed; see FAIL lines above")
print("VERIFIED")
