"""logic_batch / propose: run the Logic's UC function over the objects and stage proposals in the outbox.

Nothing is written to the object table here (the run-as service principal has no MODIFY on it).
mode=missing: only objects whose target property is NULL; mode=all: every object.
Re-runnable for the same run_id (task repair): unsent proposals of this run are replaced, sent ones are kept.
"""
import argparse
import sys

parser = argparse.ArgumentParser()
for arg in ["--src", "--catalog", "--schema", "--logic-name", "--run-id", "--mode"]:
    parser.add_argument(arg, default="")
A = parser.parse_args()
sys.path.insert(0, A.src)

import aip_logic as L  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
C, S = A.catalog, A.schema
actions, logics = L.load_all(f"{A.src}/config")
if A.logic_name not in logics:
    raise ValueError(f"unknown logic {A.logic_name!r}; configured: {list(logics)}")
if A.mode not in ("missing", "all"):
    raise ValueError("mode must be 'missing' or 'all'")
cfg = logics[A.logic_name]
o = cfg["object"]
obj_t, outbox = L.fq(C, S, o["table"]), L.fq(C, S, L.OUTBOX_TABLE)
idc, target = L.ident(o["id_column"]), L.ident(o["target_property"])
args = {"run_id": A.run_id, "logic": A.logic_name, "action": cfg["action"]["name"]}
print(f"run_as: {spark.sql('SELECT current_user()').first()[0]} | logic {A.logic_name} | mode {A.mode} | run {A.run_id}")

spark.sql(f"DELETE FROM {outbox} WHERE run_id = :run_id AND logic_name = :logic AND status IN ('PENDING', 'LLM_ERROR')", args=args)
where = f"{target} IS NULL" if A.mode == "missing" else "TRUE"
spark.sql(f"""
INSERT INTO {outbox} (proposal_id, logic_name, action, run_id, object_id, proposed_value, status, created_at)
SELECT uuid(), :logic, :action, :run_id, id, proposed,
       CASE WHEN proposed IS NULL OR trim(proposed) = '' THEN 'LLM_ERROR' ELSE 'PENDING' END, current_timestamp()
FROM (SELECT {idc} AS id, {L.function_call(cfg, C, S)} AS proposed
      FROM {obj_t}
      WHERE {where}
        AND {idc} NOT IN (SELECT object_id FROM {outbox} WHERE run_id = :run_id AND logic_name = :logic AND status = 'SENT'))
""", args=args)

for row in spark.sql(f"SELECT status, count(*) AS n FROM {outbox} WHERE run_id = :run_id AND logic_name = :logic GROUP BY status ORDER BY status",
                     args=args).collect():
    print(f"  {row.status}: {row.n}")
print("proposals staged; next task sends them to the Actions API")
