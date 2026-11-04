"""logic_setup / setup_uc (idempotent, runs as the deploying user, who becomes the owner of the generated objects).

1. schema, object tables (columns derived from config), outbox and audit tables
2. synthetic objects (demo table only, only if empty)
3. one governed UC SQL function per Logic config (the LLM step) and one UC procedure per Action (the edit)
4. least-privilege grants for the batch service principal, the app service principal and optional demo users
5. smoke test: each Logic function on one object (writes nothing)
"""
import argparse
import random
import sys

parser = argparse.ArgumentParser()
for arg in ["--src", "--catalog", "--schema", "--model-endpoint", "--batch-sp", "--app-name", "--demo-principals",
            "--reset-data"]:
    parser.add_argument(arg, default="")
A = parser.parse_args()
sys.path.insert(0, A.src)

import aip_logic as L  # noqa: E402
from databricks.sdk import WorkspaceClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()
C, S = A.catalog, A.schema
SCHEMA = f"{L.ident(C)}.{L.ident(S)}"
actions, logics = L.load_all(f"{A.src}/config")
tables = L.object_tables(actions, logics)
print(f"actions: {list(actions)} | logics: {list(logics)} | object tables: {list(tables)}")

# ---------------------------------------------------------------- 1. schema and tables
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA} COMMENT 'AIP Logic archetype demo (bundle aip_logic_summary_p1). SYNTHETIC DATA.'")
spark.sql(f"USE {SCHEMA}")  # UC procedures capture the current catalog/schema when they are created

if A.reset_data.lower() == "true":
    for t in list(tables) + [L.OUTBOX_TABLE, L.AUDIT_TABLE]:
        spark.sql(f"DROP TABLE IF EXISTS {L.fq(C, S, t)}")
        print("reset: dropped", t)

for name, t in tables.items():
    full = L.fq(C, S, name)
    spark.sql(L.object_table_ddl(C, S, name, t))
    existing = {f.name for f in spark.table(full).schema.fields}
    for col, typ in L.object_columns(t):          # a new Logic may need a new input/target property
        if col not in existing:
            spark.sql(f"ALTER TABLE {full} ADD COLUMN {L.ident(col)} {typ.replace(' NOT NULL', '')}")
            print(f"added column {name}.{col}")
    try:
        spark.sql(f"ALTER TABLE {full} SET TAGS ('data_classification' = 'synthetic', 'demo' = 'aip_logic_summary_p1')")
    except Exception as e:  # tags need APPLY TAG; not essential
        print("tagging skipped:", str(e)[:200])
spark.sql(L.outbox_ddl(C, S))
spark.sql(L.audit_ddl(C, S))


# ---------------------------------------------------------------- 2. synthetic objects
def demo_documents_rows():
    """12 deterministic, fictional contract excerpts (no real customers or prices)."""
    rnd = random.Random(42)
    customers = ["Blue Harbor Manufacturing", "Meridian Health Partners", "Northgate Retail Group", "Silverline Logistics",
                 "Crestview Financial Services", "Orchard Valley Foods", "Pinnacle Engineering", "Riverstone Hospitality",
                 "Summit Pharma Holdings", "Tidewater Energy", "Westbrook Media", "Granite Peak Insurance"]
    services = ["Dedicated Internet Access", "MPLS VPN", "Ethernet Private Line", "SD-WAN", "Business Fiber"]
    rows = []
    for i, cust in enumerate(customers, 1):
        svc, speed = rnd.choice(services), rnd.choice(["100 Mbps", "500 Mbps", "1 Gbps", "10 Gbps"])
        mrc, nrc, term = rnd.randint(8, 90) * 100, rnd.choice([0, 500, 1500]), rnd.choice([12, 24, 36])
        included = rnd.random() < 0.5
        text = (f"This Pricing Addendum is entered into between Example Telecom Inc. (the Provider) and {cust}. "
                f"The Provider supplies {svc} at {speed} for a term of {term} months. The monthly recurring charge is "
                f"{mrc:,.2f} USD and the one-time installation charge is {nrc:,.2f} USD. Local access circuit charges are "
                f"{'included' if included else 'not included and are billed separately'}. Rates are quoted on an "
                f"Individual Case Basis and do not apply to future orders, moves, adds or changes. Either party may "
                f"terminate with {rnd.choice([30, 60, 90])} days written notice; early termination fees of "
                f"{rnd.choice([50, 75, 100])}% of the remaining monthly charges apply. Prices exclude applicable taxes.")
        rows.append({"document_id": f"DOC-{i:04d}", "title": f"Pricing Addendum - {svc} - {cust}", "document_text": text})
    return rows


SEEDERS = {"demo_documents": demo_documents_rows}
for name, t in tables.items():
    full = L.fq(C, S, name)
    if name in SEEDERS and spark.table(full).count() == 0:
        schema = spark.table(full).schema
        data = [tuple(r.get(f.name) for f in schema.fields) for r in SEEDERS[name]()]
        spark.createDataFrame(data, schema).write.mode("append").saveAsTable(full)
        print(f"seeded {len(data)} synthetic rows into {name}")
    print(f"{name}: {spark.table(full).count()} objects")

# ---------------------------------------------------------------- 3. Logic functions and Action procedures
for lg in logics.values():
    spark.sql(L.function_ddl(lg, C, S, A.model_endpoint))
    print("function  ", L.function_name(lg, C, S), "<-", lg["_path"].split("/")[-1])
for a in actions.values():
    spark.sql(L.procedure_ddl(a, C, S))
    print("procedure ", L.procedure_name(a, C, S), "<- action", a["name"])

# ---------------------------------------------------------------- 4. grants (least privilege; nobody but the owner can MODIFY objects)
app_sp = w.apps.get(A.app_name).service_principal_client_id
demo = [p.strip() for p in A.demo_principals.split(",") if p.strip()]
outbox, audit = L.fq(C, S, L.OUTBOX_TABLE), L.fq(C, S, L.AUDIT_TABLE)
grants = []
for who in [A.batch_sp, app_sp] + demo:
    to = L.principal(who)
    grants += [f"GRANT USE CATALOG ON CATALOG {L.ident(C)} TO {to}", f"GRANT USE SCHEMA ON SCHEMA {SCHEMA} TO {to}"]
    grants += [f"GRANT SELECT ON TABLE {L.fq(C, S, t)} TO {to}" for t in tables]
    grants += [f"GRANT EXECUTE ON FUNCTION {L.function_name(lg, C, S)} TO {to}" for lg in logics.values()]
    grants += [f"GRANT EXECUTE ON PROCEDURE {L.procedure_name(a, C, S)} TO {to}" for a in actions.values()]
grants += [f"GRANT SELECT, MODIFY ON TABLE {outbox} TO {L.principal(A.batch_sp)}",   # batch stages proposals
           f"GRANT SELECT ON TABLE {audit} TO {L.principal(A.batch_sp)}",             # batch verifies the audit
           f"GRANT SELECT, MODIFY ON TABLE {audit} TO {L.principal(app_sp)}"]         # only the app writes the audit
failed = []
for stmt in grants:
    try:
        spark.sql(stmt)
    except Exception as e:
        failed.append((stmt, str(e)[:200]))
print(f"grants applied: {len(grants) - len(failed)}/{len(grants)} (batch SP {A.batch_sp}, app SP {app_sp}, demo {demo})")
for stmt, err in failed:
    print("GRANT FAILED:", stmt, "->", err)
if failed:
    raise RuntimeError("some grants failed; see above")

# ---------------------------------------------------------------- 5. smoke test (no writes)
for lg in logics.values():
    o = lg["object"]
    r = spark.sql(f"SELECT {L.ident(o['id_column'])} AS id, {L.function_call(lg, C, S)} AS proposed "
                  f"FROM {L.fq(C, S, o['table'])} ORDER BY 1 LIMIT 1").first()
    print(f"smoke test {lg['logic_name']} on {r.id}: {r.proposed}")
    if not r.proposed:
        raise RuntimeError(f"Logic function {lg['llm']['function_name']} returned NULL (model endpoint or permissions)")
