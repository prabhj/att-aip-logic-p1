"""Shared library for the AIP Logic archetype. Used by the setup/batch jobs AND by the Actions API app.

Plain Python + PyYAML (no Spark), so it can be unit-tested anywhere.
* Loads the Action inventory (config/actions.yml) and the Logic configs (config/logics/*.yml) and validates them.
* Generates SQL from config: object tables, the Logic's UC SQL function (ai_query) and the Action's UC procedure.

Safety rule: every identifier comes from config and is validated against a strict regex before it is quoted.
Values that come from payloads (object ids, proposed values, run ids) are never put into SQL text; callers bind
them as parameters.
"""
import glob
import os
import re

import yaml

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}")
_PLACEHOLDER = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ENDPOINT = re.compile(r"[A-Za-z0-9_.\-]{1,128}")
_PRINCIPAL = re.compile(r"[A-Za-z0-9_.@+\- ]{1,255}")
ACTION_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_]{0,63}")
OBJECT_ID = re.compile(r"[A-Za-z0-9_.:\-]{1,128}")

OUTBOX_TABLE = "logic_outbox"
AUDIT_TABLE = "action_audit"


# --------------------------------------------------------------------------- SQL quoting helpers
def ident(name):
    """Back-quoted identifier, only if it matches a strict pattern (no quoting tricks possible)."""
    if not isinstance(name, str) or not _IDENT.fullmatch(name):
        raise ValueError(f"unsafe or invalid identifier: {name!r}")
    return f"`{name}`"


def lit(value):
    """Databricks SQL string literal for CONFIG text (prompts, comments). Never use it for payload values."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def fq(catalog, schema, name):
    return f"{ident(catalog)}.{ident(schema)}.{ident(name)}"


def principal(p):
    """Quoted principal (user email, group name or service principal application id) for GRANT statements."""
    if not isinstance(p, str) or not _PRINCIPAL.fullmatch(p):
        raise ValueError(f"invalid principal: {p!r}")
    return f"`{p}`"


# --------------------------------------------------------------------------- config loading + validation
def _read(path):
    with open(path) as fh:
        return yaml.safe_load(fh) or {}


def load_actions(config_dir):
    """Action inventory: action name -> which table / id column / property it may edit, and its UC procedure."""
    raw = _read(os.path.join(config_dir, "actions.yml"))
    actions = {}
    for name, spec in (raw.get("actions") or {}).items():
        if not ACTION_NAME.fullmatch(name):
            raise ValueError(f"invalid action name {name!r}")
        for key in ("object_table", "id_column", "property", "procedure"):
            ident(spec[key])
        spec = dict(spec, name=name)
        spec["max_value_length"] = int(spec.get("max_value_length", 20000))
        actions[name] = spec
    if not actions:
        raise ValueError("config/actions.yml defines no actions")
    return actions


def load_logics(config_dir, actions):
    """All Logic configs in config/logics/*.yml, validated against the Action inventory."""
    logics = {}
    for path in sorted(glob.glob(os.path.join(config_dir, "logics", "*.yml"))):
        cfg = _read(path)
        name = cfg["logic_name"]
        ident(name)
        if name in logics:
            raise ValueError(f"duplicate logic_name {name!r} in {path}")
        obj, llm, act = cfg["object"], cfg["llm"], cfg["action"]
        for key in ("table", "id_column", "target_property"):
            ident(obj[key])
        inputs = obj["input_properties"]
        if not inputs:
            raise ValueError(f"{path}: input_properties is empty")
        for p in inputs:
            ident(p)
        if obj["target_property"] in inputs:
            raise ValueError(f"{path}: target_property cannot also be an input property")
        ident(llm["function_name"])
        missing = set(_PLACEHOLDER.findall(llm["user_prompt"])) - set(inputs)
        if missing:
            raise ValueError(f"{path}: user_prompt uses properties not in input_properties: {sorted(missing)}")
        if llm.get("endpoint") and not _ENDPOINT.fullmatch(llm["endpoint"]):
            raise ValueError(f"{path}: invalid endpoint name")
        for k, v in (llm.get("model_parameters") or {}).items():
            ident(k)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ValueError(f"{path}: model parameter {k} must be a number")
        spec = actions.get(act["name"])
        if not spec:
            raise ValueError(f"{path}: action {act['name']!r} is not in the action inventory")
        if (spec["object_table"], spec["id_column"], spec["property"]) != (obj["table"], obj["id_column"], obj["target_property"]):
            raise ValueError(f"{path}: action {act['name']!r} edits {spec['object_table']}.{spec['property']}, "
                             f"not {obj['table']}.{obj['target_property']}")
        cfg["_path"] = path
        logics[name] = cfg
    return logics


def load_all(config_dir):
    actions = load_actions(config_dir)
    return actions, load_logics(config_dir, actions)


# --------------------------------------------------------------------------- DDL generation
def object_tables(actions, logics):
    """table -> {id_column, inputs, targets}, merged across all actions and logics that use the table."""
    tables = {}
    for a in actions.values():
        t = tables.setdefault(a["object_table"], {"id_column": a["id_column"], "inputs": [], "targets": []})
        if t["id_column"] != a["id_column"]:
            raise ValueError(f"table {a['object_table']} has two id columns in the inventory")
        if a["property"] not in t["targets"]:
            t["targets"].append(a["property"])
    for lg in logics.values():
        t = tables[lg["object"]["table"]]
        for p in lg["object"]["input_properties"]:
            if p not in t["inputs"] and p not in t["targets"]:
                t["inputs"].append(p)
    return tables


def object_columns(t):
    cols = [(t["id_column"], "STRING NOT NULL")] + [(p, "STRING") for p in t["inputs"]]
    for p in t["targets"]:
        cols += [(p, "STRING"), (p + "_updated_at", "TIMESTAMP"), (p + "_updated_by", "STRING")]
    return cols


def object_table_ddl(catalog, schema, table, t):
    cols = ", ".join(f"{ident(c)} {typ}" for c, typ in object_columns(t))
    comment = (f"SYNTHETIC DEMO DATA. Objects for the AIP Logic archetype. Properties {', '.join(t['targets'])} "
               f"are written only through the Actions API (UC Action procedures).")
    return (f"CREATE TABLE IF NOT EXISTS {fq(catalog, schema, table)} ({cols}, "
            f"CONSTRAINT {ident('pk_' + table)} PRIMARY KEY ({ident(t['id_column'])})) COMMENT {lit(comment)}")


def outbox_ddl(catalog, schema):
    return (f"CREATE TABLE IF NOT EXISTS {fq(catalog, schema, OUTBOX_TABLE)} (proposal_id STRING NOT NULL, "
            "logic_name STRING, action STRING, run_id STRING, object_id STRING, proposed_value STRING, status STRING, "
            "http_status INT, response STRING, executed_as STRING, created_at TIMESTAMP, sent_at TIMESTAMP) "
            "COMMENT 'Proposed edits from batch Logic runs (staged before they are sent to the Actions API). "
            "status: PENDING | LLM_ERROR | SENT | NOT_FOUND | FAILED'")


def audit_ddl(catalog, schema):
    return (f"CREATE TABLE IF NOT EXISTS {fq(catalog, schema, AUDIT_TABLE)} (audit_id STRING NOT NULL, "
            "request_id STRING, action STRING, object_table STRING, object_id STRING, property STRING, caller STRING, "
            "executed_as STRING, auth_mode STRING, triggered_by STRING, logic_name STRING, run_id STRING, "
            "status STRING, error STRING, value_length INT, at TIMESTAMP) "
            "COMMENT 'Written only by the Actions API app service principal, one row per request (applied, not_found, error)'")


def prompt_expr(cfg):
    """concat(system prompt, user prompt) with {property} placeholders replaced by the function parameters."""
    llm = cfg["llm"]
    parts = [lit(llm["system_prompt"].strip() + "\n\n")]
    for i, piece in enumerate(_PLACEHOLDER.split(llm["user_prompt"])):
        if i % 2:
            parts.append(f"coalesce({ident('p_' + piece)}, '')")
        elif piece:
            parts.append(lit(piece))
    return "concat(" + ", ".join(parts) + ")"


def function_name(cfg, catalog, schema):
    return fq(catalog, schema, cfg["llm"]["function_name"])


def function_ddl(cfg, catalog, schema, default_endpoint):
    """The Logic's LLM step as a governed UC SQL function. It only PROPOSES a value (returns NULL on model error)."""
    llm, obj = cfg["llm"], cfg["object"]
    endpoint = llm.get("endpoint") or default_endpoint
    if not _ENDPOINT.fullmatch(endpoint):
        raise ValueError(f"invalid endpoint {endpoint!r}")
    params = ", ".join(f"{ident('p_' + p)} STRING" for p in obj["input_properties"])
    mp = llm.get("model_parameters") or {}
    mp_sql = (", modelParameters => named_struct(" + ", ".join(f"{lit(k)}, {v!r}" for k, v in mp.items()) + ")") if mp else ""
    comment = (f"AIP Logic {cfg['logic_name']}: LLM step only. Proposes a value for {obj['table']}.{obj['target_property']}; "
               f"the write goes through Action {cfg['action']['name']}. Generated from {os.path.basename(cfg['_path'])}. "
               f"Endpoint {endpoint}. Returns NULL if the model call fails.")
    return (f"CREATE OR REPLACE FUNCTION {function_name(cfg, catalog, schema)}({params})\nRETURNS STRING\n"
            f"COMMENT {lit(comment)}\n"
            f"RETURN ai_query({lit(endpoint)}, {prompt_expr(cfg)}{mp_sql}, failOnError => false).result")


def function_call(cfg, catalog, schema):
    """fn(`col1`, `col2`) for use in SELECT ... FROM <object table>."""
    args = ", ".join(ident(p) for p in cfg["object"]["input_properties"])
    return f"{function_name(cfg, catalog, schema)}({args})"


def procedure_name(action, catalog, schema):
    return fq(catalog, schema, action["procedure"])


def procedure_ddl(action, catalog, schema):
    """The Action's edit as a UC procedure (SQL SECURITY DEFINER).

    Callers only need EXECUTE on it, not MODIFY on the table, so the table cannot be edited around the Actions API.
    session_user() is the identity that issued the CALL (the caller), which is recorded as *_updated_by.
    The result set (last SELECT) is: status ('applied' | 'not_found' | 'error'), matched, executed_as.
    """
    t = fq(catalog, schema, action["object_table"])
    idc, prop = ident(action["id_column"]), action["property"]
    comment = (f"Action {action['name']}: sets {action['object_table']}.{prop} for one object. "
               f"Called by the Actions API as the caller; runs with the owner's rights (SQL SECURITY DEFINER).")
    return f"""CREATE OR REPLACE PROCEDURE {procedure_name(action, catalog, schema)}(p_object_id STRING, p_value STRING)
LANGUAGE SQL
SQL SECURITY DEFINER
COMMENT {lit(comment)}
AS BEGIN
  DECLARE n BIGINT DEFAULT 0;
  IF p_value IS NULL OR length(p_value) > {int(action['max_value_length'])} THEN
    SELECT 'error' AS status, CAST(0 AS BIGINT) AS matched, session_user() AS executed_as;
  ELSE
    SET n = (SELECT count(*) FROM {t} WHERE {idc} = p_object_id);
    IF n = 1 THEN
      UPDATE {t} SET {ident(prop)} = p_value, {ident(prop + '_updated_at')} = current_timestamp(),
        {ident(prop + '_updated_by')} = session_user() WHERE {idc} = p_object_id;
    END IF;
    SELECT CASE WHEN n = 1 THEN 'applied' WHEN n = 0 THEN 'not_found' ELSE 'error' END AS status,
           n AS matched, session_user() AS executed_as;
  END IF;
END"""
