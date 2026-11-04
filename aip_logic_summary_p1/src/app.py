"""Mock Actions API + on-demand Logic page (Databricks App, FastAPI). A placeholder for the real Actions/Ontology service.

POST /api/actions/{action}     apply an edit. Payload: object_id, value, logic_name, run_id, triggered_by [, request_id]
POST /api/logic/{logic}/run    on demand: run the Logic's UC function for one object as the caller, then the same action path
GET  /api/inventory            action inventory + Logic configs (read from the bundle's config files)
GET  /api/whoami               which identity a request executes as (diagnostic for token forwarding)
GET  /                         on-demand page: pick a Logic and an object, Run

Identity: SQL runs with the caller's forwarded token (Databricks Apps user authorization, x-forwarded-access-token).
If no token is forwarded, the app's own service principal runs it, and the response says so (auth_mode).
The edit itself is a UC procedure (SQL SECURITY DEFINER) chosen by the action inventory, never by the payload.
The audit row is always written by the app's service principal; callers have no MODIFY on the audit table.
"""
import html
import json
import logging
import os
import sys
import uuid

import uvicorn
from databricks import sql
from databricks.sdk.core import Config
from fastapi import FastAPI, Form, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import aip_logic as L  # noqa: E402

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("actions_api")
CFG = Config()
HOST = CFG.host.replace("https://", "").rstrip("/")
HTTP_PATH = f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}"
CAT, SCH = os.environ["CATALOG"], os.environ["SCHEMA"]
ACTIONS, LOGICS = L.load_all(os.path.join(HERE, "config"))
AUDIT = L.fq(CAT, SCH, L.AUDIT_TABLE)
PAYLOAD_KEYS = {"object_id", "value", "logic_name", "run_id", "triggered_by", "request_id"}
app = FastAPI(title="Mock Actions API", version="0.1")


class ActionError(Exception):
    def __init__(self, http, msg):
        super().__init__(msg)
        self.http, self.msg = http, msg


def token_of(request):
    return request.headers.get("x-forwarded-access-token")


def caller_of(request):
    h = request.headers
    return h.get("x-forwarded-email") or h.get("x-forwarded-preferred-username") or h.get("x-forwarded-user") or "unknown"


def connect(token):
    """(connection, auth_mode). With a forwarded token, SQL runs as the caller; otherwise as the app's service principal."""
    if token:
        return sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=token), "caller_token"
    return (sql.connect(server_hostname=HOST, http_path=HTTP_PATH, credentials_provider=lambda: CFG.authenticate),
            "app_service_principal")


def write_audit(rec):
    """One row per request, written by the app's service principal. Returns an error string or None."""
    try:
        conn, _ = connect(None)
        with conn as c, c.cursor() as cur:
            cur.execute(
                f"INSERT INTO {AUDIT} (audit_id, request_id, action, object_table, object_id, property, caller, executed_as, "
                "auth_mode, triggered_by, logic_name, run_id, status, error, value_length, at) VALUES (:audit_id, :request_id, "
                ":action, :object_table, :object_id, :property, :caller, :executed_as, :auth_mode, :triggered_by, :logic_name, "
                ":run_id, :status, :error, :value_length, current_timestamp())", rec)
        return None
    except Exception as e:
        log.exception("audit write failed")
        return str(e)[:300]


def validate(spec, payload):
    if not isinstance(payload, dict):
        raise ActionError(400, "payload must be a JSON object")
    extra = set(payload) - PAYLOAD_KEYS
    if extra:
        raise ActionError(400, f"unexpected fields {sorted(extra)}: the action inventory decides the table and property")
    oid, value = payload.get("object_id"), payload.get("value")
    if not isinstance(oid, str) or not L.OBJECT_ID.fullmatch(oid):
        raise ActionError(400, "object_id must be a string of 1-128 characters [A-Za-z0-9_.:-]")
    if not isinstance(value, str) or not value.strip():
        raise ActionError(400, "value must be a non-empty string")
    if len(value) > spec["max_value_length"]:
        raise ActionError(400, f"value longer than {spec['max_value_length']} characters")
    for k in ("logic_name", "run_id", "triggered_by", "request_id"):
        if payload.get(k) is not None and (not isinstance(payload[k], str) or len(payload[k]) > 500):
            raise ActionError(400, f"{k} must be a string of at most 500 characters")


def apply_action(action_name, payload, token, caller):
    """Validate, run the action's UC procedure as the caller, audit. Returns (http_status, body)."""
    rid = payload.get("request_id") if isinstance(payload, dict) and isinstance(payload.get("request_id"), str) else None
    rec = {"audit_id": str(uuid.uuid4()), "request_id": (rid or str(uuid.uuid4()))[:500], "action": action_name[:100],
           "object_table": None, "object_id": None, "property": None, "caller": caller, "executed_as": None,
           "auth_mode": None, "triggered_by": None, "logic_name": None, "run_id": None, "status": "error",
           "error": None, "value_length": None}
    http = 500
    try:
        spec = ACTIONS.get(action_name)
        if not spec:
            raise ActionError(404, f"unknown action {action_name!r}")
        rec.update(object_table=f"{CAT}.{SCH}.{spec['object_table']}", property=spec["property"])
        validate(spec, payload)
        rec.update(object_id=payload["object_id"], value_length=len(payload["value"]), logic_name=payload.get("logic_name"),
                   run_id=payload.get("run_id"), triggered_by=payload.get("triggered_by") or caller)
        conn, rec["auth_mode"] = connect(token)
        with conn as c, c.cursor() as cur:
            cur.execute("SELECT session_user()")
            rec["executed_as"] = cur.fetchone()[0]
            cur.execute(f"CALL {L.procedure_name(spec, CAT, SCH)}(:object_id, :value)",
                        {"object_id": payload["object_id"], "value": payload["value"]})
            status = cur.fetchone()[0]
        rec["status"] = status
        http = {"applied": 200, "not_found": 404}.get(status, 422)
        if status not in ("applied", "not_found"):
            rec["error"] = "rejected by the action procedure"
    except ActionError as e:
        rec["error"], http = e.msg, e.http
    except Exception as e:
        msg = str(e)
        rec["error"] = msg[:500]
        http = 403 if any(k in msg for k in ("PERMISSION_DENIED", "INSUFFICIENT_PERMISSIONS")) else 500
    audit_error = write_audit(rec)
    body = {k: rec[k] for k in ("status", "action", "object_table", "object_id", "property", "executed_as", "auth_mode",
                                "caller", "triggered_by", "logic_name", "run_id", "request_id", "audit_id", "error")}
    body["audit_written"] = audit_error is None
    if audit_error:
        body["audit_error"] = audit_error
    log.info("action=%s object=%s status=%s executed_as=%s auth_mode=%s", action_name, rec["object_id"], rec["status"],
             rec["executed_as"], rec["auth_mode"])
    return http, body


def run_logic(logic_name, object_id, token, caller):
    """On demand: the Logic's UC function (LLM step) as the caller, then the same action path as the batch."""
    cfg = LOGICS.get(logic_name)
    if not cfg:
        return 404, {"status": "error", "error": f"unknown logic {logic_name!r}"}
    if not isinstance(object_id, str) or not L.OBJECT_ID.fullmatch(object_id):
        return 400, {"status": "error", "error": "invalid object_id"}
    o = cfg["object"]
    conn, mode = connect(token)
    with conn as c, c.cursor() as cur:
        cur.execute(f"SELECT session_user(), {L.function_call(cfg, CAT, SCH)} FROM {L.fq(CAT, SCH, o['table'])} "
                    f"WHERE {L.ident(o['id_column'])} = :id", {"id": object_id})
        row = cur.fetchone()
    if row is None:
        return 404, {"status": "not_found", "object_id": object_id}
    llm_as, value = row[0], row[1]
    out = {"logic_name": logic_name, "object_id": object_id, "llm_step_executed_as": llm_as, "llm_auth_mode": mode,
           "proposed_value": value}
    if not value:
        return 502, dict(out, status="error", error="the LLM step returned no value")
    payload = {"object_id": object_id, "value": value, "logic_name": logic_name, "run_id": f"on-demand-{uuid.uuid4()}",
               "triggered_by": caller, "request_id": str(uuid.uuid4())}
    http, result = apply_action(cfg["action"]["name"], payload, token, caller)
    return http, dict(out, status=result["status"], action_result=result)


# ------------------------------------------------------------------------------------------------ JSON API
@app.post("/api/actions/{action}")
async def action_endpoint(action: str, request: Request):
    raw = await request.body()
    try:
        payload = json.loads(raw or b"null")
    except ValueError:
        payload = "<invalid json>"
    http, body = await run_in_threadpool(apply_action, action, payload, token_of(request), caller_of(request))
    return JSONResponse(body, status_code=http)


@app.post("/api/logic/{logic_name}/run")
async def logic_run_endpoint(logic_name: str, request: Request):
    try:
        body = json.loads((await request.body()) or b"{}")
    except ValueError:
        body = {}
    oid = body.get("object_id") if isinstance(body, dict) else None
    http, out = await run_in_threadpool(run_logic, logic_name, oid, token_of(request), caller_of(request))
    return JSONResponse(out, status_code=http)


@app.get("/api/whoami")
def whoami(request: Request):
    token = token_of(request)
    conn, mode = connect(token)
    with conn as c, c.cursor() as cur:
        cur.execute("SELECT session_user()")
        executed_as = cur.fetchone()[0]
    return {"caller_header": caller_of(request), "has_forwarded_token": bool(token), "auth_mode": mode,
            "executed_as": executed_as}


@app.get("/api/inventory")
def inventory():
    return {"actions": {n: {k: a[k] for k in ("description", "object_table", "id_column", "property", "procedure")}
                        for n, a in ACTIONS.items()},
            "logics": {n: {"description": g.get("description"), "function": g["llm"]["function_name"],
                           "inputs": g["object"]["input_properties"], "action": g["action"]["name"]}
                       for n, g in LOGICS.items()}}


@app.get("/api/health")
def health():
    return {"ok": True, "actions": list(ACTIONS), "logics": list(LOGICS)}


# ------------------------------------------------------------------------------------------------ on-demand page
CSS = """
:root{--ink:#1b2430;--muted:#5f6b7a;--line:#e3e7ec;--bg:#f5f7f9;--card:#fff;--brand:#ff3621;--brand-dk:#d92b17;
--ok:#1f8a4c;--ok-bg:#e6f4ec;--warn:#9a6b00;--warn-bg:#fdf3dc;--err:#b42318;--err-bg:#fdecea;--sp:#3b5bdb;--sp-bg:#e8edff}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 -apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
color:var(--ink);background:var(--bg)}
header{background:#1b3139;color:#fff;padding:14px 32px;display:flex;align-items:center;justify-content:space-between}
header .t{font-size:17px;font-weight:600}header .t span{color:#ff8a7a;font-weight:500}
header .u{font-size:13px;opacity:.9}main{max-width:1100px;margin:24px auto;padding:0 24px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:20px 22px;margin-bottom:18px;
box-shadow:0 1px 2px rgba(16,24,40,.04)}h2{font-size:15px;margin:0 0 12px}h2 .sub{color:var(--muted);font-weight:400}
.flow{display:flex;gap:8px;align-items:stretch;flex-wrap:wrap}
.step{flex:1;min-width:170px;border:1px solid var(--line);border-radius:8px;padding:10px 12px;background:#fafbfc}
.step b{display:block;font-size:13px}.step small{color:var(--muted)}.step code{font-size:12px}
.arrow{align-self:center;color:var(--muted);font-size:18px}
form.run{display:flex;gap:12px;align-items:flex-end;flex-wrap:wrap}label{font-size:12px;color:var(--muted);display:block;margin-bottom:4px}
select{padding:8px 10px;border:1px solid #cfd6de;border-radius:6px;font-size:14px;background:#fff;min-width:220px}
select.obj{min-width:380px}
button{background:var(--brand);color:#fff;border:0;border-radius:6px;padding:9px 20px;font-size:14px;font-weight:600;cursor:pointer}
button:hover{background:var(--brand-dk)}button:disabled{opacity:.6;cursor:wait}
table{width:100%;border-collapse:collapse}th{text-align:left;font-size:12px;color:var(--muted);font-weight:600;
border-bottom:1px solid var(--line);padding:8px}td{border-bottom:1px solid var(--line);padding:9px 8px;vertical-align:top}
td.id{white-space:nowrap;font-weight:600}td.id div{font-weight:400;color:var(--muted);font-size:12px}
td.sum{color:#334155;max-width:560px}td.when{white-space:nowrap;color:var(--muted);font-size:12px}
.pill{display:inline-block;padding:1px 9px;border-radius:999px;font-size:12px;font-weight:600;white-space:nowrap}
.ok{background:var(--ok-bg);color:var(--ok)}.warn{background:var(--warn-bg);color:var(--warn)}.err{background:var(--err-bg);color:var(--err)}
.sp{background:var(--sp-bg);color:var(--sp)}.user{background:#f1f3f5;color:#343a40}
.muted{color:var(--muted)}.alert{border-radius:8px;padding:10px 14px;margin-bottom:14px}p.note{margin:12px 0 0}
blockquote{margin:0;padding:14px 16px;background:#fafbfc;border-left:4px solid var(--brand);border-radius:6px;white-space:pre-wrap}
dl{display:grid;grid-template-columns:200px 1fr;gap:8px 16px;margin:0}dt{color:var(--muted)}dd{margin:0;word-break:break-all}
details summary{cursor:pointer;color:var(--muted)}pre{background:#0f172a;color:#e2e8f0;padding:14px;border-radius:8px;
overflow:auto;font-size:12px}a.back{display:inline-block;margin-top:4px;color:var(--brand);font-weight:600;text-decoration:none}
footer{max-width:1100px;margin:0 auto 30px;padding:0 24px;color:var(--muted);font-size:12px}
"""


def page(body, user=""):  # plain concatenation: body may contain braces (JSON)
    e = html.escape
    return ("<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>"
            "<title>AIP Logic on Databricks</title><style>" + CSS + "</style></head><body>"
            "<header><div class='t'>AIP Logic on Databricks <span>&middot; Actions API demo</span></div>"
            "<div class='u'>" + ("Signed in as <b>" + e(user) + "</b>" if user else "") + "</div></header>"
            "<main>" + body + "</main><footer>Placeholder Actions API on Databricks Apps &middot; schema <code>"
            + e(CAT) + "." + e(SCH) + "</code> &middot; JSON API: <code>POST /api/actions/&lt;action&gt;</code>, "
            "<code>POST /api/logic/&lt;logic&gt;/run</code>, <code>GET /api/inventory</code></footer></body></html>")


def who(v):
    """Identity badge: users by email; service principals (UUID application ids) shortened."""
    if not v:
        return "<span class='muted'>-</span>"
    v = str(v)
    if "@" in v:
        return "<span class='pill user'>" + html.escape(v) + "</span>"
    return "<span class='pill sp' title='" + html.escape(v) + "'>service principal " + html.escape(v[:8]) + "</span>"


def status_pill(s):
    cls = {"applied": "ok", "not_found": "warn"}.get(str(s), "err")
    return "<span class='pill " + cls + "'>" + html.escape(str(s)) + "</span>"


def value_cell(v, n=260):
    if not v:
        return "<span class='pill warn'>no value yet</span>"
    v = str(v)
    return html.escape(v[:n]) + ("&hellip;" if len(v) > n else "")


def recent_actions(limit=8):
    """Latest audit rows, read with the app's own identity (it owns the audit trail). Empty on any error."""
    try:
        conn, _ = connect(None)
        with conn as c, c.cursor() as cur:
            cur.execute(f"SELECT at, action, object_id, executed_as, triggered_by, status, run_id FROM {AUDIT} "
                        f"ORDER BY at DESC LIMIT {int(limit)}")
            return cur.fetchall()
    except Exception:
        log.exception("could not read recent actions")
        return []


@app.get("/", response_class=HTMLResponse)
def home(request: Request, logic: str = ""):
    e = html.escape
    name = logic if logic in LOGICS else next(iter(LOGICS))
    cfg = LOGICS[name]
    o, prop = cfg["object"], cfg["object"]["target_property"]
    try:
        conn, mode = connect(token_of(request))
        with conn as c, c.cursor() as cur:
            cur.execute(f"SELECT {L.ident(o['id_column'])}, {L.ident(o['input_properties'][0])}, {L.ident(prop)}, "
                        f"{L.ident(prop + '_updated_by')}, {L.ident(prop + '_updated_at')} FROM {L.fq(CAT, SCH, o['table'])} ORDER BY 1")
            rows = cur.fetchall()
        err = ""
    except Exception as ex:
        rows, err = [], f"<div class='alert err'>Could not read objects as you: {e(str(ex)[:300])}</div>"
    act = ACTIONS[cfg["action"]["name"]]
    logic_opts = "".join(f"<option value='{e(n)}' {'selected' if n == name else ''}>{e(n)}</option>" for n in LOGICS)
    opts = "".join(f"<option value='{e(r[0])}'>{e(r[0])} &middot; {e(str(r[1])[:60])}</option>" for r in rows)
    done = sum(1 for r in rows if r[2])
    table = "".join(
        f"<tr><td class='id'>{e(r[0])}<div>{e(str(r[1])[:48])}</div></td><td class='sum'>{value_cell(r[2])}</td>"
        f"<td>{who(r[3])}</td><td class='when'>{e(str(r[4] or ''))[:16]}</td></tr>" for r in rows)
    recent = recent_actions()
    recent_html = "".join(
        f"<tr><td class='when'>{e(str(a[0]))[:19]}</td><td>{e(str(a[1]))}</td><td class='id'>{e(str(a[2] or '-'))}</td>"
        f"<td>{who(a[3])}</td><td>{status_pill(a[5])}</td><td class='when'>{e(str(a[6] or ''))[:28]}</td></tr>" for a in recent)
    spinner = "var b=this.querySelector('button');b.disabled=true;b.textContent='Running...';"
    body = (
        f"{err}<div class='card'><h2>How it works</h2><div class='flow'>"
        f"<div class='step'><b>1 &middot; Object</b><small>UC table row</small><br><code>{e(o['table'])}</code></div><div class='arrow'>&rarr;</div>"
        f"<div class='step'><b>2 &middot; Logic (LLM)</b><small>UC function, runs as you</small><br><code>{e(cfg['llm']['function_name'])}</code></div><div class='arrow'>&rarr;</div>"
        f"<div class='step'><b>3 &middot; Action</b><small>Actions API &rarr; UC procedure</small><br><code>{e(cfg['action']['name'])}</code></div><div class='arrow'>&rarr;</div>"
        f"<div class='step'><b>4 &middot; Updated + audited</b><small>property written</small><br><code>{e(act['property'])}</code></div>"
        f"</div></div>"
        f"<div class='card'><h2>Run a Logic on demand</h2><form method='get' action='/' id='lf'></form>"
        f"<form class='run' method='post' action='/run' onsubmit=\"{spinner}\">"
        f"<div><label>Logic</label><select name='logic' form='lf' onchange='this.form.submit()'>{logic_opts}</select></div>"
        f"<input type='hidden' name='logic_name' value='{e(name)}'>"
        f"<div><label>Object</label><select class='obj' name='object_id'>{opts}</select></div>"
        f"<div><button type='submit'>Run Logic</button></div></form>"
        f"<p class='muted note'>The LLM call and the edit both execute with your identity; the app writes the audit row.</p></div>"
        f"<div class='card'><h2>{e(o['table'])} <span class='sub'>&middot; {done}/{len(rows)} with {e(prop)}</span></h2>"
        f"<table><tr><th>Object</th><th>{e(prop)}</th><th>Updated by</th><th>Updated at</th></tr>{table}</table></div>")
    if recent:
        body += (f"<div class='card'><h2>Recent actions <span class='sub'>&middot; audit trail</span></h2><table><tr><th>When</th>"
                 f"<th>Action</th><th>Object</th><th>Executed as</th><th>Status</th><th>Run</th></tr>{recent_html}</table></div>")
    return page(body, caller_of(request))


@app.post("/run", response_class=HTMLResponse)
def run_form(request: Request, logic_name: str = Form(...), object_id: str = Form(...)):
    http, out = run_logic(logic_name, object_id, token_of(request), caller_of(request))
    e = html.escape
    ar = out.get("action_result") or {}
    value = out.get("proposed_value")
    error = out.get("error") or ar.get("error")
    facts = [
        ("Object", e(object_id)), ("Logic", e(logic_name)),
        ("LLM step executed as", who(out.get("llm_step_executed_as"))),
        ("Edit executed as", who(ar.get("executed_as"))),
        ("Action", e(str(ar.get("action") or "-"))),
        ("Target", e(f"{ar.get('object_table') or '-'} . {ar.get('property') or '-'}")),
        ("Auth mode", e(str(ar.get("auth_mode") or out.get("llm_auth_mode") or "-"))),
        ("Audit written", "<span class='pill ok'>yes</span>" if ar.get("audit_written") else "<span class='pill err'>no</span>"),
        ("Run id", e(str(ar.get("run_id") or "-"))), ("Request id", e(str(ar.get("request_id") or "-")))]
    body = f"<div class='card'><h2>{e(object_id)} &nbsp;{status_pill(out.get('status'))}</h2>"
    if error:
        body += f"<div class='alert err'>{e(str(error))}</div>"
    if value:
        body += f"<label>New value</label><blockquote>{e(str(value))}</blockquote>"
    body += ("</div><div class='card'><h2>Who did what</h2><dl>" + "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in facts)
             + f"</dl></div><div class='card'><details><summary>Raw API response</summary><pre>{e(json.dumps(out, indent=2))}</pre>"
             f"</details></div><a class='back' href='/?logic={e(logic_name)}'>&larr; Back to objects</a>")
    return HTMLResponse(page(body, caller_of(request)), status_code=http)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("DATABRICKS_APP_PORT", "8000")))
