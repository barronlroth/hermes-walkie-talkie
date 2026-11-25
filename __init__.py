"""walkie-talkie: message another existing Hermes gateway session.

Delivery uses the authenticated local gateway owner's FIFO. Admission is not execution:
the sender does not wait for a receiving turn or its reply.

Guards (deliberately few):
- label: every handoff starts with "📻 from <sender>" so it is visible in the target chat
- one hop: a turn that itself arrived over the walkie-talkie cannot transmit again (no loops)
- repeats: an identical message to the same target within 10 minutes is dropped
- burst cap: at most MAX_PER_WINDOW handoffs per target per 10 minutes
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import sys
import threading
import time
from contextlib import closing
from pathlib import Path
from typing import Any, TypeGuard
from uuid import uuid4

logger = logging.getLogger(__name__)

LABEL = "📻 from"
WINDOW_S = 600
MAX_PER_WINDOW = 5
PLUGIN = "walkie-talkie"
_SUBMIT_REFUSALS = frozenset({"permission_denied", "not_found", "invalid_params",
                              "runtime_draining", "session_busy", "unknown_method"})

_ctx = None
_lock = threading.Lock()
_recent: dict[str, list[tuple[float, str]]] = {}   # target session_key -> [(ts, digest)]
_injected_turn: dict[str, bool] = {}                # session_id -> current turn arrived via radio


# ---------------------------------------------------------------- helpers

def _home() -> Path:
    try:
        from hermes_constants import get_hermes_home
        return Path(get_hermes_home())
    except Exception:
        return Path.home() / ".hermes"


def _session_env(name: str) -> str:
    try:
        from gateway.session_context import get_session_env
        return get_session_env(name, "") or ""
    except Exception:
        return ""


def _topic_names() -> dict[tuple[str, str], str]:
    """(chat_id, thread_id) -> configured topic name, from platforms.telegram.extra."""
    out: dict[tuple[str, str], str] = {}
    try:
        from hermes_cli.config import load_config_readonly
        cfg = load_config_readonly() or {}
        extra = (((cfg.get("platforms") or {}).get("telegram") or {}).get("extra") or {})
        for group in (extra.get("dm_topics") or []) + (extra.get("group_topics") or []):
            for t in group.get("topics") or []:
                out[(str(group.get("chat_id")), str(t.get("thread_id")))] = str(t.get("name"))
    except Exception:
        logger.debug("walkie-talkie: topic names unavailable", exc_info=True)
    return out


def _default_home() -> Path | None:
    from hermes_constants import get_default_hermes_root, get_hermes_home, profile_name_for_home

    caller = get_hermes_home()
    home = get_default_hermes_root(home="").resolve()
    if (_ctx is not None and caller.resolve() == home and profile_name_for_home(caller) == "default"
            and _ctx._manager.home_path.resolve() == home
            and profile_name_for_home(_ctx._manager.owner_home) == "default"):
        return home
    return None


def _scope(home: Path) -> str:
    return str((home / "sessions").resolve())


def _routes(home: Path | None = None, *, scope: str | None = None,
            key: str | None = None) -> list[dict[str, Any]]:
    """Read one current owner scope, never an unscoped/history fallback."""
    from gateway.config import Platform

    home = home if home is not None else _default_home()
    if home is None:
        return []
    names = _topic_names() if key is None else {}
    sql = ("select r.session_key, r.entry_json, s.title from gateway_routing r "
           "left join sessions s on s.id = json_extract("
           "case when json_valid(r.entry_json) then r.entry_json end, '$.session_id') "
           "where r.scope = ? and r.session_key glob 'agent:main:*'")
    params = (scope if scope is not None else _scope(home),)
    if key is not None:
        sql += " and r.session_key = ?"
        params += (key,)
    rows = []
    try:
        uri = (home / "state.db").as_uri() + "?mode=ro"
        with closing(sqlite3.connect(uri, uri=True, timeout=2)) as con:
            for route_key, entry_json, title in con.execute(sql, params):
                try:
                    entry = json.loads(entry_json)
                    parts = route_key.split(":")
                    platform = Platform(parts[2])
                except (ValueError, TypeError, IndexError):
                    continue
                if (not isinstance(entry, dict) or len(parts) < 5 or not parts[4]
                        or platform in (Platform.LOCAL, Platform.API_SERVER)
                        or entry.get("platform") not in (None, platform.value)
                        or not _nonempty(entry.get("session_id"))):
                    continue
                thread = parts[5] if len(parts) > 5 else ""
                rows.append({"session_key": route_key, "session_id": entry["session_id"],
                             "name": names.get((parts[4], thread)) or "", "title": title or "",
                             "platform": platform.value, "chat_type": entry.get("chat_type") or "",
                             "updated_at": entry.get("updated_at") or ""})
    except sqlite3.Error:
        logger.warning("walkie-talkie: cannot read current gateway routes")
        return []
    return sorted(rows, key=lambda row: str(row["updated_at"]), reverse=True)


def _nonempty(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _resolve(target: str, home: Path | None = None, *, scope: str | None = None,
             exact_key: bool = False) -> tuple[dict | None, list[dict]]:
    t = target.strip()
    if not t:
        return None, []
    routes = _routes(home, scope=scope, key=t if exact_key or t.startswith("agent:") else None)
    matches = [r for r in routes if t == r["session_key"] or (not exact_key and t == r["session_id"])]
    if not matches and not (exact_key or t.startswith("agent:")):
        folded = t.casefold()
        matches = [r for r in routes if folded in (r["name"].casefold(), r["title"].casefold())]
        if not matches:
            matches = [r for r in routes if folded in r["name"].casefold() or folded in r["title"].casefold()]
    return (matches[0], []) if len(matches) == 1 else (None, matches[:8])


def _cron_sender(session_id: str) -> tuple[str, str] | None:
    """For a cron run (session_id cron_<jobid>_<ts>): ('<Home topic> · <job name>', home session_id)."""
    sid = session_id or ""
    if not sid.startswith("cron_"):
        return None
    job_id = sid.split("_")[1] if len(sid.split("_")) > 1 else ""
    job: dict = {}
    try:
        data = json.loads((_home() / "cron" / "jobs.json").read_text())
        jobs = data.get("jobs", []) if isinstance(data, dict) else data
        job = next((j for j in jobs if j.get("id") == job_id), {})
    except Exception:
        logger.debug("walkie-talkie: cannot read cron jobs", exc_info=True)
    name = job.get("name") or (job_id[:6] if job_id else "cron")
    chat = thread = ""
    deliver = str(job.get("deliver") or "origin")
    if deliver.startswith("telegram:"):
        parts = deliver.split(":")
        chat = parts[1] if len(parts) > 1 else ""
        thread = parts[2] if len(parts) > 2 else ""
    elif deliver == "origin":
        o = job.get("origin") or {}
        chat, thread = str(o.get("chat_id") or ""), str(o.get("thread_id") or "")
    home_name, home_sid = "", ""
    if chat:
        home_name = _topic_names().get((chat, thread), "")
        suffix = f":{chat}:{thread}" if thread else f":{chat}"
        for r in _routes():
            if r["session_key"].endswith(suffix):
                home_name = home_name or r["name"] or r["title"]
                home_sid = r["session_id"]
                break
    label = f"{home_name} · {name}" if home_name else f"cron · {name}"
    return label, home_sid


def _sender_label(session_id: str) -> str:
    key = _session_env("HERMES_SESSION_KEY")
    if key:
        for r in _routes():
            if r["session_key"] == key:
                return r["name"] or r["title"] or key
        return key
    if _session_env("HERMES_SESSION_SOURCE") == "cron" or "cron" in (session_id or ""):
        return f"cron ({session_id})" if session_id else "cron"
    return session_id or "another session"


def _allowed() -> bool:
    from hermes_cli.config import load_config_readonly

    try:
        plugins = load_config_readonly().get("plugins", {})
        enabled, disabled = plugins.get("enabled"), plugins.get("disabled", [])
        return (isinstance(enabled, list) and PLUGIN in enabled
                and isinstance(disabled, list) and PLUGIN not in disabled
                and plugins["entries"][PLUGIN].get("allow_gateway_injection") is True)
    except Exception:
        return False


def _refused(error: str) -> dict:
    return {"status": "refused", "error": error}


def _unknown() -> dict:
    return {"status": "unknown", "error": "Delivery unknown after submission; do not resend."}


def _admitted(receipt: Any, profile_id: str, session_id: str) -> bool:
    if not isinstance(receipt, dict):
        return False
    ref = receipt.get("ref")
    return (isinstance(ref, dict) and ref.get("profile_id") == profile_id
            and ref.get("session_id") == session_id and _nonempty(receipt.get("admission_id"))
            and type(receipt.get("sequence")) is int and receipt["sequence"] > 0
            and type(receipt.get("authority_epoch")) is int and receipt["authority_epoch"] > 0
            and receipt.get("status") in ("queued", "started")
            and "outcome" in receipt and receipt["outcome"] is None)


async def _submit_topic(home: Path, scope: str, route: dict, content: str, sender_id: str) -> dict:
    from hermes_cli.gateway_client import GatewayRPCError, connect_gateway
    from hermes_cli.gateway_runtime import discover_gateway_endpoint

    result = None
    attempted = False
    try:
        async with asyncio.timeout(35):
            found = await asyncio.to_thread(discover_gateway_endpoint, home, timeout=2.0)
            if found.state != "ready" or found.endpoint is None:
                return {"status": "unavailable", "error": f"Local gateway owner not ready ({found.state})."}
            if found.endpoint.profile_id != str(home):
                return _refused("Local gateway owner is not the default profile.")
            async with connect_gateway(found.endpoint) as client:
                try:
                    resumed = await client.rpc("session.resume", session_id=route["session_id"],
                                               omit_messages=True, _timeout=8.0)
                    canonical = resumed.get("session_id") if isinstance(resumed, dict) else None
                    current = await asyncio.to_thread(_routes, home, scope=scope, key=route["session_key"])
                    if not _nonempty(canonical):
                        result = _refused("Local gateway did not return a canonical session ID.")
                    elif canonical == sender_id:
                        result = _refused("that is this session")
                    elif _scope(home) != scope or len(current) != 1 or current[0]["session_id"] != route["session_id"]:
                        result = _refused("Target route changed or became unavailable before submission.")
                    else:
                        submission_id = str(uuid4())
                        attempted = True
                        receipt = await client.rpc("prompt.submit", session_id=canonical, text=content,
                                                   submission_id=submission_id, queued=True, _timeout=8.0)
                        if _admitted(receipt, str(home), canonical):
                            result = {"status": "scheduled", "to": route["name"] or route["title"],
                                      "session_id": canonical, "admission_id": receipt["admission_id"],
                                      "shown_in_chat": False,
                                      "note": "Accepted by the gateway FIFO; receiving turn and reply are not verified."}
                        else:
                            result = _unknown()
                except GatewayRPCError as exc:
                    reason = exc.data.get("reason", str(exc))
                    if exc.code in (4001, -32601) and reason in _SUBMIT_REFUSALS:
                        result = _refused(f"Local gateway refused the handoff ({reason}).")
                    elif attempted:
                        result = _unknown()
                    else:
                        result = _refused("Local gateway did not resume the target; nothing was submitted.")
    except Exception:
        # In particular, connection teardown must not overwrite a known receipt/refusal.
        if result is None:
            result = _unknown() if attempted else {
                "status": "unavailable", "error": "Could not attach to the local gateway; nothing was submitted."}
    return result


# ---------------------------------------------------------------- tools

def _list_topics(args: dict, **_: Any) -> str:
    rows = [{k: r[k] for k in ("name", "title", "session_id", "chat_type")} for r in _routes()]
    limit = int(args.get("limit") or 40)
    return json.dumps({"topics": rows[:limit], "total": len(rows)})


async def _schedule_topic(target: str, message: str, session_id: str = "", *, cli: bool = False) -> dict:
    message = message.strip()
    if not message:
        return _refused("message is empty")
    if _injected_turn.get(session_id):
        return _refused("one hop only: this turn arrived over the walkie-talkie, so it cannot transmit again")
    home = _default_home()
    if home is None or not _allowed():
        return _refused("Enable the default-profile walkie-talkie and set allow_gateway_injection: true.")
    scope = _scope(home)
    route, candidates = await asyncio.to_thread(_resolve, target, home, scope=scope, exact_key=cli)
    if route is None:
        return {**_refused(f"no unique current session matches {target!r}"),
                "candidates": [{k: c[k] for k in ("name", "title", "session_id")} for c in candidates],
                "hint": "pass the exact current route key" if cli else "call list_topics, then pass a session_id"}
    key = route["session_key"]
    sender_id = session_id or _session_env("HERMES_SESSION_ID")
    if key == _session_env("HERMES_SESSION_KEY") or route["session_id"] == sender_id:
        return _refused("that is this session")

    cron = None if cli else await asyncio.to_thread(_cron_sender, session_id)
    if cli:
        sender, reply_to = "scheduler CLI", ""
    elif cron:
        sender, reply_to = cron
    else:
        sender = await asyncio.to_thread(_sender_label, session_id)
        reply_to = sender_id
    content = f"{LABEL} {sender}" + (f" · reply: {reply_to}" if reply_to else "") + f"\n\n{message}"
    digest = hashlib.sha256(message.encode()).hexdigest()[:16]
    now = time.time()
    reservation = (now, digest)
    with _lock:
        hist = [(ts, d) for ts, d in _recent.get(key, []) if now - ts < WINDOW_S]
        if any(d == digest for _, d in hist):
            return {"status": "dropped", "reason": "identical handoff attempted for this session in the last 10 minutes"}
        if len(hist) >= MAX_PER_WINDOW:
            return _refused(f"burst cap: {MAX_PER_WINDOW} handoffs to this session in 10 minutes already")
        hist.append(reservation)
        _recent[key] = hist

    result = await _submit_topic(home, scope, route, content, sender_id)
    if result["status"] in ("refused", "unavailable"):
        with _lock:
            _recent[key] = [h for h in _recent.get(key, []) if h is not reservation]
    return result


async def _message_topic(args: dict, session_id: str = "", **_: Any) -> str:
    return json.dumps(await _schedule_topic(str(args.get("target") or ""), str(args.get("message") or ""), session_id))


def _cli_setup(parser) -> None:
    parser.add_argument("--target", required=True, help="Exact current route key")
    parser.add_argument("--message-file", required=True, choices=("-",), help="Read message from stdin")


def _cli_send(args) -> None:
    result = asyncio.run(_schedule_topic(args.target, sys.stdin.read(), cli=True))
    print(json.dumps(result))
    if result["status"] != "scheduled":
        raise SystemExit(1)


# ---------------------------------------------------------------- hook

def _on_pre_llm_call(session_id: str = "", user_message: Any = None, **_: Any) -> None:
    text = user_message if isinstance(user_message, str) else ""
    _injected_turn[session_id] = text.lstrip().startswith(LABEL)
    return None


LIST_SCHEMA = {
    "name": "list_topics",
    "description": "List existing chat sessions (Telegram topics, group threads, DMs) you can message with message_topic. Returns topic name, auto title, session_id and chat type, most recent first.",
    "parameters": {"type": "object", "properties": {"limit": {"type": "integer", "description": "Max rows (default 40)"}}},
}

SEND_SCHEMA = {
    "name": "message_topic",
    "description": (
        "Send a message to another existing session (by topic name like 'Cubby', or a pasted session_id). "
        "Scheduled means the target's gateway accepted the message in FIFO order, not that it ran. "
        "You do not get the reply here. If delivery is unknown, do not resend."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "target": {"type": "string", "description": "Topic name, session title fragment, or session_id"},
            "message": {"type": "string", "description": "What the other session should do or know"},
        },
        "required": ["target", "message"],
    },
}


def register(ctx) -> None:
    global _ctx
    _ctx = ctx
    ctx.register_tool("list_topics", "walkie_talkie", LIST_SCHEMA, _list_topics, emoji="📻",
                      description=LIST_SCHEMA["description"])
    ctx.register_tool("message_topic", "walkie_talkie", SEND_SCHEMA, _message_topic, emoji="📻",
                      description=SEND_SCHEMA["description"], is_async=True)
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
    ctx.register_cli_command(PLUGIN, "Queue a message in a default-profile topic", _cli_setup, _cli_send)
