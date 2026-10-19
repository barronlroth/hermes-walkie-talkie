"""walkie-talkie: message another existing Hermes gateway session.

Delivery goes through ``ctx.inject_message(session_key=...)``, so the target session runs a normal
turn inside the gateway (same path as a voice note) and its reply is posted in that chat.
Fire-and-forget: the sender never waits for the answer.

Guards (deliberately few):
- label: every handoff starts with "📻 from <sender>" so it is visible in the target chat
- one hop: a turn that itself arrived over the walkie-talkie cannot transmit again (no loops)
- repeats: an identical message to the same target within 10 minutes is dropped
- burst cap: at most MAX_PER_WINDOW handoffs per target per 10 minutes
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LABEL = "📻 from"
WINDOW_S = 600
MAX_PER_WINDOW = 5

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


def _routes() -> list[dict[str, Any]]:
    """Every live gateway route: session_key, session_id, name, title, platform, chat_type."""
    names = _topic_names()
    rows: list[dict[str, Any]] = []
    db = _home() / "state.db"
    try:
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True, timeout=5)
        for key, entry_json, title in con.execute(
            "select r.session_key, r.entry_json, s.title from gateway_routing r "
            "left join sessions s on s.id = json_extract(r.entry_json, '$.session_id')"
        ):
            e = json.loads(entry_json or "{}")
            parts = key.split(":")          # agent:main:<platform>:<type>:<chat>[:<thread>]
            chat = parts[4] if len(parts) > 4 else ""
            thread = parts[5] if len(parts) > 5 else ""
            rows.append({
                "session_key": key,
                "session_id": e.get("session_id", ""),
                "name": names.get((chat, thread)) or "",
                "title": title or "",
                "platform": e.get("platform") or (parts[2] if len(parts) > 2 else ""),
                "chat_type": e.get("chat_type", ""),
                "updated_at": e.get("updated_at", ""),
            })
        con.close()
    except Exception as exc:
        logger.warning("walkie-talkie: cannot read gateway routes: %s", exc)
    rows.sort(key=lambda r: r["updated_at"], reverse=True)
    return rows


def _resolve(target: str) -> tuple[dict | None, list[dict]]:
    t = (target or "").strip().lower()
    routes = _routes()
    exact = [r for r in routes if t in (r["session_key"].lower(), r["session_id"].lower(), r["name"].lower())]
    if exact:
        return exact[0], []
    fuzzy = [r for r in routes if t and (t in r["name"].lower() or t in r["title"].lower())]
    if len(fuzzy) == 1:
        return fuzzy[0], []
    return None, fuzzy[:8]


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


def _post_visible(session_key: str, text: str) -> bool:
    """Best-effort: post the outbound message into the target chat so humans see it.

    session_key looks like agent:main:<platform>:<type>:<chat>[:<thread>].
    """
    parts = session_key.split(":")
    if len(parts) < 5:
        return False
    target = ":".join([parts[2], parts[4]] + ([parts[5]] if len(parts) > 5 and parts[5] else []))
    if _ctx is None:
        return False
    try:
        res = json.loads(_ctx.dispatch_tool("send_message", {"action": "send", "target": target, "message": text}) or "{}")
        if res.get("error"):
            logger.warning("walkie-talkie: visible post to %s failed: %s", target, res["error"])
            return False
        return True
    except Exception as exc:
        logger.warning("walkie-talkie: visible post to %s failed: %s", target, exc)
        return False


# ---------------------------------------------------------------- tools

def _list_topics(args: dict, **_: Any) -> str:
    rows = [{k: r[k] for k in ("name", "title", "session_id", "chat_type")} for r in _routes()]
    limit = int(args.get("limit") or 40)
    return json.dumps({"topics": rows[:limit], "total": len(rows)})


def _message_topic(args: dict, session_id: str = "", **_: Any) -> str:
    target = str(args.get("target") or "")
    message = str(args.get("message") or "").strip()
    if not message:
        return json.dumps({"error": "message is empty"})
    if _injected_turn.get(session_id):
        return json.dumps({"error": "one hop only: this turn arrived over the walkie-talkie, so it cannot transmit again"})
    if _ctx is None:
        return json.dumps({"error": "walkie-talkie not initialised"})

    route, candidates = _resolve(target)
    if route is None:
        return json.dumps({"error": f"no unique session matches {target!r}",
                           "candidates": [{"name": c["name"], "title": c["title"], "session_id": c["session_id"]} for c in candidates],
                           "hint": "call list_topics, then pass a name or session_id"})
    if route["session_key"] == _session_env("HERMES_SESSION_KEY"):
        return json.dumps({"error": "that is this session"})

    digest = hashlib.sha256(message.encode()).hexdigest()[:16]
    now = time.time()
    with _lock:
        hist = [(ts, d) for ts, d in _recent.get(route["session_key"], []) if now - ts < WINDOW_S]
        if any(d == digest for _, d in hist):
            return json.dumps({"status": "dropped", "reason": "identical message sent to this session in the last 10 minutes"})
        if len(hist) >= MAX_PER_WINDOW:
            return json.dumps({"error": f"burst cap: {MAX_PER_WINDOW} handoffs to this session in 10 minutes already"})
        hist.append((now, digest))
        _recent[route["session_key"]] = hist

    cron = _cron_sender(session_id)
    if cron:
        sender, reply_to = cron          # reply goes to the job's home topic (a cron run ends)
    else:
        sender = _sender_label(session_id)
        reply_to = session_id or _session_env("HERMES_SESSION_ID")
    content = f"{LABEL} {sender}" + (f" · reply: {reply_to}" if reply_to else "") + f"\n\n{message}"
    heading = f"📻 Sent from {sender}:" if cron else f"📻 Sent from {sender} session:"
    shown = _post_visible(route["session_key"], f"{heading}\n\n{message}")
    ok = _ctx.inject_message(content, session_key=route["session_key"])
    if not ok:
        with _lock:
            _recent[route["session_key"]] = [h for h in _recent.get(route["session_key"], []) if h[1] != digest]
        return json.dumps({"error": "gateway refused the handoff (is allow_gateway_injection enabled and the gateway running?)"})
    logger.info("walkie-talkie: %s -> %s (%s)", sender, route["name"] or route["session_key"], route["session_id"])
    return json.dumps({"status": "sent", "to": route["name"] or route["title"], "session_id": route["session_id"],
                       "shown_in_chat": shown,
                       "note": "fire-and-forget; the reply appears in that chat. Read its history later if you need the result."})


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
        "The target wakes up with its full history, handles it as a new turn, and replies in its own chat. "
        "Fire-and-forget: you do not get the reply here. Use it to hand work to the session that owns it."
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
                      description=SEND_SCHEMA["description"])
    ctx.register_hook("pre_llm_call", _on_pre_llm_call)
