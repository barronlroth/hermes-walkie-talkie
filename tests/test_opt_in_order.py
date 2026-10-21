"""A refused injection (plugin enabled, allow_gateway_injection not set) must not post anything."""
import importlib.util
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load(monkeypatch):
    calls = []
    fake = types.ModuleType("tools.send_message_tool")

    def send_message_tool(args, **_):
        calls.append(args)
        return json.dumps({"success": True})

    fake.send_message_tool = send_message_tool
    monkeypatch.setitem(sys.modules, "tools", types.ModuleType("tools"))
    monkeypatch.setitem(sys.modules, "tools.send_message_tool", fake)

    spec = importlib.util.spec_from_file_location("walkie_talkie_under_test", ROOT / "__init__.py")
    wt = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(wt)

    route = {"session_key": "agent:main:telegram:dm:111:22", "session_id": "target_sid",
             "name": "Wiki", "title": "Wiki", "platform": "telegram", "chat_type": "dm", "updated_at": ""}
    monkeypatch.setattr(wt, "_resolve", lambda target: (route, []))
    monkeypatch.setattr(wt, "_session_env", lambda name: "")
    monkeypatch.setattr(wt, "_cron_sender", lambda sid: None)
    monkeypatch.setattr(wt, "_sender_label", lambda sid: "Ops")
    wt._recent.clear()
    wt._injected_turn.clear()
    return wt, calls


class Ctx:
    def __init__(self, accept):
        self.accept = accept
        self.injected = []

    def inject_message(self, content, session_key=None, **_):
        self.injected.append((content, session_key))
        return self.accept


def test_refused_injection_makes_no_visible_post(monkeypatch):
    wt, calls = _load(monkeypatch)
    wt._ctx = Ctx(accept=False)
    out = json.loads(wt._message_topic({"target": "Wiki", "message": "hello"}, session_id="sender_sid"))
    assert "error" in out
    assert calls == []                      # nothing posted into the chat
    assert wt._recent.get("agent:main:telegram:dm:111:22", []) == []   # de-dupe rolled back


def test_accepted_injection_posts_visible_copy_after_inject(monkeypatch):
    wt, calls = _load(monkeypatch)
    ctx = Ctx(accept=True)
    wt._ctx = ctx
    out = json.loads(wt._message_topic({"target": "Wiki", "message": "hello"}, session_id="sender_sid"))
    assert out["status"] == "sent" and out["shown_in_chat"] is True
    assert len(ctx.injected) == 1
    assert len(calls) == 1 and calls[0]["target"] == "telegram:111:22"
    assert "📻 Sent from Ops session:" in calls[0]["message"]
