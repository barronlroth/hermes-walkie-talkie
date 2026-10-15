# walkie-talkie 📻

Let one Hermes session message another **existing** session. A Telegram topic, a group thread or a DM wakes up with its full history, handles the message as a new turn, and replies **in its own chat**.

It works like this: from your coordinator topic you say "tell Cubby to check the build", and the Cubby topic picks it up and answers in Cubby.

## How it works

- `list_topics`: lists the sessions you can reach, with their topic names, titles and session IDs.
- `message_topic(target, message)`: target a topic name (`Cubby`) or paste a session ID.
- Delivery goes through the gateway's `ctx.inject_message()`, the same path a voice note takes. No second process, so two writers never touch one transcript.
- Fire-and-forget: the sender doesn't wait. If it needs the result, it reads the target's history later.

## What you see

In the target chat, two bot messages appear back to back:

1. `📻 Sent from Hermes Ops session:` followed by the message, so humans can see what was asked.
2. The target session's reply, from its normal turn.

## Guards

- **Label:** the injected turn starts with `📻 from <sender> · reply: <session id>`, so the target knows who sent it and where to answer.
- **One hop:** a turn that arrived over the walkie-talkie can't send another, so there are no loops.
- **Repeats:** an identical message to the same target within 10 minutes is dropped.
- **Burst cap:** at most 5 handoffs per target per 10 minutes.

## Install

```bash
hermes plugins install barronlroth/hermes-walkie-talkie
```

```yaml
# config.yaml
plugins:
  entries:
    walkie-talkie:
      allow_gateway_injection: true
```

Then `/restart` the gateway.

## Disclosure

- **Starts agent turns in other chats** via `ctx.inject_message`. Any session that can call the tool (including cron jobs) can wake any session the gateway routes to, including group chats. The target turn runs with the target session's own tools and permissions. Nothing happens until you set `allow_gateway_injection: true`.
- **Posts a visible message** in the target chat through Hermes's built-in send path (`tools.send_message_tool`), using your existing gateway credentials.
- **Reads outside its own data, read-only:** `state.db` (the `gateway_routing` and `sessions` tables, to list and resolve sessions) and `config.yaml` (Telegram topic names).
- No network calls of its own, no shell commands, no background processes, no stored credentials, no telemetry.
