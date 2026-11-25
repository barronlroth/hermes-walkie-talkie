# Walkie Talkie 📻

Schedule a one-hop message in another existing Hermes messaging session on the default profile. The receiving session handles it as a new turn and can reply in its own chat or thread, using that session's tools and permissions.

## Agent tools

- `list_topics` lists eligible current sessions with their names, titles and session IDs. It does not list historical or other-profile routes.
- `message_topic(target, message)` accepts a session ID, exact current route key, or a unique topic name or title match. If a name is ambiguous, use `list_topics` and choose the session ID.

## Standalone scheduler or shell

With the plugin enabled on the default profile and its local gateway owner running, provide the exact current route key and pass the message on stdin:

```bash
printf '%s\n' 'Check the build and reply here.' |
  hermes -p default walkie-talkie --target "$CURRENT_ROUTE_KEY" --message-file -
```

The CLI prints one JSON result and exits zero only for `scheduled`. Pass the exact current route key, not a display name or a session ID. The CLI and the agent tool use the same submission path.

## What the result means

- `scheduled`: the authenticated local gateway owner resumed the canonical session and accepted the message into its FIFO queue. This does **not** verify that the receiving turn ran or replied. The sender does not receive the reply.
- `unknown`: the submission was attempted but a matching acceptance could not be confirmed. **Do not resend**, even if the CLI exited nonzero.
- `refused` or `unavailable`: the plugin could not complete the handoff. Check the reported error. `dropped` means a matching handoff was recently attempted in this process.

There is no separate visible echo of the request in the target chat (`shown_in_chat` is false on a scheduling receipt). The target sees the handoff as turn input labelled `📻 from <sender>` and, when known, a reply session ID. Turns received over Walkie Talkie cannot use it to forward again. The plugin also limits repeats and bursts per target in the current process; this is not a durable retry or deduplication service.

## Host compatibility

The FIFO path was tested on a Hermes 0.23.4-based development build. It requires in-process plugins and uses internal gateway-owner APIs, including `ctx._manager`, `hermes_cli.gateway_client`, and `hermes_cli.gateway_runtime`. The manifest's `>=0.23.4` is a lower bound for the gateway modules, not a stock-release compatibility guarantee. Hermes 0.21.7 lacks both modules. Stock-release and cross-version support have not been established.

## Enable on the default profile

The plugin must be installed and explicitly enabled on the default profile. The opt-in setting keeps its existing name even though delivery uses the authenticated owner queue rather than the old gateway injection helper:

```yaml
plugins:
  enabled:
    - walkie-talkie
  entries:
    walkie-talkie:
      allow_gateway_injection: true
```

Add `walkie-talkie` to any existing `plugins.enabled` list; do not replace other entries. The plugin honours `plugins.disabled` and does not auto-start a missing gateway owner.

## Data and permissions

The plugin reads the current route and session indexes in the default profile's `state.db`, Telegram topic labels from read-only config, and cron metadata for agent-tool sender labels. It uses the native authenticated local owner connection to submit one queued prompt. It does not post directly to the chat platform, store its own credentials, or start a separate agent writer.
