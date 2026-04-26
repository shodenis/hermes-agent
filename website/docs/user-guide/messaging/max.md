---
sidebar_position: 18
title: "MAX Setup"
description: "Configure Hermes Agent with MAX (max.ru / VK Teams), including transport semantics and restart behavior."
---

# MAX Setup

Hermes supports MAX (max.ru / VK Teams) via a polling bot adapter.

## Required variables

Add these to `~/.hermes/.env`:

```bash
MAX_BOT_TOKEN=...
MAX_ALLOWED_USERS=123456789,987654321
MAX_ALLOW_ALL_USERS=false
MAX_HOME_CHANNEL=-1001234567890
MAX_HOME_CHANNEL_NAME=Home
```

Then restart the gateway:

```bash
hermes gateway restart
```

## Transport semantics

The MAX adapter (`gateway/platforms/max.py`) uses a transport-only design:

- Polling: `GET /updates` with `timeout=30`.
- Marker offset: server cursor returned as `marker`; held in process memory (`self._marker`).
- Deduplication: SQLite store at `<HERMES_HOME>/max_dedup.db` with `mid` primary key and 300s TTL.
- Restart behavior: marker resets on process restart, so recent updates can be redelivered; SQLite `mid` dedup absorbs duplicates.
- Backoff: exponential retry from 1s up to 60s on transport failures.
- Self-message guard: inbound messages from the bot's own `user_id` are ignored.
- Outbound chunking: messages are split at 4096 chars before `POST /messages`.

No product-specific auto-acknowledgements or "thinking" filler messages are sent by the adapter.

## Notes

- MAX markdown rendering is limited; plain text is recommended.
- Use `send_message` with `target='max:CHAT_ID'` for explicit cross-chat delivery.
