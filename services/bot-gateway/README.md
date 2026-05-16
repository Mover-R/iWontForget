# Bot Gateway

Telegram bot entry point for the **iWontForget** application.

Built with **python-telegram-bot v21** (PTB), it receives Telegram updates, routes them to Go backend services via gRPC, and delivers notifications from Temporal workflows.

---

## Architecture

```
Telegram API ←→ Bot Gateway (Python/python-telegram-bot)
                    ├── gRPC clients → Go backend services
                    ├── gRPC server  ← Temporal notification activities (Phase 1+)
                    ├── Redis        (conversation persistence, rate limiting, dedup)
                    └── Kafka        → user.activity events (Phase 1+)
```

### Modes

| Mode | How to run | When to use |
|---|---|---|
| **Long-polling** (`TELEGRAM__MODE=longpoll`) | `python -m bot_gateway` | Local development — zero config, no tunnel needed |
| **Webhook** (`TELEGRAM__MODE=webhook`) | Set `TELEGRAM__WEBHOOK_URL` + run | Production — Telegram POSTs updates to your server |

### Directory Structure

```
bot_gateway/
├── __main__.py              # Entry point: run_polling or run_webhook
├── app.py                   # Application factory + DI container
├── config.py                # Pydantic Settings (env vars with __ nesting)
├── handlers/                # Catch-all handlers (registered last)
│   ├── commands.py          # Unknown commands → show help
│   ├── messages.py          # Free-text → AI classification (Phase 4)
│   ├── callbacks.py         # Unknown callbacks → dismiss
│   └── deeplink.py          # Deep link handler (Phase 3)
├── routers/                 # Domain handlers (registered first)
│   ├── common.py            # /start, /help, /menu, /cancel, /settings ✅
│   ├── wish.py              # /wish, /wishes (Phase 1 stub)
│   ├── reminder.py          # /remind, /reminders, /today (Phase 1 stub)
│   └── friend.py            # /friend, /friends, /birthdays (Phase 2 stub)
├── states/                  # Conversation state integer constants
│   ├── wish.py              # WishStates: NAME=10, CATEGORY=11, PRICE=12, CONFIRM=13
│   ├── reminder.py          # ReminderStates: TEXT=20, TIME=21, RECURRENCE=22, CONFIRM=23
│   └── friend.py            # FriendStates: USERNAME=30, CONFIRM=31
├── keyboards/               # Inline keyboard builders (pure functions)
│   ├── main_menu.py         # 5-button main menu
│   ├── wish.py              # Category picker, wish actions
│   ├── reminder.py          # Snooze/done/delete, recurrence
│   └── common.py            # Confirm/cancel, skip, back
├── grpc_server/             # gRPC server (Temporal → Bot Gateway, Phase 1+)
│   ├── server.py
│   └── notification_service.py
├── grpc_clients/            # gRPC clients (Bot Gateway → Go services)
│   ├── base.py              # Base async channel manager
│   ├── user.py              # User Service ✅ (mock stub until proto codegen)
│   └── ...                  # Others: Phase 1+ stubs
├── services/                # Framework-agnostic business logic
│   ├── ratelimit.py         # Redis token bucket (Lua script)
│   ├── dedup.py             # Redis SET NX deduplication
│   ├── notification.py      # Telegram message sender (PTB Bot wrapper)
│   └── formatter.py         # HTML message formatting helpers
├── middleware/              # PTB TypeHandler-based cross-cutting concerns
│   ├── logging.py           # structlog request logging (TypeHandler, group=-1)
│   ├── metrics.py           # Prometheus counters/histograms (TypeHandler, group=-1)
│   ├── user_context.py      # Load user from User Service (TypeHandler, group=-1)
│   └── error_handler.py     # Global error handler (app.add_error_handler)
├── kafka/
│   └── producer.py          # aiokafka async producer (Phase 1+)
└── utils/
    └── callback_data.py     # Callback data string builders + pattern constants
```

---

## Quick Start (Local Development)

### Prerequisites

- Python 3.12+
- A Telegram bot token from [@BotFather](https://t.me/BotFather)
- Redis (optional — falls back to `PicklePersistence` if unavailable)

### Setup

```bash
cd services/bot-gateway

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"

# Configure environment
cp .env.example .env
# Edit .env — set TELEGRAM__TOKEN at minimum
```

### Run (long-polling — no webhook needed)

```bash
python -m bot_gateway
```

The bot starts immediately. Send `/start` to your bot in Telegram.

### Run Tests

```bash
pytest tests/ -v

# Unit tests only (no Redis/Kafka needed)
pytest tests/unit/ -v

# With coverage
pytest tests/ --cov=bot_gateway --cov-report=term-missing
```

---

## Production (Webhook Mode)

```bash
# Set in .env or environment:
TELEGRAM__MODE=webhook
TELEGRAM__WEBHOOK_URL=https://bot.yourdomain.com
TELEGRAM__WEBHOOK_PORT=8443

python -m bot_gateway
```

PTB starts an HTTPS server on port 8443. Telegram will POST updates to
`https://bot.yourdomain.com/webhook`.

---

## Configuration

All settings use `__` as the nested delimiter.

| Variable | Default | Description |
|---|---|---|
| `TELEGRAM__TOKEN` | **required** | Bot API token from @BotFather |
| `TELEGRAM__MODE` | `longpoll` | `longpoll` for dev, `webhook` for prod |
| `TELEGRAM__WEBHOOK_URL` | `""` | Public HTTPS base URL (webhook mode only) |
| `TELEGRAM__WEBHOOK_PORT` | `8443` | Webhook server port |
| `REDIS__ADDR` | `redis:6379` | Redis host:port |
| `GRPC__ENABLED` | `false` | Enable gRPC server (Phase 1+) |
| `KAFKA__ENABLED` | `false` | Enable Kafka producer (Phase 1+) |
| `LOG_FORMAT` | `console` | `console` for dev, `json` for prod |

See [`.env.example`](.env.example) for the full list.

---

## Conversation State Persistence

PTB's `RedisPersistence` (from `python-telegram-bot[redis]`) stores:
- **Conversation states** — which step of a multi-step dialog the user is in
- **user_data** — per-user context data
- **chat_data** — per-chat data

State survives pod restarts and scales across multiple pods (all pods share Redis).

If Redis is unavailable, falls back to `PicklePersistence` (local file, single-pod only).

---

## Development Phases

| Phase | Bot Gateway work |
|---|---|
| **Phase 0** ✅ | Project scaffold, `/start`, `/help`, `/menu`, `/cancel`, main menu keyboard |
| **Phase 1** | Full `/wish` + `/remind` ConversationHandlers, gRPC calls, notification gRPC server, Kafka |
| **Phase 2** | Friend commands, birthday notification callbacks |
| **Phase 3** | Deep links for wishlist sharing, inline mode |
| **Phase 4** | AI classification for free-text messages |
| **Phase 5** | Group gift callbacks, voting keyboards |
| **Phase 6** | Event commands, RSVP callbacks |

---

## Proto Codegen

Python gRPC stubs are generated during the Docker build.
For local development:

```bash
# From the repo root
python -m grpc_tools.protoc \
    --proto_path=proto \
    --python_out=services/bot-gateway/bot_gateway/proto_gen \
    --grpc_python_out=services/bot-gateway/bot_gateway/proto_gen \
    proto/bot/v1/bot.proto \
    proto/users/v1/users.proto
```

Until stubs are generated, `UserClient` uses a mock stub returning fake data.
