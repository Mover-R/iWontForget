"""Application configuration via Pydantic Settings.

All settings are read from environment variables.
Nested settings use double-underscore delimiter:
  TELEGRAM__TOKEN=xxx
  REDIS__ADDR=redis:6379
  SERVICES__USER=user-service:50051

Modes:
  TELEGRAM__MODE=longpoll   → local development (no webhook needed)
  TELEGRAM__MODE=webhook    → production (requires public HTTPS URL)
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class TelegramSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TELEGRAM__")

    token: str = Field(description="Telegram Bot API token from @BotFather")
    mode: str = Field(
        default="longpoll",
        description="'longpoll' for local dev, 'webhook' for production",
    )
    # Webhook settings (only used when mode=webhook)
    webhook_url: str = Field(
        default="",
        description="Public HTTPS URL Telegram will POST updates to (e.g. https://bot.example.com)",
    )
    webhook_path: str = Field(
        default="/webhook",
        description="URL path for the webhook endpoint",
    )
    webhook_port: int = Field(
        default=8443,
        description="Port for the webhook server (Telegram supports 443, 80, 88, 8443)",
    )
    webhook_secret_token: str = Field(
        default="",
        description="Optional secret token to validate webhook requests from Telegram",
    )


class GrpcSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="GRPC__")

    port: int = Field(default=50051, description="gRPC server port (for Temporal activities)")
    max_recv_msg_size: int = Field(
        default=4 * 1024 * 1024,
        description="Max gRPC message size in bytes",
    )
    enabled: bool = Field(
        default=False,
        description="Enable gRPC server (Phase 1+). False in Phase 0.",
    )


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS__")

    addr: str = Field(default="redis:6379", description="Redis host:port")
    session_db: int = Field(default=0, description="Redis DB for PTB conversation persistence")
    ratelimit_db: int = Field(default=1, description="Redis DB for rate limiting")
    dedup_db: int = Field(default=2, description="Redis DB for notification dedup")
    password: str = Field(default="", description="Redis password (empty = no auth)")


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA__")

    brokers: list[str] = Field(default=["kafka:9092"])
    topic_user_activity: str = Field(default="user.activity")
    enabled: bool = Field(
        default=False,
        description="Enable Kafka producer (Phase 1+). False in Phase 0.",
    )


class ServiceEndpoints(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVICES__")

    user: str = Field(default="user-service:50051")
    wishlist: str = Field(default="wishlist-service:50051")
    reminder: str = Field(default="reminder-service:50051")
    friends: str = Field(default="friends-service:50051")
    gift: str = Field(default="gift-service:50051")
    event: str = Field(default="event-service:50051")
    ai: str = Field(default="ai-service:50051")


class RateLimitSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="RATELIMIT__")

    global_per_second: int = Field(default=30)
    per_chat_per_second: int = Field(default=1)


class Settings(BaseSettings):
    """Root settings — reads from environment with __ nesting."""

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    grpc: GrpcSettings = Field(default_factory=GrpcSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    services: ServiceEndpoints = Field(default_factory=ServiceEndpoints)
    ratelimit: RateLimitSettings = Field(default_factory=RateLimitSettings)

    log_level: str = Field(default="info")
    log_format: str = Field(
        default="console",
        description="'console' for dev, 'json' for prod",
    )
    environment: str = Field(default="development")
