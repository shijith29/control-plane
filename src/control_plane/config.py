"""Configuration, read from environment variables (see README §15 and .env.example)."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore", frozen=True)

    database_url: str = Field(description="postgresql+asyncpg://user:pass@host:5432/db")
    rabbitmq_url: str = Field(description="amqp://user:pass@host:5672/")

    http_port: int = 8080
    db_pool_size: int = 10
    db_max_overflow: int = 20

    outbox_poll_interval_ms: int = 200
    outbox_batch_size: int = 100

    consumer_prefetch: int = 16
    consumer_max_retries: int = 5

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # values come from the environment
