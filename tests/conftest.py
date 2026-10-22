"""Shared fixtures.

Tests run against REAL Postgres and RabbitMQ (no mocks for infrastructure):

* TEST_DATABASE_URL   postgresql+asyncpg://...   (schema is dropped & migrated once per run)
* TEST_RABBITMQ_URL   amqp://...                 (queues are purged before each broker test)

``make test`` provides both via compose.test.yaml. When a variable is missing the tests that
need it are skipped — unless REQUIRE_SERVICES=1 (set in CI / make test), where it is an error.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import aio_pika
import httpx
import pytest
from alembic import command
from alembic.config import Config
from asgi_lifespan import LifespanManager
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from control_plane.config import Settings
from control_plane.main import create_app
from control_plane.messaging.topology import ALL_QUEUES, EXCHANGE, RK_TASK_UPDATE, declare_topology
from control_plane.service import TenantService

ROOT = Path(__file__).resolve().parent.parent


def _require(var: str) -> str:
    value = os.environ.get(var)
    if not value:
        if os.environ.get("REQUIRE_SERVICES") == "1":
            pytest.fail(f"{var} must be set (REQUIRE_SERVICES=1)")
        pytest.skip(f"{var} not set")
    return value


# ----------------------------------------------------------------------------- database


@pytest.fixture(scope="session")
def database_url() -> str:
    return _require("TEST_DATABASE_URL")


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Fresh schema for the whole run, created by the real Alembic migrations."""

    async def reset_schema() -> None:
        engine = create_async_engine(database_url)
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
        await engine.dispose()

    asyncio.run(reset_schema())
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.attributes["database_url"] = database_url
    command.upgrade(cfg, "head")
    return database_url


@pytest.fixture(scope="session")
async def engine(migrated_database: str) -> AsyncIterator[AsyncEngine]:
    eng = create_async_engine(migrated_database, pool_size=20, max_overflow=80)
    yield eng
    await eng.dispose()


@pytest.fixture
async def db(engine: AsyncEngine) -> AsyncEngine:
    """Empty tables before each test."""
    async with engine.begin() as conn:
        await conn.execute(
            text("TRUNCATE processed_updates, outbox, tasks, tenants RESTART IDENTITY CASCADE")
        )
    return engine


@pytest.fixture
def service(db: AsyncEngine) -> TenantService:
    return TenantService(db)


async def fetch_all(engine: AsyncEngine, sql: str, **params: Any) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        return [dict(r) for r in (await conn.execute(text(sql), params)).mappings()]


# ----------------------------------------------------------------------------- HTTP


def make_settings(database_url: str, rabbitmq_url: str = "amqp://unused:5672/") -> Settings:
    return Settings(
        database_url=database_url,
        rabbitmq_url=rabbitmq_url,
        outbox_poll_interval_ms=50,
        log_level="WARNING",
    )


@pytest.fixture
async def client(db: AsyncEngine, migrated_database: str) -> AsyncIterator[httpx.AsyncClient]:
    """HTTP client against the app WITHOUT background workers (no broker needed)."""
    app = create_app(make_settings(migrated_database), engine=db, run_background=False)
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


# ----------------------------------------------------------------------------- broker


@pytest.fixture(scope="session")
def rabbitmq_url() -> str:
    return _require("TEST_RABBITMQ_URL")


@pytest.fixture(scope="session")
def rabbitmq_mgmt_url() -> str:
    """e.g. http://user:pass@host:15672 — used to kill connections like a broker restart."""
    return _require("TEST_RABBITMQ_MGMT_URL")


class AmqpHelper:
    def __init__(self, channel: aio_pika.abc.AbstractChannel) -> None:
        self.channel = channel

    async def publish_update(self, body: bytes | str) -> None:
        raw = body.encode() if isinstance(body, str) else body
        exchange = await self.channel.get_exchange(EXCHANGE, ensure=False)
        await exchange.publish(aio_pika.Message(raw), routing_key=RK_TASK_UPDATE)

    async def depth(self, queue: str) -> int:
        declared = await self.channel.declare_queue(queue, passive=True)
        count = declared.declaration_result.message_count
        return int(count or 0)


@pytest.fixture
async def amqp(rabbitmq_url: str) -> AsyncIterator[AmqpHelper]:
    connection = await aio_pika.connect(rabbitmq_url)
    async with connection:
        channel = await connection.channel(publisher_confirms=True)
        queues = await declare_topology(channel)
        for name in ALL_QUEUES:
            await queues[name].purge()
        yield AmqpHelper(channel)


# ----------------------------------------------------------------------------- utils


async def eventually(
    check: Callable[[], Awaitable[bool]], *, within: float = 10.0, interval: float = 0.05
) -> None:
    """Poll until ``check()`` is true or fail after ``timeout`` seconds."""
    deadline = asyncio.get_running_loop().time() + within
    while True:
        if await check():
            return
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"condition not met within {within}s")
        await asyncio.sleep(interval)
