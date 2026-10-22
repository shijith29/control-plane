"""Control-plane process: REST API + outbox relay + updates consumer (README §1).

Run with:  uvicorn control_plane.main:create_app --factory --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from control_plane.api import routes_tasks, routes_tenants
from control_plane.api.errors import install_error_handlers
from control_plane.config import Settings, get_settings
from control_plane.db.engine import make_engine
from control_plane.logs import configure_logging, request_id_var
from control_plane.messaging.broker import Broker
from control_plane.messaging.outbox_relay import AmqpPublisher, OutboxRelay
from control_plane.messaging.updates_consumer import UpdateApplier, UpdatesConsumer
from control_plane.service import TenantService

log = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    *,
    engine: AsyncEngine | None = None,
    run_background: bool = True,
) -> FastAPI:
    """App factory.

    ``run_background=False`` serves HTTP only (no broker) — used by API tests.
    """
    settings = settings or get_settings()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging(settings.log_level)
        db = engine or make_engine(settings)
        app.state.engine = db
        app.state.service = TenantService(db)
        app.state.broker = None

        stop = asyncio.Event()
        tasks: list[asyncio.Task[object]] = []
        if run_background:
            broker = Broker(settings.rabbitmq_url)
            app.state.broker = broker
            relay = OutboxRelay(
                db,
                AmqpPublisher(broker),
                batch_size=settings.outbox_batch_size,
                poll_interval_s=settings.outbox_poll_interval_ms / 1000,
            )
            consumer = UpdatesConsumer(UpdateApplier(db), max_retries=settings.consumer_max_retries)

            async def relay_when_ready() -> None:
                await broker.wait_ready()
                await relay.run(stop)

            tasks = [
                asyncio.create_task(broker.connect(stop), name="broker-connect"),
                asyncio.create_task(relay_when_ready(), name="outbox-relay"),
                asyncio.create_task(
                    consumer.run(broker, stop, settings.consumer_prefetch), name="updates-consumer"
                ),
            ]
        log.info("control plane started", extra={"background": run_background})
        try:
            yield
        finally:
            stop.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if app.state.broker is not None:
                await app.state.broker.close()
            if engine is None:
                await db.dispose()
            log.info("control plane stopped")

    app = FastAPI(
        title="Tenant Provisioning Control Plane",
        version="0.1.0",
        lifespan=lifespan,
        description="Tenants are provisioned asynchronously through tasks. See README.",
    )
    install_error_handlers(app)
    app.include_router(routes_tenants.router)
    app.include_router(routes_tasks.router)

    @app.middleware("http")
    async def request_id(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        rid = request.headers.get("x-request-id") or str(uuid.uuid4())
        token = request_id_var.set(rid[:64])
        try:
            response = await call_next(request)
        finally:
            request_id_var.reset(token)
        response.headers["X-Request-ID"] = rid[:64]
        return response

    @app.get("/healthz", tags=["ops"], summary="Liveness")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["ops"], summary="Readiness: DB reachable and broker connected")
    async def readyz(request: Request) -> JSONResponse:
        checks: dict[str, str] = {}
        try:
            async with request.app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            checks["database"] = "ok"
        except Exception:
            checks["database"] = "unavailable"
        broker: Broker | None = request.app.state.broker
        checks["broker"] = (
            "disabled" if broker is None else ("ok" if broker.is_connected else "unavailable")
        )
        ready = all(v in ("ok", "disabled") for v in checks.values())
        return JSONResponse(status_code=200 if ready else 503, content={"ready": ready, **checks})

    return app
