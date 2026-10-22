"""Engine construction and helpers for recognising constraint violations."""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from control_plane.config import Settings

UNIQUE_VIOLATION = "23505"


def make_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
    )


def violated_constraint(exc: IntegrityError) -> str | None:
    """Name of the unique constraint/index an IntegrityError violated, if any.

    SQLAlchemy wraps the asyncpg exception; the original (with ``sqlstate`` and
    ``constraint_name``) is the ``__cause__`` of ``exc.orig``.
    """
    original = getattr(exc.orig, "__cause__", None)
    sqlstate = getattr(original, "sqlstate", None) or getattr(exc.orig, "sqlstate", None)
    if sqlstate != UNIQUE_VIOLATION:
        return None
    name = getattr(original, "constraint_name", None)
    return str(name) if name else None
