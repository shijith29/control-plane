"""SQLAlchemy Core table definitions — the single source of truth for the schema.

Alembic migrations (``migrations/versions``) are written to match these definitions, and
``make migration`` autogenerates new revisions from them. Constraint names are explicit so
the code can recognise *which* constraint a write violated.
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

metadata = MetaData()

SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"

TENANT_SLUG_UNIQUE = "tenants_slug_key"
TASK_ONE_OPEN_PER_TENANT = "tasks_one_open_per_tenant"

tenants = Table(
    "tenants",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("slug", Text, nullable=False),
    Column("name", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("version", Integer, nullable=False, server_default=text("1")),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    UniqueConstraint("slug", name=TENANT_SLUG_UNIQUE),
    CheckConstraint(f"slug ~ '{SLUG_PATTERN}'", name="tenants_slug_format"),
    CheckConstraint("length(btrim(name)) > 0", name="tenants_name_present"),
    CheckConstraint(
        "status IN ('provisioning','active','updating','destroying','destroyed','failed')",
        name="tenants_status_valid",
    ),
    CheckConstraint("version >= 1", name="tenants_version_pos"),
)
Index("tenants_page_idx", tenants.c.created_at, tenants.c.id)
Index("tenants_status_idx", tenants.c.status)

tasks = Table(
    "tasks",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True),
    Column("tenant_id", UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False),
    Column("type", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("error", Text, nullable=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("type IN ('deploy','update','destroy')", name="tasks_type_valid"),
    CheckConstraint(
        "status IN ('accepted','in_progress','done','failed')", name="tasks_status_valid"
    ),
)
Index(
    TASK_ONE_OPEN_PER_TENANT,
    tasks.c.tenant_id,
    unique=True,
    postgresql_where=text("status IN ('accepted', 'in_progress')"),
)
Index("tasks_tenant_page_idx", tasks.c.tenant_id, tasks.c.created_at, tasks.c.id)
Index("tasks_page_idx", tasks.c.created_at, tasks.c.id)

outbox = Table(
    "outbox",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("event_id", UUID(as_uuid=True), nullable=False, unique=True),
    Column("routing_key", Text, nullable=False),
    Column("payload", JSONB, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("published_at", DateTime(timezone=True), nullable=True),
    Column("attempts", Integer, nullable=False, server_default=text("0")),
    Column("last_error", Text, nullable=True),
)
Index(
    "outbox_pending_idx",
    outbox.c.id,
    postgresql_where=text("published_at IS NULL"),
)

processed_updates = Table(
    "processed_updates",
    metadata,
    Column("update_id", UUID(as_uuid=True), primary_key=True),
    Column("task_id", UUID(as_uuid=True), nullable=False),
    Column("status", Text, nullable=False),
    Column("outcome", Text, nullable=False),
    Column("received_at", DateTime(timezone=True), nullable=False, server_default=text("now()")),
    CheckConstraint(
        "outcome IN ('applied','stale','unknown_task')", name="processed_updates_outcome_valid"
    ),
)
