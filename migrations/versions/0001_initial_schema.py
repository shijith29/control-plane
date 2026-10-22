"""initial schema: tenants, tasks, outbox, processed_updates

Revision ID: 0001
Revises:
Create Date: 2026-10-22
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SLUG_PATTERN = r"^[a-z][-a-z0-9]{1,26}[a-z0-9]$"


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("slug", sa.Text(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        # slug is unique FOREVER, destroyed tenants included (rows are never deleted)
        sa.UniqueConstraint("slug", name="tenants_slug_key"),
        sa.CheckConstraint(f"slug ~ '{SLUG_PATTERN}'", name="tenants_slug_format"),
        sa.CheckConstraint("length(btrim(name)) > 0", name="tenants_name_present"),
        sa.CheckConstraint(
            "status IN ('provisioning','active','updating','destroying','destroyed','failed')",
            name="tenants_status_valid",
        ),
        sa.CheckConstraint("version >= 1", name="tenants_version_pos"),
    )
    op.create_index("tenants_page_idx", "tenants", ["created_at", "id"])
    op.create_index("tenants_status_idx", "tenants", ["status"])

    op.create_table(
        "tasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "tenant_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("tenants.id"), nullable=False
        ),
        sa.Column("type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("type IN ('deploy','update','destroy')", name="tasks_type_valid"),
        sa.CheckConstraint(
            "status IN ('accepted','in_progress','done','failed')", name="tasks_status_valid"
        ),
    )
    # Safety net: at most ONE open task per tenant (the API guards already guarantee this)
    op.create_index(
        "tasks_one_open_per_tenant",
        "tasks",
        ["tenant_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('accepted', 'in_progress')"),
    )
    op.create_index("tasks_tenant_page_idx", "tasks", ["tenant_id", "created_at", "id"])
    op.create_index("tasks_page_idx", "tasks", ["created_at", "id"])

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("routing_key", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "outbox_pending_idx",
        "outbox",
        ["id"],
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.create_table(
        "processed_updates",
        sa.Column("update_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("task_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("outcome", sa.Text(), nullable=False),
        sa.Column(
            "received_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint(
            "outcome IN ('applied','stale','unknown_task')",
            name="processed_updates_outcome_valid",
        ),
    )


def downgrade() -> None:
    op.drop_table("processed_updates")
    op.drop_index("outbox_pending_idx", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("tasks_page_idx", table_name="tasks")
    op.drop_index("tasks_tenant_page_idx", table_name="tasks")
    op.drop_index("tasks_one_open_per_tenant", table_name="tasks")
    op.drop_table("tasks")
    op.drop_index("tenants_status_idx", table_name="tenants")
    op.drop_index("tenants_page_idx", table_name="tenants")
    op.drop_table("tenants")
