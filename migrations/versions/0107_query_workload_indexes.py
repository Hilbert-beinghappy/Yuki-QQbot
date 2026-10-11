"""Remove the retired Memory target cap and index actual wait/due scans."""

import sqlalchemy as sa
from alembic import op

revision = "0107"
down_revision = "0106"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "DELETE FROM runtime_config_overrides WHERE config_key = 'memory.max_referenced_targets'"
    )
    op.create_index("ix_runtime_work_wait_scan", "runtime_work_waits", ["status", "created", "id"])
    op.create_index(
        "ix_person_aliases_nickname_current",
        "person_aliases",
        ["canonical_person_id", "last_seen_at", "id"],
        sqlite_where=sa.text("alias_type = 'nickname' AND canonical_space_id IS NULL"),
    )
    op.execute(
        "CREATE INDEX ix_canonical_rollup_pending_due ON canonical_conversation_rollup_jobs "
        "(status, julianday(next_attempt_at)) WHERE status = 'pending'"
    )
    op.execute(
        "CREATE INDEX ix_canonical_rollup_processing_due ON canonical_conversation_rollup_jobs "
        "(status, julianday(lease_until)) WHERE status = 'processing'"
    )


def downgrade() -> None:
    op.drop_index("ix_canonical_rollup_processing_due", "canonical_conversation_rollup_jobs")
    op.drop_index("ix_canonical_rollup_pending_due", "canonical_conversation_rollup_jobs")
    op.drop_index("ix_runtime_work_wait_scan", "runtime_work_waits")
    op.drop_index("ix_person_aliases_nickname_current", "person_aliases")
