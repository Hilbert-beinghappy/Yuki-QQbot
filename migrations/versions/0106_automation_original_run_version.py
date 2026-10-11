"""Bind new automation runs to their original definition and delegation."""

import sqlalchemy as sa
from alembic import op

revision = "0106"
down_revision = "0105"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Existing versions have no original delegation receipt;
    # never reconstruct one from a mutable plan or a matching script hash.
    additions = {
        "authority_snapshot_json": "TEXT",
        "canonical_target_person_id": "VARCHAR(36) REFERENCES persons(id) ON UPDATE RESTRICT ON DELETE RESTRICT",
        "canonical_target_space_id": "VARCHAR(36) REFERENCES spaces(id) ON UPDATE RESTRICT ON DELETE RESTRICT",
        "canonical_presence_id": "VARCHAR(36) REFERENCES presences(id) ON UPDATE RESTRICT ON DELETE RESTRICT",
    }
    for name, declaration in additions.items():
        op.execute(f"ALTER TABLE automation_versions ADD COLUMN {name} {declaration}")
    op.execute(
        "ALTER TABLE automation_runs ADD COLUMN version_id INTEGER REFERENCES automation_versions(id) ON DELETE RESTRICT"
    )
    op.drop_column("automations", "misfire_grace_seconds")
    op.create_index(
        "ix_automation_runs_running",
        "automation_runs",
        ["automation_id", "scheduled_for", "id"],
        sqlite_where=sa.text("status = 'running'"),
    )


def downgrade() -> None:
    # Original definition identities and grants remain recovery facts.
    pass
