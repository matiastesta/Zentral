from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'k1b2c3d4e5f7'
down_revision = 'j2k3l4m5n6o7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'company' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('company') or [])}
        if 'pause_scheduled_for' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE company ADD COLUMN pause_scheduled_for DATE'))
            except Exception:
                pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'company' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('company') or [])}
        if 'pause_scheduled_for' in cols:
            try:
                op.execute(sa.text('ALTER TABLE company DROP COLUMN pause_scheduled_for'))
            except Exception:
                pass
