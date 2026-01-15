from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'l2c3d4e5f6g7'
down_revision = 'k1b2c3d4e5f7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'company' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('company') or [])}
        if 'subscription_ends_at' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE company ADD COLUMN subscription_ends_at DATE'))
            except Exception:
                pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'company' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('company') or [])}
        if 'subscription_ends_at' in cols:
            try:
                op.execute(sa.text('ALTER TABLE company DROP COLUMN subscription_ends_at'))
            except Exception:
                pass
