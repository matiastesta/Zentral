from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'h1a2b3c4d5e6'
down_revision = 'g9h8i7j6k5l4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    if 'business_settings' not in tables:
        return

    cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
    if 'habilitar_sistema_cuotas' in cols:
        return

    try:
        op.execute(sa.text(
            'ALTER TABLE business_settings '
            'ADD COLUMN IF NOT EXISTS habilitar_sistema_cuotas BOOLEAN NOT NULL DEFAULT FALSE'
        ))
    except Exception:
        pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    if 'business_settings' not in tables:
        return

    cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
    if 'habilitar_sistema_cuotas' not in cols:
        return

    try:
        op.execute(sa.text('ALTER TABLE business_settings DROP COLUMN habilitar_sistema_cuotas'))
    except Exception:
        pass
