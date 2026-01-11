from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'j2k3l4m5n6o7'
down_revision = (
    'b1c2d3e4f5a6',
    'i1j2k3l4m5n6',
)
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'business_settings' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
        if 'habilitar_sistema_cuotas' not in cols:
            op.execute(sa.text('ALTER TABLE business_settings ADD COLUMN habilitar_sistema_cuotas BOOLEAN NOT NULL DEFAULT FALSE'))

    if 'sale' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        if 'is_installments' not in cols:
            op.execute(sa.text('ALTER TABLE sale ADD COLUMN is_installments BOOLEAN NOT NULL DEFAULT FALSE'))


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])

    if 'sale' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        if 'is_installments' in cols:
            try:
                op.execute(sa.text('ALTER TABLE sale DROP COLUMN is_installments'))
            except Exception:
                pass

    if 'business_settings' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('business_settings') or [])}
        if 'habilitar_sistema_cuotas' in cols:
            try:
                op.execute(sa.text('ALTER TABLE business_settings DROP COLUMN habilitar_sistema_cuotas'))
            except Exception:
                pass
