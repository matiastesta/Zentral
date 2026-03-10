from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'r1s2t3u4v5w6'
down_revision = 'q1r2s3t4u5v6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    
    # Crear tabla tanda_carga
    if 'tanda_carga' not in tables:
        try:
            op.create_table(
                'tanda_carga',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('company_id', sa.String(length=36), nullable=False, index=True),
                sa.Column('identificador', sa.String(length=64), nullable=False, index=True),
                sa.Column('tipo_origen', sa.String(length=32), nullable=False, server_default=sa.text("'excel'"), index=True),
                sa.Column('fecha_hora_creacion', sa.DateTime(), nullable=False, index=True),
                sa.Column('user_id', sa.Integer(), sa.ForeignKey('user.id'), nullable=True, index=True),
                sa.Column('cantidad_items', sa.Integer(), nullable=False, server_default=sa.text('0')),
                sa.Column('cantidad_total_unidades', sa.Float(), nullable=False, server_default=sa.text('0')),
                sa.Column('observacion', sa.Text(), nullable=True),
                sa.Column('estado', sa.String(length=16), nullable=False, server_default=sa.text("'activa'"), index=True),
                sa.Column('created_at', sa.DateTime(), nullable=False),
                sa.Column('updated_at', sa.DateTime(), nullable=False),
            )
        except Exception:
            pass
    
    # Agregar tanda_carga_id a inventory_lot
    if 'inventory_lot' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('inventory_lot') or [])}
        if 'tanda_carga_id' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE inventory_lot ADD COLUMN IF NOT EXISTS tanda_carga_id INTEGER'))
                op.execute(sa.text('CREATE INDEX IF NOT EXISTS idx_inventory_lot_tanda_carga_id ON inventory_lot (tanda_carga_id)'))
            except Exception:
                pass
    
    # Agregar tanda_carga_id a inventory_movement
    if 'inventory_movement' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('inventory_movement') or [])}
        if 'tanda_carga_id' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE inventory_movement ADD COLUMN IF NOT EXISTS tanda_carga_id INTEGER'))
                op.execute(sa.text('CREATE INDEX IF NOT EXISTS idx_inventory_movement_tanda_carga_id ON inventory_movement (tanda_carga_id)'))
            except Exception:
                pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    
    # Remover columnas tanda_carga_id
    if 'inventory_movement' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('inventory_movement') or [])}
        if 'tanda_carga_id' in cols:
            try:
                op.execute(sa.text('ALTER TABLE inventory_movement DROP COLUMN tanda_carga_id'))
            except Exception:
                pass
    
    if 'inventory_lot' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('inventory_lot') or [])}
        if 'tanda_carga_id' in cols:
            try:
                op.execute(sa.text('ALTER TABLE inventory_lot DROP COLUMN tanda_carga_id'))
            except Exception:
                pass
    
    # Eliminar tabla tanda_carga
    try:
        op.drop_table('tanda_carga')
    except Exception:
        pass
