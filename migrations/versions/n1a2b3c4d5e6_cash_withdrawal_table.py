from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'n1a2b3c4d5e6'
down_revision = 'm3d4e5f6g7h8'
branch_labels = None
depends_on = None


def upgrade() -> None:
    try:
        bind = op.get_bind()
        insp = inspect(bind)
        if 'cash_withdrawals' in set(insp.get_table_names() or []):
            return
    except Exception:
        pass
    op.create_table(
        'cash_withdrawals',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('company_id', sa.String(length=36), nullable=False, index=True),
        sa.Column('fecha_imputacion', sa.Date(), nullable=False, index=True),
        sa.Column('fecha_registro', sa.DateTime(), nullable=False),
        sa.Column('monto', sa.Float(), nullable=False, server_default='0'),
        sa.Column('nota', sa.Text(), nullable=True),
        sa.Column('usuario_registro_id', sa.Integer(), nullable=True),
        sa.Column('usuario_responsable_id', sa.Integer(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
    )
    try:
        op.create_index('ix_cash_withdrawals_company_id', 'cash_withdrawals', ['company_id'], unique=False)
    except Exception:
        pass
    try:
        op.create_index('ix_cash_withdrawals_fecha_imputacion', 'cash_withdrawals', ['fecha_imputacion'], unique=False)
    except Exception:
        pass
    try:
        op.create_index('ix_cash_withdrawals_company_imputacion', 'cash_withdrawals', ['company_id', 'fecha_imputacion'], unique=False)
    except Exception:
        pass


def downgrade() -> None:
    try:
        op.drop_index('ix_cash_withdrawals_company_imputacion', table_name='cash_withdrawals')
    except Exception:
        pass
    try:
        op.drop_index('ix_cash_withdrawals_fecha_imputacion', table_name='cash_withdrawals')
    except Exception:
        pass
    try:
        op.drop_index('ix_cash_withdrawals_company_id', table_name='cash_withdrawals')
    except Exception:
        pass
    op.drop_table('cash_withdrawals')
