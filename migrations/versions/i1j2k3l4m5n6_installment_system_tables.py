from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'i1j2k3l4m5n6'
down_revision = 'h1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # sale.is_installments
    bind = op.get_bind()
    insp = inspect(bind)
    tables = set(insp.get_table_names() or [])
    if 'sale' in tables:
        cols = {str(c.get('name') or '') for c in (insp.get_columns('sale') or [])}
        if 'is_installments' not in cols:
            try:
                op.execute(sa.text('ALTER TABLE sale ADD COLUMN IF NOT EXISTS is_installments BOOLEAN NOT NULL DEFAULT FALSE'))
            except Exception:
                pass

    # installment tables
    if 'installment_plan' not in tables:
        try:
            op.create_table(
                'installment_plan',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('company_id', sa.String(length=36), nullable=False, index=True),
                sa.Column('sale_id', sa.Integer(), sa.ForeignKey('sale.id'), nullable=False, index=True),
                sa.Column('sale_ticket', sa.String(length=32), nullable=True, index=True),
                sa.Column('customer_id', sa.String(length=64), nullable=False, index=True),
                sa.Column('customer_name', sa.String(length=255), nullable=True),
                sa.Column('start_date', sa.Date(), nullable=False, index=True),
                sa.Column('interval_days', sa.Integer(), nullable=False, server_default=sa.text('30')),
                sa.Column('installments_count', sa.Integer(), nullable=False, server_default=sa.text('1')),
                sa.Column('total_amount', sa.Float(), nullable=False, server_default=sa.text('0')),
                sa.Column('installment_amount', sa.Float(), nullable=False, server_default=sa.text('0')),
                sa.Column('first_payment_method', sa.String(length=32), nullable=True),
                sa.Column('status', sa.String(length=16), nullable=False, server_default=sa.text("'activo'")),
                sa.Column('created_at', sa.DateTime(), nullable=False),
                sa.Column('updated_at', sa.DateTime(), nullable=False),
            )
        except Exception:
            pass

    # refrescar lista por si se creó recién
    tables = set(insp.get_table_names() or [])
    if 'installment' not in tables:
        try:
            op.create_table(
                'installment',
                sa.Column('id', sa.Integer(), primary_key=True),
                sa.Column('company_id', sa.String(length=36), nullable=False, index=True),
                sa.Column('plan_id', sa.Integer(), sa.ForeignKey('installment_plan.id'), nullable=False, index=True),
                sa.Column('installment_number', sa.Integer(), nullable=False),
                sa.Column('due_date', sa.Date(), nullable=False, index=True),
                sa.Column('amount', sa.Float(), nullable=False, server_default=sa.text('0')),
                sa.Column('status', sa.String(length=16), nullable=False, server_default=sa.text("'pendiente'")),
                sa.Column('paid_at', sa.DateTime(), nullable=True),
                sa.Column('paid_payment_method', sa.String(length=32), nullable=True),
                sa.Column('paid_sale_id', sa.Integer(), sa.ForeignKey('sale.id'), nullable=True, index=True),
            )
        except Exception:
            pass


def downgrade() -> None:
    try:
        op.drop_table('installment')
    except Exception:
        pass
    try:
        op.drop_table('installment_plan')
    except Exception:
        pass
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
