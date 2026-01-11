from alembic import op
import sqlalchemy as sa

revision = 'i1j2k3l4m5n6'
down_revision = 'h1a2b3c4d5e6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # sale.is_installments
    with op.batch_alter_table('sale', schema=None) as batch_op:
        try:
            batch_op.add_column(sa.Column('is_installments', sa.Boolean(), nullable=False, server_default=sa.text('0')))
        except Exception:
            pass

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


def downgrade() -> None:
    try:
        op.drop_table('installment')
    except Exception:
        pass
    try:
        op.drop_table('installment_plan')
    except Exception:
        pass
    with op.batch_alter_table('sale', schema=None) as batch_op:
        try:
            batch_op.drop_column('is_installments')
        except Exception:
            pass
