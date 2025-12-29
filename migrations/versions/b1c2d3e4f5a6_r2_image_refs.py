from alembic import op
import sqlalchemy as sa

revision = 'b1c2d3e4f5a6'
down_revision = 'a1b2c3d4e5f6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('product', schema=None) as batch_op:
        batch_op.add_column(sa.Column('image_file_id', sa.String(length=64), nullable=True))
        batch_op.create_index(batch_op.f('ix_product_image_file_id'), ['image_file_id'], unique=False)

    with op.batch_alter_table('business_settings', schema=None) as batch_op:
        batch_op.add_column(sa.Column('logo_file_id', sa.String(length=64), nullable=True))
        batch_op.add_column(sa.Column('background_file_id', sa.String(length=64), nullable=True))
        batch_op.create_index(batch_op.f('ix_business_settings_logo_file_id'), ['logo_file_id'], unique=False)
        batch_op.create_index(batch_op.f('ix_business_settings_background_file_id'), ['background_file_id'], unique=False)


def downgrade() -> None:
    with op.batch_alter_table('business_settings', schema=None) as batch_op:
        try:
            batch_op.drop_index(batch_op.f('ix_business_settings_background_file_id'))
        except Exception:
            pass
        try:
            batch_op.drop_index(batch_op.f('ix_business_settings_logo_file_id'))
        except Exception:
            pass
        batch_op.drop_column('background_file_id')
        batch_op.drop_column('logo_file_id')

    with op.batch_alter_table('product', schema=None) as batch_op:
        try:
            batch_op.drop_index(batch_op.f('ix_product_image_file_id'))
        except Exception:
            pass
        batch_op.drop_column('image_file_id')
