from alembic import op
import sqlalchemy as sa

revision = 'e3f2c9a4b1d0'
down_revision = 'd4f1e8a0c9b7'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('business_settings', schema=None) as batch_op:
        batch_op.add_column(sa.Column('background_image_filename', sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column('background_brightness', sa.Float(), nullable=True))
        batch_op.add_column(sa.Column('background_contrast', sa.Float(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table('business_settings', schema=None) as batch_op:
        batch_op.drop_column('background_contrast')
        batch_op.drop_column('background_brightness')
        batch_op.drop_column('background_image_filename')
