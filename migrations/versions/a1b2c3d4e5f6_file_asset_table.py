from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect

revision = 'a1b2c3d4e5f6'
down_revision = 'd1e2f3a4b5c6'
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    if 'file_asset' not in set(insp.get_table_names() or []):
        op.create_table(
            'file_asset',
            sa.Column('id', sa.String(length=64), nullable=False),
            sa.Column('company_id', sa.String(length=36), nullable=False),
            sa.Column('entity_type', sa.String(length=32), nullable=True),
            sa.Column('entity_id', sa.String(length=64), nullable=True),
            sa.Column('storage_provider', sa.String(length=16), nullable=False),
            sa.Column('bucket', sa.String(length=128), nullable=True),
            sa.Column('object_key', sa.String(length=512), nullable=True),
            sa.Column('original_name', sa.String(length=255), nullable=True),
            sa.Column('content_type', sa.String(length=128), nullable=True),
            sa.Column('size_bytes', sa.Integer(), nullable=False),
            sa.Column('checksum_sha256', sa.String(length=64), nullable=True),
            sa.Column('etag', sa.String(length=128), nullable=True),
            sa.Column('status', sa.String(length=16), nullable=False),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('company_id', 'storage_provider', 'bucket', 'object_key', name='uq_file_asset_company_object'),
        )

    with op.batch_alter_table('file_asset', schema=None) as batch_op:
        try:
            batch_op.create_index(batch_op.f('ix_file_asset_company_id'), ['company_id'], unique=False)
        except Exception:
            pass
        try:
            batch_op.create_index(batch_op.f('ix_file_asset_entity_type'), ['entity_type'], unique=False)
        except Exception:
            pass
        try:
            batch_op.create_index(batch_op.f('ix_file_asset_entity_id'), ['entity_id'], unique=False)
        except Exception:
            pass
        try:
            batch_op.create_index(batch_op.f('ix_file_asset_object_key'), ['object_key'], unique=False)
        except Exception:
            pass


def downgrade() -> None:
    bind = op.get_bind()
    insp = inspect(bind)
    if 'file_asset' not in set(insp.get_table_names() or []):
        return
    with op.batch_alter_table('file_asset', schema=None) as batch_op:
        try:
            batch_op.drop_index(batch_op.f('ix_file_asset_object_key'))
        except Exception:
            pass
        try:
            batch_op.drop_index(batch_op.f('ix_file_asset_entity_id'))
        except Exception:
            pass
        try:
            batch_op.drop_index(batch_op.f('ix_file_asset_entity_type'))
        except Exception:
            pass
        try:
            batch_op.drop_index(batch_op.f('ix_file_asset_company_id'))
        except Exception:
            pass

    op.drop_table('file_asset')
