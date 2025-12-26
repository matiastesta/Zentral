from alembic import op
import sqlalchemy as sa

revision = 'b7c0d01c9a1f'
down_revision = '75eb0b70b404'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute('ALTER TABLE "user" DROP CONSTRAINT IF EXISTS user_username_key')
    op.execute('ALTER TABLE "user" DROP CONSTRAINT IF EXISTS uq_user_username')
    op.execute('ALTER TABLE "user" DROP CONSTRAINT IF EXISTS uq_user_company_username')

    with op.batch_alter_table('user', schema=None) as batch_op:
        batch_op.alter_column('email', existing_type=sa.String(length=255), nullable=True)

    op.execute('CREATE UNIQUE INDEX IF NOT EXISTS ix_user_email ON "user" (email)')
    op.execute('ALTER TABLE "user" ADD CONSTRAINT uq_user_company_username UNIQUE (company_id, username)')


def downgrade() -> None:
    op.execute('ALTER TABLE "user" DROP CONSTRAINT IF EXISTS uq_user_company_username')
    op.execute('ALTER TABLE "user" ADD CONSTRAINT user_username_key UNIQUE (username)')
