"""add api error log preference to users

Revision ID: f7e1b5c29a44
Revises: d8f4c1a9b732
Create Date: 2026-02-18 12:20:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'f7e1b5c29a44'
down_revision = 'd8f4c1a9b732'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}

    if 'api_error_log_enabled' not in columns:
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.add_column(sa.Column('api_error_log_enabled', sa.Boolean(), nullable=True))

    op.execute("UPDATE users SET api_error_log_enabled = 0 WHERE api_error_log_enabled IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column(
            'api_error_log_enabled',
            existing_type=sa.Boolean(),
            nullable=False
        )


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    if 'api_error_log_enabled' in columns:
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.drop_column('api_error_log_enabled')
