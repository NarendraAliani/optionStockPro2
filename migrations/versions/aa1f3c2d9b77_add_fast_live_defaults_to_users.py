"""add fast live defaults to users

Revision ID: aa1f3c2d9b77
Revises: 9b1d2e3f4a6b
Create Date: 2026-03-11 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa1f3c2d9b77'
down_revision = '9b1d2e3f4a6b'
branch_labels = None
depends_on = None


def _add_column_if_missing(table_name, column):
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns(table_name)}
    if column.name in columns:
        return
    with op.batch_alter_table(table_name, schema=None) as batch_op:
        batch_op.add_column(column)


def upgrade():
    _add_column_if_missing('users', sa.Column('live_fast_mode_enabled', sa.Boolean(), nullable=True))
    _add_column_if_missing('users', sa.Column('live_fast_max_stocks', sa.Integer(), nullable=True))
    _add_column_if_missing('users', sa.Column('live_fast_max_strikes_per_stock', sa.Integer(), nullable=True))

    op.execute("UPDATE users SET live_fast_mode_enabled = 1 WHERE live_fast_mode_enabled IS NULL")
    op.execute("UPDATE users SET live_fast_max_stocks = 60 WHERE live_fast_max_stocks IS NULL")
    op.execute("UPDATE users SET live_fast_max_strikes_per_stock = 24 WHERE live_fast_max_strikes_per_stock IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column('live_fast_mode_enabled', existing_type=sa.Boolean(), nullable=False)
        batch_op.alter_column('live_fast_max_stocks', existing_type=sa.Integer(), nullable=False)
        batch_op.alter_column('live_fast_max_strikes_per_stock', existing_type=sa.Integer(), nullable=False)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    with op.batch_alter_table('users', schema=None) as batch_op:
        for name in (
            'live_fast_max_strikes_per_stock',
            'live_fast_max_stocks',
            'live_fast_mode_enabled'
        ):
            if name in columns:
                batch_op.drop_column(name)
