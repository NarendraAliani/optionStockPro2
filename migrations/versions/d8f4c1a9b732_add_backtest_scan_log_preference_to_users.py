"""add backtest scan logfile preference to users

Revision ID: d8f4c1a9b732
Revises: c3f9a8b2d1e4
Create Date: 2026-02-18 10:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd8f4c1a9b732'
down_revision = 'c3f9a8b2d1e4'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}

    if 'backtest_scan_log_enabled' not in columns:
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.add_column(sa.Column('backtest_scan_log_enabled', sa.Boolean(), nullable=True))

    op.execute("UPDATE users SET backtest_scan_log_enabled = 0 WHERE backtest_scan_log_enabled IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column(
            'backtest_scan_log_enabled',
            existing_type=sa.Boolean(),
            nullable=False
        )


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    if 'backtest_scan_log_enabled' in columns:
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.drop_column('backtest_scan_log_enabled')
