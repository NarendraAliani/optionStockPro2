"""add scan_workers to users

Revision ID: a4c8f2d19e6b
Revises: 1b2d4e6f8a10
Create Date: 2026-02-14 13:30:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a4c8f2d19e6b'
down_revision = '1b2d4e6f8a10'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}

    if 'scan_workers' not in columns:
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.add_column(sa.Column('scan_workers', sa.Integer(), nullable=True))

    op.execute("UPDATE users SET scan_workers = 4 WHERE scan_workers IS NULL")

    with op.batch_alter_table('users', schema=None) as batch_op:
        batch_op.alter_column(
            'scan_workers',
            existing_type=sa.Integer(),
            nullable=False
        )


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    columns = {col['name'] for col in inspector.get_columns('users')}
    if 'scan_workers' in columns:
        with op.batch_alter_table('users', schema=None) as batch_op:
            batch_op.drop_column('scan_workers')
