"""add signal details table

Revision ID: 9a7c4b21d5ef
Revises: f1a2c9d4b7e3
Create Date: 2026-02-13 16:25:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9a7c4b21d5ef'
down_revision = 'f1a2c9d4b7e3'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not inspector.has_table('signal_details'):
        op.create_table(
            'signal_details',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('signal_id', sa.Integer(), nullable=False),
            sa.Column('candle_open', sa.Numeric(precision=10, scale=2), nullable=True),
            sa.Column('candle_high', sa.Numeric(precision=10, scale=2), nullable=True),
            sa.Column('candle_low', sa.Numeric(precision=10, scale=2), nullable=True),
            sa.Column('candle_close', sa.Numeric(precision=10, scale=2), nullable=True),
            sa.Column('displayed_at', sa.DateTime(), nullable=True),
            sa.ForeignKeyConstraint(['signal_id'], ['signals.id'], ondelete='CASCADE'),
            sa.PrimaryKeyConstraint('id'),
            sa.UniqueConstraint('signal_id')
        )
    # Re-read inspector after potential create.
    inspector = sa.inspect(bind)
    existing_indexes = {idx.get('name') for idx in inspector.get_indexes('signal_details')}
    index_name = op.f('ix_signal_details_signal_id')
    if index_name not in existing_indexes:
        op.create_index(index_name, 'signal_details', ['signal_id'], unique=True)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if inspector.has_table('signal_details'):
        existing_indexes = {idx.get('name') for idx in inspector.get_indexes('signal_details')}
        index_name = op.f('ix_signal_details_signal_id')
        if index_name in existing_indexes:
            op.drop_index(index_name, table_name='signal_details')
        op.drop_table('signal_details')
