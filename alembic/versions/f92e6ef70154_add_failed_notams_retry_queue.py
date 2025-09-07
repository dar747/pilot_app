"""add failed notams retry queue

Revision ID: f92e6ef70154
Revises: bafb1fc857f1
Create Date: 2025-09-06 16:46:46.137692

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'f92e6ef70154'
down_revision: Union[str, Sequence[str], None] = 'bafb1fc857f1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('failed_notams',
        sa.Column('id', sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column('notam_number', sa.String(50), nullable=False, index=True),
        sa.Column('icao_message', sa.Text(), nullable=False),
        sa.Column('airport', sa.String(4), nullable=False, index=True),
        sa.Column('issue_time', sa.String(100), nullable=True),
        sa.Column('raw_hash', sa.String(64), nullable=False, index=True),
        sa.Column('failure_reason', sa.Text(), nullable=True),
        sa.Column('retry_count', sa.Integer(), default=0, nullable=False),
        sa.Column('last_retry_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Index('idx_failed_notams_retry', 'retry_count', 'last_retry_at'),
    )

def downgrade() -> None:
    op.drop_table('failed_notams')
