"""add is_active flag to notams

Revision ID: a0299faefcd5
Revises: f92e6ef70154
Create Date: 2025-09-06 17:51:55.692881

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a0299faefcd5'
down_revision: Union[str, Sequence[str], None] = 'f92e6ef70154'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('notams', sa.Column('is_active', sa.Boolean(), nullable=False, server_default='true'))

def downgrade() -> None:
    op.drop_column('notams', 'is_active')
