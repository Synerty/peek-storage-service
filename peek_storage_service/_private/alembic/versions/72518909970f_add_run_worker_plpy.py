"""add run worker plpy

Revision ID: 72518909970f
Revises: bab14faf0cd7
Create Date: 2020-05-20 20:13:19.092876

"""

# revision identifiers, used by Alembic.

revision = "72518909970f"
down_revision = "bab14faf0cd7"
branch_labels = None
depends_on = None

from alembic import op


def upgrade():
    # This is now moved to objects and created every time Peek starts
    pass


def downgrade():
    pass
