"""add run_generic_python

Revision ID: 12a0ab3826f3
Revises: 34490abd765c
Create Date: 2020-04-25 15:28:50.279318

"""

# revision identifiers, used by Alembic.
revision = "12a0ab3826f3"
down_revision = "34490abd765c"
branch_labels = None
depends_on = None

from alembic import op


def upgrade():
    # This is now moved to objects and created every time Peek starts
    pass


def downgrade():
    pass
