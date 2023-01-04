"""up1_run_generic_py

Revision ID: 51e48a6594d5
Revises: 72518909970f
Create Date: 2020-06-02 22:27:48.650874

"""

# revision identifiers, used by Alembic.
revision = "51e48a6594d5"
down_revision = "72518909970f"
branch_labels = None
depends_on = None

from alembic import op


def upgrade():
    # This is now moved to objects and created every time Peek starts
    pass


def downgrade():
    pass
