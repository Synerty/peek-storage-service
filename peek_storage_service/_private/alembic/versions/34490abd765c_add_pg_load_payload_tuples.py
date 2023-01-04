"""add pg load_payload_tuples

Revision ID: 34490abd765c
Revises: 2efc91c0f0f6
Create Date: 2020-04-24 19:47:40.784157

"""

# revision identifiers, used by Alembic.
revision = "34490abd765c"
down_revision = "2efc91c0f0f6"
branch_labels = None
depends_on = None

from alembic import op
import sqlalchemy as sa
import geoalchemy2


def upgrade():
    # This is now moved to objects and created every time Peek starts
    pass


def downgrade():
    pass
