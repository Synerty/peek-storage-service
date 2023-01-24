import logging
from time import sleep

from peek_plugin_base.storage.DbConnection import DbConnection
from psycopg2.extensions import (
    ISOLATION_LEVEL_AUTOCOMMIT,
    ISOLATION_LEVEL_DEFAULT,
)

logger = logging.getLogger(__name__)


class StorageInit:
    def __init__(self, dbConnection: DbConnection):
        self._dbConnection = dbConnection

    def runPreMigrate(self):
        self._upgradeTimescaleDbExtension()

    def runPostMigrate(self):
        from .alembic.objects import object_load_paylaod_tuples
        from .alembic.objects import object_run_generic_python
        from .alembic.objects import object_run_worker_task_python

        session = self._dbConnection.ormSessionCreator()

        objects = (
            object_load_paylaod_tuples,
            object_run_generic_python,
            object_run_worker_task_python,
        )

        for obj in objects:
            logger.debug("(Re)creating object %s", obj.__name__)
            session.execute(obj.sql)

        session.commit()
        session.close()

    def _upgradeTimescaleDbExtension(self):
        rawConn = self._dbConnection.dbEngine.raw_connection()
        rawConn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = rawConn.cursor()
        try:

            logger.debug("Updating timescaledb extension")
            cursor.execute("ALTER EXTENSION timescaledb UPDATE")

            while len(rawConn.notices) < 2:
                if "shared_preload_libraries" in rawConn.notices:
                    raise Exception(
                        "|shared_preload_libraries = 'timescaledb'| is "
                        "missing from postgresql.conf"
                    )
                sleep(2.0)

            else:
                logger.debug(rawConn.notices[1].replace("\n", ", "))

        except Exception as e:
            if "Start a new session" in str(e):
                logger.debug(
                    "Skipping timescale extension update as the extension is"
                    " already loaded"
                )

                return

            if 'extension "timescaledb" does not exist' in str(e):
                logger.debug(
                    "Skipping timescale extension update as the extension"
                    " doesn't exist yet"
                )

                return

            logger.error("Updating timescaledb extesion failed")
            logger.exception(e)

        finally:
            rawConn.set_isolation_level(ISOLATION_LEVEL_DEFAULT)
            rawConn.close()
            cursor.close()
