import sys
from typing import Callable, Dict, Any

import ujson
from sqlalchemy import func

from peek_plugin_base.storage.DbConnection import DbSessionCreator

__sysPathsJson = ujson.dumps(sys.path)


def runPyInPgBlocking(dbSessionCreator: DbSessionCreator,
                      classMethodToRun: Callable,
                      *args,
                      **kwargs) -> Any:
    argsJson = ujson.dumps(args if args else [])
    kwargsJson = ujson.dumps(kwargs if kwargs else {})

    loaderModuleClassMethodStr = '.'.join([
        classMethodToRun.__self__.__module__,
        classMethodToRun.__self__.__name__,
        classMethodToRun.__name__
    ])

    session = dbSessionCreator()
    try:
        sqlFunc = func.peek_storage.run_generic_python(
            argsJson,
            kwargsJson,
            loaderModuleClassMethodStr,
            __sysPathsJson
        )

        resultJsonStr: str = next(session.execute(sqlFunc))[0]

        resultJson: Dict = ujson.loads(resultJsonStr)
        return resultJson["result"]

    finally:
        session.close()
