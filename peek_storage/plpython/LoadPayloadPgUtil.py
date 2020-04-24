import sys
from collections import namedtuple
from typing import Callable, Dict, Optional

import ujson
from sqlalchemy import func
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import Select

from peek_plugin_base.storage.DbConnection import DbSessionCreator

LoadPayloadTupleResult = namedtuple("LoadPayloadTupleResult", ['count', 'encodedPayload'])

__sysPathsJson = ujson.dumps(sys.path)


def callPGLoadPayloadTuplesBlocking(dbSessionCreator: DbSessionCreator,
                                    sql: Select,
                                    sqlCoreLoadTupleClassmethod: Callable,
                                    payloadFilt: Optional[Dict] = None,
                                    fetchSize=50) -> LoadPayloadTupleResult:
    payloadFileJson = ujson.dumps(payloadFilt if payloadFilt else {})

    sqlStr = str(sql.compile(dialect=postgresql.dialect(),
                             compile_kwargs={"literal_binds": True}))

    loaderModuleClassMethodStr = '.'.join([
        sqlCoreLoadTupleClassmethod.__self__.__module__,
        sqlCoreLoadTupleClassmethod.__self__.__name__,
        sqlCoreLoadTupleClassmethod.__name__
    ])

    session = dbSessionCreator()
    try:
        sqlFunc = func.peek_storage.load_paylaod_tuples(
            sqlStr,
            payloadFileJson,
            loaderModuleClassMethodStr,
            __sysPathsJson,
            fetchSize
        )

        resultJsonStr: str = next(session.execute(sqlFunc))[0]

        resultJson: Dict = ujson.loads(resultJsonStr)
        if resultJson["encodedPayload"]:
            resultJson["encodedPayload"] = resultJson["encodedPayload"].encode()

        return LoadPayloadTupleResult(**resultJson)

    finally:
        session.close()
