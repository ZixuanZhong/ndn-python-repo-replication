import asyncio
import logging
import pickle
from ndn.types import *
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component
from ..tlv_models.sqls import SqlsTlvModel
from ..tlv_models.sqlresults import SqlresultsTlvModel
import sqlite3

class CommandHandle():
    def __init__(self, app: NDNApp, prefix: str, db: str):
        self.app = app
        self.prefix = prefix
        self.db = db

    def listen(self):
        # listen on interests
        logging.info('Start Listening on : {}\n'.format(self.prefix))
        self.app.route(self.prefix)(self._on_interest)

    def _on_interest(self, int_name, _int_param, _app_param):
        # logging.info('Interest Received: {}\n'.format(Name.to_str(int_name)))
        logging.info('Interest Received')

        # an invalid Interest which has the exactly same name as the catalog's prefix
        if(len(int_name)==len(self.prefix)):
            return

        sqls = self.decode_sql(int_name, self.prefix)

        logging.info('SQLs Executed: {}'.format(sqls))

        results = self.execute_sqls(sqls)

        serialized_results = pickle.dumps(results)

        # print(type(serialized_results))

        sqlresults_tlv_model = SqlresultsTlvModel()
        sqlresults_tlv_model.results = serialized_results
        # deserialized_results = pickle.loads(serialized_results)
    
        # print(type(deserialized_results[1][0]))
        # print(deserialized_results)

        # assert deserialized_results == results

        # print()
        # print(type(sqlresults_tlv_model.encode()))

        logging.info('SQLs Results: {}\n'.format(results))
        self.app.put_data(int_name, content=sqlresults_tlv_model.encode(), freshness_period=1000)

    @staticmethod
    def decode_sql(name, catalog_prefix):
        #remove the prefix
        sql_name_component = name[len(Name.normalize(catalog_prefix))]
        # parse the sql_name_component
        sqls_tlv_model = SqlsTlvModel.parse(Component.get_value(sql_name_component).tobytes())
        # from list of bytearray to list of string
        sqls = list(map(lambda x: x.tobytes().decode(), sqls_tlv_model.sqls))
        return sqls

    def execute_sqls(self, sqls):      
        conn = sqlite3.connect(self.db)
        results = []
        for sql in sqls:
            result = self.execute_sql(conn, sql)
            results.append(result)
        conn.close()
        return results

    def execute_sql(self, conn, sql):      
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
        result = c.fetchall()
        return result
        