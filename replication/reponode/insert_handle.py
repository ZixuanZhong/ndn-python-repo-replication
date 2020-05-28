import asyncio as aio
import logging
from ndn.types import *
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component
from ..tlv_models import SqlsTlvModel, DatainfoTlvModel
from .command_client import CommandClient
import sqlite3

class InsertHandle():
    def __init__(self, app: NDNApp, command_client : CommandClient, node_name: NonStrictName, repo_prefix : NonStrictName):
        self.app = app
        self.command_client = command_client
        self.node_name = node_name
        self.repo_prefix = repo_prefix
        node_name_name = Name.normalize(self.node_name)
        self.node_prefix = Name.normalize(self.repo_prefix)
        self.node_prefix.extend(node_name_name)
        self.node_insert_prefix = self.node_prefix
        self.node_insert_prefix.append(Component.from_str('insert'))


    def listen(self):
        # listen on interests
        self.app.route(self.node_insert_prefix)(self._on_insert_interest)

    def _on_insert_interest(self, int_name, _int_param, _app_param):

        logging.info('INSERT Interest Received: {}\n'.format(Name.to_str(int_name)))

        # an invalid Interest which has the exactly same name as the catalog's prefix
        if(len(int_name)==len(self.node_insert_prefix)):
            return

        data_name, hash, desired_copies = self.decode_datainfo(int_name, self.node_insert_prefix)
        self.app.put_data(int_name, content=b'ok', freshness_period=1000)
        
        if desired_copies == None:
            aio.ensure_future(self.command_client.add(data_name, hash))
        else:
            aio.ensure_future(self.command_client.add(data_name, hash, desired_copies))

    @staticmethod
    def decode_datainfo(name, node_insert_prefix):
        # get the datainfo name component
        datainfo_name_component = name[len(node_insert_prefix)]
        # parse the datainfo_name_component
        datainfo_tlv_model = DatainfoTlvModel.parse(Component.get_value(datainfo_name_component).tobytes())
        # from bytearray to string (uint)
        data_name = datainfo_tlv_model.data_name.tobytes().decode()
        hash = datainfo_tlv_model.hash.tobytes().decode()
        desired_copies = datainfo_tlv_model.desired_copies
        return data_name, hash, desired_copies


    # def execute_sqls(self, sqls):      
    #     conn = sqlite3.connect(self.db)
    #     c = conn.cursor()
    #     for sql in sqls:
    #         c.execute(sql)
    #     conn.commit()
    #     results = c.fetchall()
    #     print(type(results))
    #     conn.close()
    #     return results

        