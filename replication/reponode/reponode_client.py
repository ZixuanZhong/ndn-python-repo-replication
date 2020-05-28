import time
import asyncio as aio
import logging
import pickle
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from ndn.types import InterestNack, InterestTimeout,InterestCanceled,ValidationFailure
from ndn.security import KeychainDigest
from ndn.utils import gen_nonce
from ..tlv_models import SqlsTlvModel
from ..tlv_models import SqlresultsTlvModel
from ..tlv_models import DatainfoTlvModel
import os
import platform
from typing import List

class ReponodeClient():

    def __init__(self, app: NDNApp):
        """
   
        """
        self.app = app

    async def send_repo_command(self, node_prefix : NonStrictName, verb : str , datainfo : DatainfoTlvModel):
        # "/a/b" -> list of bytearrays
        name = Name.normalize(node_prefix)
        # "/a/b" -> "/a/b/insert"
        name.append(Component.from_str(verb))
        datainfo_name_component = Component.from_bytes(datainfo.encode())
        name.append(datainfo_name_component)
        logging.info('Interest Sent: {}\n'.format(Name.to_str(name)))
        try:
            data_name, meta_info, content = await self.app.express_interest(name, must_be_fresh=True, can_be_prefix=False, nonce=gen_nonce(), lifetime=1000)
            logging.info('Data Received: {}\n'.format(Name.to_str(data_name)))
            # print(meta_info)
            # print(bytes(content) if content else None)
        except InterestNack as e:
            # A NACK is received
            logging.warning(f'Interest Nacked with reason={e.reason}\n')
            return 0
        except InterestTimeout:
            # Interest times out
            logging.warning(f'Interest Timeout\n')
            return 0
        # results = self.parse_results(content)
        # logging.info(results)
        return 1
    
    async def insert(self, node_prefix : NonStrictName, data_name : str, hash : str, desired_copies : int = 3):
        datainfo_tlv_model = DatainfoTlvModel()
        datainfo_tlv_model.data_name = data_name.encode()
        datainfo_tlv_model.hash = hash.encode()
        datainfo_tlv_model.desired_copies = desired_copies

        return await self.send_repo_command(node_prefix, 'insert', datainfo_tlv_model)



    
