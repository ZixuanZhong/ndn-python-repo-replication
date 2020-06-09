import time
import asyncio as aio
import logging
import pickle
import random
from ndn.app import NDNApp
from ndn.encoding import Name, NonStrictName, Component, DecodeError
from ndn.types import InterestNack, InterestTimeout,InterestCanceled,ValidationFailure
from ndn.security import KeychainDigest
from ndn.utils import gen_nonce
from .reponode_client import ReponodeClient
from ..tlv_models import SqlsTlvModel
from ..tlv_models import SqlresultsTlvModel

import os
import platform
from typing import List


class CommandClient():

    def __init__(self, app: NDNApp, reponode_client : ReponodeClient, repo_prefix:NonStrictName, catalog_prefix: NonStrictName, node_name: NonStrictName, update_period: int):
        """
        A client to send SQL commands to remote catalog

        :param app: NDNApp.
        :param catalog_prefix: NonStrictName. Routable name to remote catalog. For example, '/217B/catalog'
        :param node_name: NonStrictName. the name of the node. For example, 'node/A'
        :param update_period: int. The time period in seconds for periodically UPDATE commands
        """
        self.app = app
        self.reponode_client = reponode_client
        self.repo_prefix = repo_prefix
        self.catalog_prefix = catalog_prefix
        self.node_name = node_name
        self.update_period = update_period

    async def send_sqls(self, sqls : List[str]):
        # change to TLV encoding

        sqls_bytes = list(map(lambda x: x.encode(), sqls))

        sqls_tlv_model = SqlsTlvModel()
        sqls_tlv_model.sqls = sqls_bytes

        # logging.info(sqls_tlv_model.encode())

        sqls_name_component = Component.from_bytes(sqls_tlv_model.encode())
        name = Name.normalize(self.catalog_prefix)
        name.append(sqls_name_component)
        # logging.info('Interest Sent: {}\n'.format(Name.to_str(name)))
        logging.info('Interest Sent')
        try:
            data_name, meta_info, content = await self.app.express_interest(name, must_be_fresh=True, can_be_prefix=False, nonce=gen_nonce(), lifetime=1000)
            # logging.info('Data Received: {}\n'.format(Name.to_str(data_name)))
            logging.info('Data Received')
            # print(meta_info)
            # print(bytes(content) if content else None)
        except InterestNack as e:
            # A NACK is received
            logging.warning(f'Interest Nacked with reason={e.reason}')
            return []
        except InterestTimeout:
            # Interest times out
            logging.warning(f'Interest Timeout')
            return []
        results = self.parse_results(content)
        # logging.info(results)
        return results

    def parse_results(self, content):
        # print(type(content))
        sqlresults_tlv_model = SqlresultsTlvModel.parse(content)
        serialized_results = sqlresults_tlv_model.results
        deserialized_results = pickle.loads(serialized_results)
        # print(deserialized_results)
        # print(type(deserialized_results[1][0]))
        return deserialized_results

    async def register(self, files):
        num = 2
        logging.info('Catalog Command: REGISTER')
        now = time.time()
        sqls = [ \
        'REPLACE INTO nodes (id, node_name, valid_thru, updated_at) VALUES( (SELECT id FROM nodes WHERE node_name="{0}"), "{0}",{1:.0f},{2:.0f});'.format(self.node_name, now+self.update_period, now), \
        'SELECT DISTINCT B.id, B.data_name, B.hash FROM data_nodes A, data B, nodes C WHERE A.data_id=B.id and A.node_id=C.id and (C.node_name="{0}");'.format(self.node_name)]
        sql_results=await self.send_sqls(sqls)
        logging.info('SQLs Results: {}\n'.format(sql_results))
        # recover 
        await self.recover(sql_results[1], files)

    async def recover(self, previous_files, current_files):
        logging.info('Catalog Command: RECOVER')
        previous_files = list(map(lambda x: x[1], previous_files))

        if len(current_files) >= (len(previous_files)/2):
            # optimistic recover
            corrupted_files =  (list(set(previous_files) - set(current_files)))
            sqls = ['DELETE FROM data_nodes WHERE id IN ( SELECT A.id  FROM data_nodes A, nodes B, data C WHERE A.node_id=B.id AND A.data_id=C.id AND ( C.data_name IN ("{0}") ) AND B.node_name="{1}" );'.format('", "'.join(corrupted_files), self.node_name)]
            sql_results=await self.send_sqls(sqls)
            logging.info('SQLs Results: {}\n'.format(sql_results))

        else:
            # pessimistic recover
            sqls = [ \
            'DELETE FROM data_nodes \
            WHERE id IN \
            ( SELECT A.id FROM data_nodes A, nodes B WHERE A.node_id=B.id AND B.node_name="{0}" );'.format(self.node_name)]

            for current_file in current_files:
                sql = 'INSERT INTO data_nodes (data_id, node_id) VALUES ( (SELECT A.id FROM data A WHERE A.data_name="{0}" ), (SELECT B.id FROM nodes B WHERE B.node_name="{1}" ) );'.format(current_file, self.node_name)
                sqls.append(sql)
            
            sql_results=await self.send_sqls(sqls)
            logging.info('SQLs Results: {}\n'.format(sql_results))
        


    async def add(self, data_name :str, hash : str, desired_copies : int=3):
        num = 2
        # now = time.time()
        logging.info('Catalog Command: ADD {} {} {}'.format(data_name, hash, desired_copies))
        sqls = [ \
        'INSERT OR IGNORE INTO data (data_name, hash, desired_copies) VALUES("{0}","{1}",{2:d});'.format(data_name, hash, desired_copies), \
        'INSERT OR IGNORE INTO data_nodes (data_id, node_id) VALUES((SELECT DISTINCT id FROM data WHERE data_name="{0}"),(SELECT DISTINCT id FROM nodes WHERE node_name="{1}"));'.format(data_name, self.node_name)]
        sql_results = await self.send_sqls(sqls)
        # trigger an UPDATE
        await self.update()

    async def update(self):
        num = 4
        logging.info('Catalog Command: UPDATE')
        now = time.time()
        sqls = [ \
        'UPDATE nodes SET valid_thru={0:.0f}, updated_at={1:.0f} WHERE node_name="{2}";'.format(now+self.update_period, now, self.node_name), \
        'SELECT DISTINCT node_name FROM nodes WHERE valid_thru>={0:.0f} AND accept_new=1 ORDER BY updated_at DESC'.format(now),
        'SELECT DISTINCT B.id, B.data_name, B.hash, C.node_name \
        FROM data_nodes A, data B, nodes C \
        WHERE A.data_id=B.id AND A.node_id=C.id AND \
        (B.id IN (SELECT F.data_id FROM data_nodes F, nodes G WHERE F.node_id=G.id and G.node_name="{0}") ) AND \
        B.desired_copies > (SELECT COUNT(*) FROM data_nodes D, nodes E WHERE D.node_id=E.id AND D.data_id=B.id AND E.valid_thru>={1:.0f}) AND \
        C.valid_thru>={2:.0f};'.format(self.node_name, now, now), \
        'SELECT DISTINCT B.id, B.data_name, B.hash \
        FROM data_nodes A, data B, nodes C \
        WHERE A.data_id=B.id AND A.node_id=C.id AND \
        (B.id IN (SELECT F.data_id FROM data_nodes F, nodes G WHERE F.node_id=G.id and G.node_name="{0}") ) AND \
        B.desired_copies=0;'.format(self.node_name)]
        sql_results = await self.send_sqls(sqls)

        logging.info('SQLs Results: {}\n'.format(sql_results))


        # check the length of the sql_results
        if len(sql_results) != num:
            # TODO: error message
            return 

        # replicate / delete data
        data_to_be_deleted = sql_results[3]
        # delte data locally and send RECALL command
        for data in data_to_be_deleted:
            data_name = data[1]
            hash = data[2]
            await self.recall(data_name, hash)
        
        # get the list of active nodes
        active_nodes = sql_results[1]

        data_to_be_replicated = sql_results[2]

        # merge rows
        data_to_be_replicated_dict = {}
        for data in data_to_be_replicated:
            id = data[0]
            if id in data_to_be_replicated_dict:
                data_dict = data_to_be_replicated_dict[id]
                data_dict['nodes'].append(data[3])
            else:
                data_dict = {}
                data_dict['data_name'] = data[1]
                data_dict['hash'] = data[2]
                data_dict['nodes'] = [data[3]]
                data_to_be_replicated_dict[id] = data_dict

        # print(data_to_be_replicated_dict)

        
        await aio.gather(*(self.replicate(active_nodes, data) for key, data in data_to_be_replicated_dict.items()))


    async def remove(self, data_name :str, hash : str):
        num = 1
        logging.info('Catalog Command: REMOVE {} {}'.format(data_name, hash))
        sqls = [ \
        'UPDATE data SET desired_copies=0 WHERE data_name="{0}" AND hash="{1}";'.format(data_name, hash), \
        'DELETE FROM data_nodes WHERE \
        id=(SELECT A.id FROM data_nodes A, data B, nodes C \
        WHERE A.data_id=B.id AND A.node_id=C.id AND \
        B.data_name="{0}" AND B.hash="{1}" AND C.node_name="{2}");'.format(data_name, hash, self.node_name)]
        sql_results = await self.send_sqls(sqls)
        # remove data locally and send RECALL as confirmation
        # await self.recall(data_name, hash)

    async def recall(self, data_name :str, hash : str):
        num = 1
        logging.info('Catalog Command: RECALL {} {}'.format(data_name, hash))
        sqls = [ \
        'DELETE FROM data_nodes WHERE \
        id=(SELECT A.id FROM data_nodes A, data B, nodes C \
        WHERE A.data_id=B.id AND A.node_id=C.id AND \
        B.data_name="{0}" AND B.hash="{1}" AND C.node_name="{2}");'.format(data_name, hash, self.node_name)]
        sql_results = await self.send_sqls(sqls)
        logging.info('\n')

    async def replicate(self, active_nodes, data):
        if len(active_nodes) == 0:
            logging.warning('Want to replicate data but no active node exists')
            return 
        data_name = data['data_name']
        hash = data['hash']
        nodes = data['nodes']

        active_nodes = list(map(lambda x: x[0],active_nodes))

        # candidate_nodes = active_nodes - nodes

        candidate_nodes = (list(set(active_nodes) - set(nodes)))

        # shuffle the active_nodes list
        random.shuffle(candidate_nodes)
        insert_success = 0
        index = 0
        while insert_success == 0 and index < len(candidate_nodes):
            
            name = Name.normalize(self.repo_prefix)
            name.extend(Name.normalize(candidate_nodes[index]))

            logging.info('Replication {} to {}'.format(data_name, Name.to_str(name)))

            insert_success = await self.reponode_client.insert(name, data_name, hash)
            index += 1
        if insert_success == 0:
            logging.warning('Replication failed after {} trials\n'.format(len(candidate_nodes)))    

    