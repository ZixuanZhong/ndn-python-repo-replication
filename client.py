import argparse
import asyncio as aio
import logging
from ndn.app import NDNApp
from ndn.encoding import Name, Component, FormalName
from ndn.types import InterestNack, InterestTimeout,InterestCanceled,ValidationFailure
from ndn.security import KeychainDigest
from ndn.utils import gen_nonce
from replication.reponode import CommandClient, InsertHandle, DeleteHandle
from replication.tlv_models import DatainfoTlvModel

async def send(app : NDNApp, name : FormalName):
    logging.info('Interest Sent: {}\n'.format(Name.to_str(name)))
    try:
        data_name, meta_info, content = await app.express_interest( name, must_be_fresh=True, can_be_prefix=False, nonce=gen_nonce(), lifetime=1000)
        # Print out Data Name, MetaInfo and its conetnt.
        logging.info('Data Received: {}\n'.format(Name.to_str(data_name)))
    except InterestNack as e:
        # A NACK is received
        logging.warning(f'Interest Nacked with reason={e.reason}\n')
    except InterestTimeout:
        # Interest times out
        logging.warning(f'Interest Timeout\n')
    finally:
        app.shutdown()
    

def main():
    parser = argparse.ArgumentParser(description='reponode')
    parser.add_argument('-n', '--node_prefix',
                        required=True, help='Prefix of catalog ("/217B/repo/node/A")')
    parser.add_argument('-c', '--command',
                        default='insert', choices=['insert', 'delete', 'recall'], help='Command Verb')
    parser.add_argument('-d', '--data_name',
                        required=True, help='data name ("/foo/bar/1.txt")')
    parser.add_argument('-s', '--hash',
                        required=True, help='data hash ("1bd109fe")')
    parser.add_argument('-o', '--desired_copies',
                        type=int, default=3, help='desired copies')
    args = parser.parse_args()

    logging.basicConfig(format='[%(asctime)s]%(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

    app = NDNApp(face=None, keychain=KeychainDigest())

    name = Name.from_str(args.node_prefix)
    name.append(Component.from_str(args.command))

    datainfo_tlv_model = DatainfoTlvModel()
    datainfo_tlv_model.data_name = args.data_name.encode()
    datainfo_tlv_model.hash = args.hash.encode()
    datainfo_tlv_model.desired_copies = args.desired_copies
    datainfo_name_component = Component.from_bytes(datainfo_tlv_model.encode())

    name.append(datainfo_name_component)
    
    logging.info(name)
    
    try:
        app.run_forever(after_start=send(app, name))
    except FileNotFoundError:
        logging.error('Error: could not connect to NFD.\n')
    return 0


if __name__ == '__main__':
    main()