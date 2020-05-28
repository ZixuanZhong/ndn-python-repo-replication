import argparse
import time
import asyncio as aio
import logging
from ndn.app import NDNApp
from ndn.security import KeychainDigest
from replication.reponode import CommandClient, InsertHandle, DeleteHandle, RecallHandle, ReponodeClient

async def cmd(command_client : CommandClient, period : int, files):
    # register
    await command_client.register(files)
    # start timer
    while True:
        await aio.sleep(period)
        # print(period*1000)
        # print(str(time.time())+"\n")
        await command_client.update()

def main():
    parser = argparse.ArgumentParser(description='reponode')
    parser.add_argument('-r', '--repo_prefix',
                        required=True, help='Prefix of Repo ("/217B/repo")')
    parser.add_argument('-c', '--catalog_prefix',
                        required=True, help='Prefix of Catalog ("/217B/catalog")')
    parser.add_argument('-n', '--node_name',
                        required=True, help='Node name ("node/A")')
    parser.add_argument('-p', '--period',
                        type=int, default=10, help='Update period in second')
    parser.add_argument('-f', '--files',
                        nargs='+', default=[], help='List of uncorrupted files')
    args = parser.parse_args()

    logging.basicConfig(format='[%(asctime)s]%(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

    app = NDNApp(face=None, keychain=KeychainDigest())


    reponode_client = ReponodeClient(app)
    command_client = CommandClient(app, reponode_client, args.repo_prefix, args.catalog_prefix, args.node_name, args.period)
    insert_handle = InsertHandle(app, command_client, args.node_name, args.repo_prefix)
    delete_handle = DeleteHandle(app, command_client, args.node_name, args.repo_prefix)
    recall_handle = RecallHandle(app, command_client, args.node_name, args.repo_prefix)

    # TODO: start command_client's periodically timer
    # 
    # listens on /<repo_prefix>/<node_name>/insert
    insert_handle.listen()
    # listens on /<repo_prefix>/<node_name>/delete
    delete_handle.listen()
    # listens on /<repo_prefix>/<node_name>/recall
    recall_handle.listen()
    
    try:
        app.run_forever(after_start=cmd(command_client, args.period, args.files))
    except FileNotFoundError:
        logging.error('Error: could not connect to NFD.\n')
    return 0


if __name__ == '__main__':
    main()