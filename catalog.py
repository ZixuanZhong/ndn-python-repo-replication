import sys
import argparse
import asyncio as aio
import logging
from ndn.app import NDNApp
from replication.catalog.command_handle import CommandHandle

def main():
    parser = argparse.ArgumentParser(description='catalog')
    parser.add_argument('-d', '--database_file',
                        required=True, help='Path to (sqlite3) database file')
    parser.add_argument('-p', '--prefix',
                        required=True, help='Prefix of Catalog ("/217B/catalog")')
    args = parser.parse_args()
    logging.basicConfig(format='[%(asctime)s]%(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

    app = NDNApp()

    command_handle = CommandHandle(app, args.prefix, args.database_file)
    # listens on /<prefix>
    command_handle.listen()
    try:
        app.run_forever()
    except FileNotFoundError:
        logging.error('Error: could not connect to NFD.\n')
    return 0

if __name__ == "__main__":
    sys.exit(main())
