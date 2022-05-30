#!/usr/bin/python3

import optparse
import sys
import time
import toml

from util.database_manager import DatabaseManager
from util.witnet_node import WitnetNode

#########################
# List all known TAPI's #
#########################

def list_TAPI(db_mngr):
    sql = "SELECT id, title, description, start_epoch, stop_epoch, bit, urls FROM tapi ORDER BY id ASC"
    result = db_mngr.sql_return_all(sql)
    for tapi in result:
        tapi_id, title, description, start_epoch, stop_epoch, bit, urls = tapi
        print(f"TAPI {tapi_id}")
        print(f"\tTitle: {title}")
        print(f"\tDescription: {description}")
        print(f"\tStarted at epoch: {start_epoch}")
        print(f"\tStopped at epoch: {stop_epoch}")
        print(f"\tUsing signaling bit: {bit}")
        for counter, url in enumerate(urls):
            print(f"\tURL of WIP {counter + 1}: {url}")
    return result

############################################
# Add a new TAPI with associated paramters #
############################################

def add_TAPI(db_mngr):
    # Read the TAPI title
    tapi_title = input("Specify the title of the TAPI? ")

    # Read the TAPI description
    tapi_description = input("Specify the description of the TAPI? ")

    # Read the start epoch
    while True:
        try:
            start_epoch = int(input("Specify the start epoch of the TAPI? "))
            break
        except ValueError:
            continue

    # Read the stop epoch
    while True:
        try:
            stop_epoch = int(input("Specify the stop epoch of the TAPI? "))
            break
        except ValueError:
            continue

    # Read the bit used for this TAPI
    while True:
        try:
            bit = int(input("Specify the signaling bit used for the TAPI? "))
            break
        except ValueError:
            continue

    # Read all URLs of all WIP's for this TAPI
    urls = []
    while True:
        try:
            url = input("Specify the url of (one of) the WIP's (an empty input sequence stops this prompt)? ")
            if len(url) == 0:
                break
            urls.append(url)
        except ValueError:
            continue

    sql = "INSERT INTO tapi (title, description, start_epoch, stop_epoch, bit, urls) VALUES (%s, %s, %s, %s, %s, %s)"
    db_mngr.sql_insert_one(sql, (tapi_title, tapi_description, start_epoch, stop_epoch, bit, urls))

######################################################
# Update all TAPI blocks with their acceptance value #
######################################################

def process_TAPI(db_mngr, witnet_node):
    tapis = list_TAPI(db_mngr)
    for tapi in tapis:
        tapi_id, title, description, start_epoch, stop_epoch, bit, urls = tapi
        sql = "SELECT epoch, block_hash, tapi_accept, confirmed FROM blocks WHERE epoch BETWEEN %s and %s ORDER BY epoch ASC" % (start_epoch, stop_epoch)
        result = db_mngr.sql_return_all(sql)
        for db_block in result:
            epoch, block_hash, tapi_accept, confirmed = db_block
            if confirmed and tapi_accept == None:
                print(f"Updating TAPI signal for epoch {epoch}")

                block = witnet_node.get_block(bytes(block_hash).hex())
                if type(block) is dict and "error" in block:
                    sys.stderr.write(f"Could not fetch block: {block}\n")
                    continue

                tapi_signal = block["result"]["block_header"]["signals"]
                tapi_accept = (tapi_signal & (1 << bit)) != 0

                sql = "UPDATE blocks SET tapi_accept=%s WHERE epoch=%s" % (tapi_accept, epoch)
                db_mngr.sql_update_table(sql)

#####################################################
#   Test how many blocks are signaling acceptance   #
#####################################################

def test_TAPI(db_mngr, witnet_node, start_epoch, stop_epoch, bit):
    sql = "SELECT epoch, block_hash, confirmed FROM blocks WHERE epoch BETWEEN %s and %s ORDER BY epoch ASC" % (start_epoch, stop_epoch)
    result = db_mngr.sql_return_all(sql)

    blocks_analyzed, blocks_accepting, blocks_rejecting = 0, 0, 0
    for db_block in result:
        epoch, block_hash, confirmed = db_block
        if confirmed:
            print(f"Checking TAPI signal for epoch {epoch}")

            block = witnet_node.get_block(bytes(block_hash).hex())
            if type(block) is dict and "error" in block:
                sys.stderr.write(f"Could not fetch block: {block}\n")
                continue

            tapi_signal = block["result"]["block_header"]["signals"]
            tapi_accept = (tapi_signal & (1 << bit)) != 0

            blocks_analyzed += 1
            if tapi_accept:
                blocks_accepting += 1
            else:
                blocks_rejecting += 1

    accepting_percentage = blocks_accepting / (blocks_accepting + blocks_rejecting) * 100
    rejecting_percentage = blocks_rejecting / (blocks_accepting + blocks_rejecting) * 100
    print(f"Blocks analyzed: {stop_epoch - start_epoch + 1}\nBlocks accepting: {blocks_accepting} ({accepting_percentage:.2f}%)\nBlocks rejecting: {blocks_rejecting} ({rejecting_percentage:.2f}%)")

def main():
    parser = optparse.OptionParser()

    parser.add_option("--list", action="store_true", dest="list", default=False, help="List all TAPI entries")

    parser.add_option("--add", action="store_true", dest="add", default=False, help="Add a new TAPI entry")

    parser.add_option("--process", action="store_true", dest="process", default=False, help="Check all TAPI epochs and add signals if needed")

    parser.add_option("--test", action="store_true", dest="test", default=False, help="Test the TAPI processing code")
    parser.add_option("--start_epoch", dest="start_epoch", type=int, help="Test the TAPI processing code")
    parser.add_option("--stop_epoch", dest="stop_epoch", type=int, help="Test the TAPI processing code")
    parser.add_option("--bit", dest="bit", type=int, help="Test the TAPI processing code")

    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")

    options, args = parser.parse_args()

    config = toml.load(options.config_file)

    db_mngr = DatabaseManager(config["database"]["user"], config["database"]["name"], config["database"]["password"], None)

    if options.list:
        list_TAPI(db_mngr)

    if options.add:
        add_TAPI(db_mngr)

    witnet_node = WitnetNode(config["node-pool"]["host"], config["node-pool"]["port"], 15, None, "")

    if options.process:
        process_TAPI(db_mngr, witnet_node)

    if options.test:
        assert options.start_epoch != None and options.stop_epoch != None and options.bit != None
        test_TAPI(db_mngr, witnet_node, options.start_epoch, options.stop_epoch, options.bit)

if __name__ == "__main__":
    main()
