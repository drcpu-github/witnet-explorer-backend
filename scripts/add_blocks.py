import optparse
import sys

import toml

from blockchain.objects.block import Block
from blockchain.witnet_database import WitnetDatabase
from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode
from util.database_manager import DatabaseManager


def add_block(
    config,
    db_mngr,
    witnet_node,
    consensus_constants,
    block_epoch=None,
    block_hash=None,
):
    assert block_epoch is not None or block_hash is not None

    sql = """
        SELECT
            tapi_start_epoch,
            tapi_stop_epoch,
            tapi_bit
        FROM
            wips
        WHERE
            tapi_bit IS NOT NULL
    """
    tapi_periods = db_mngr.sql_return_all(sql)

    if block_epoch:
        block = Block(
            consensus_constants,
            block_epoch=block_epoch,
            database=db_mngr,
            witnet_node=witnet_node,
            tapi_periods=tapi_periods,
        )
    else:
        block = Block(
            consensus_constants,
            block_hash=block_hash,
            database=db_mngr,
            witnet_node=witnet_node,
            tapi_periods=tapi_periods,
        )

    block_json = block.process_block("explorer")
    addresses = block.process_addresses()

    epoch = block_json["details"]["epoch"]
    print(f"Adding block {block_json['details']['hash']} for epoch {epoch}")

    witnet_database = WitnetDatabase(config["database"])
    witnet_database.insert_block(block_json)
    witnet_database.insert_mint_txn(block_json["transactions"]["mint"], epoch)
    for txn_details in block_json["transactions"]["value_transfer"]:
        witnet_database.insert_value_transfer_txn(txn_details, epoch)
    for txn_details in block_json["transactions"]["data_request"]:
        witnet_database.insert_data_request_txn(txn_details, epoch)
    for txn_details in block_json["transactions"]["commit"]:
        witnet_database.insert_commit_txn(txn_details, epoch)
    for txn_details in block_json["transactions"]["reveal"]:
        witnet_database.insert_reveal_txn(txn_details, epoch)
    for txn_details in block_json["transactions"]["tally"]:
        witnet_database.insert_tally_txn(txn_details, epoch)
    witnet_database.insert_addresses(addresses)
    witnet_database.finalize(epoch)


def main():
    parser = optparse.OptionParser()
    parser.add_option("--epochs", type="string", dest="epochs")
    parser.add_option("--start-epoch", type="int", dest="start_epoch")
    parser.add_option("--stop-epoch", type="int", dest="stop_epoch")
    parser.add_option("--hashes", type="string", dest="hashes")
    parser.add_option(
        "--config-file",
        type="string",
        default="explorer.toml",
        dest="config_file",
        help="Specify a configuration file",
    )
    options, args = parser.parse_args()

    config = toml.load(options.config_file)
    db_mngr = DatabaseManager(config["database"])
    witnet_node = WitnetNode(config["node-pool"], timeout=300)
    consensus_constants = ConsensusConstants(database=db_mngr, witnet_node=witnet_node)

    if options.epochs is not None:
        epochs_to_add = [int(epoch) for epoch in options.epochs.split(",")]
    elif options.start_epoch is not None and options.stop_epoch is not None:
        epochs_to_add = range(options.start_epoch, options.stop_epoch + 1)
    elif options.hashes is not None:
        hashes_to_add = options.hashes.split(",")
    else:
        sys.stderr.write("Usage of this script:\n")
        sys.stderr.write("./add_blocks --epochs <comma-separated string>\n")
        sys.stderr.write("./add_blocks --start-epoch <x> --stop-epoch <y>\n")
        sys.stderr.write("./add_blocks --hashes <comma-separated string>\n")
        sys.exit(1)

    if epochs_to_add:
        for block_epoch in epochs_to_add:
            add_block(
                config,
                db_mngr,
                witnet_node,
                consensus_constants,
                block_epoch=block_epoch,
            )
    else:
        for block_hash in hashes_to_add:
            add_block(
                config,
                db_mngr,
                witnet_node,
                consensus_constants,
                block_hash=block_hash,
            )


if __name__ == "__main__":
    main()
