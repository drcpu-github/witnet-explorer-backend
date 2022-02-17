#!/usr/bin/python3

import datetime
import logging
import logging.handlers
import os
import optparse
import re
import shutil
import signal
import sys
import time
import toml

from multiprocessing import Lock
from multiprocessing import Process
from multiprocessing import Queue

from queue import Empty

from blockchain.witnet_database import WitnetDatabase

from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode

from objects.block import Block

from transactions.data_request import DataRequest
from transactions.value_transfer import ValueTransfer

class BlockExplorer(object):
    def __init__(self, config, logging_queue):
        error_retry = config["explorer"]["error_retry"]

        self.pending_interval = config["explorer"]["pending_interval"]

        # Set up logger
        self.configure_logging_process(logging_queue, "explorer")
        self.logger = logging.getLogger("explorer")

        # Set up logging queue for logging from different processes
        self.logging_queue = logging_queue

        # Get configuration to connect to the node pool
        self.node_config = config["node-pool"]
        socket_host, socket_port = self.node_config["host"], self.node_config["port"]

        # Create nodes to connect to the node pool
        self.insert_blocks_node = WitnetNode(socket_host, socket_port, 30, self.logging_queue, "node-insert")
        self.confirm_blocks_node = WitnetNode(socket_host, socket_port, 30, self.logging_queue, "node-confirm")
        self.insert_pending_node = WitnetNode(socket_host, socket_port, 30, self.logging_queue, "node-pending")

        # Get consensus constants
        self.consensus_constants = ConsensusConstants(socket_host, socket_port, error_retry, self.logging_queue, "node-consensus")

        # Get configuration to connect to the database
        self.db_config = config["database"]
        db_user, db_name, db_pass = self.db_config["user"], self.db_config["name"], self.db_config["password"]

        # Create database objects
        self.insert_blocks_database = WitnetDatabase(db_user, db_name, db_pass, self.logging_queue, "db-insert")
        self.confirm_blocks_database = WitnetDatabase(db_user, db_name, db_pass, self.logging_queue, "db-confirm")
        self.insert_pending_database = WitnetDatabase(db_user, db_name, db_pass, self.logging_queue, "db-pending")

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def terminate(self):
        self.insert_blocks_database.terminate()
        self.confirm_blocks_database.terminate()
        self.insert_pending_database.terminate()
        self.logger.info("Terminating explorer")

    def insert_block(self, database, block_hash_hex_str, block, epoch, tapi_periods):
        # Create block object and parse it to a JSON object
        block = Block(block_hash_hex_str, self.consensus_constants, self.logging_queue, db_config=self.db_config, block=block, tapi_periods=tapi_periods, node_config=self.node_config)
        block_json = block.process_block("explorer")

        # Insert block
        database.insert_block(block_json)

        # Insert transactions
        self.insert_transactions(database, block_json, epoch)

        # Finalize insertions and updates on every block
        database.finalize(epoch)

    def insert_transactions(self, database, block_json, epoch):
        # Insert mint transaction
        database.insert_mint_txn(block_json["mint_txn"], epoch)

        # Insert value transfer transactions
        for txn_details in block_json["value_transfer_txns"]:
            database.insert_value_transfer_txn(txn_details, epoch)

        # Insert data request transactions
        for txn_details in block_json["data_request_txns"]:
            database.insert_data_request_txn(txn_details, epoch)

        # Insert commit transactions
        for txn_details in block_json["commit_txns"]:
            database.insert_commit_txn(txn_details, epoch)

        # Insert reveal transactions
        for txn_details in block_json["reveal_txns"]:
            database.insert_reveal_txn(txn_details, epoch)

        # Insert tally transactions
        for txn_details in block_json["tally_txns"]:
            database.insert_tally_txn(txn_details, epoch)

    def insert_blocks_and_transactions(self, logging_queue, unconfirmed_blocks_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "explorer-insert")
        logger = logging.getLogger("explorer-insert")

        # Get some consensus constants
        checkpoints_period = self.consensus_constants.checkpoints_period

        logger.info("Querying database for last confirmed block")
        # Get the last block we inserted into the database
        last_block_hash, last_epoch = self.insert_blocks_database.get_last_block()
        if last_epoch == -1:
            logger.info(f"Last confirmed block was at epoch 0")
        else:
            logger.info(f"Last confirmed block was {last_block_hash} at epoch {last_epoch}")
        # If we are adding the first block, initialize last_block_hash with the bootstrap_hash
        if last_block_hash == "":
            last_block_hash = self.consensus_constants.bootstrap_hash

        # sleep until the next poll interval
        next_poll_interval = (int(time.time() / checkpoints_period) + 1) * checkpoints_period + 1
        sleep_for = max(0, next_poll_interval - time.time())
        logger.info(f"Waiting {int(sleep_for)}s until the start of the next epoch")
        time.sleep(sleep_for)

        # Infinite loop
        while True:
            next_poll_interval = (int(time.time() / checkpoints_period) + 1) * checkpoints_period + 1

            # Get TAPI periods
            sql = "SELECT start_epoch, stop_epoch, bit FROM tapi"
            tapi_periods = self.insert_blocks_database.sql_return_all(sql)

            # Get all new blockchain digests
            blockchain = self.insert_blocks_node.get_blockchain(epoch=last_epoch + 1)
            if type(blockchain) is dict and "error" in blockchain:
                logger.warning(f"Unable to fetch recent blocks: {blockchain['error']}")
                blockchain = []
            else:
                blockchain = blockchain["result"]

            for epoch, block_hash_hex_str in blockchain:
                logger.info(f"Inserting data for epoch {epoch}")

                block = self.insert_blocks_node.get_block(block_hash_hex_str)
                # The database entries related to this block have not been modified yet
                # Break and retry fetching the blockchain from this point at the next epoch
                if type(block) is dict and "error" in block:
                    logger.warning(f"Unable to fetch block {block_hash_hex_str}: {block['error']}")
                    break
                block = block["result"]

                # Insert block
                self.insert_block(self.insert_blocks_database, block_hash_hex_str, block, epoch, tapi_periods)

                # Check if the block is confirmed and if it isn't track the hash
                confirmed = block["confirmed"]
                if not confirmed:
                    # Put the block hash from unconfirmed blocks in the queue to be processed by the confirm process
                    unconfirmed_blocks_queue.put((epoch, block_hash_hex_str))

                # Every bit of necessary data was inserted into the database for the current epoch and block
                last_epoch = epoch
                last_block_hash = block_hash_hex_str

            sleep_for = max(0, next_poll_interval - time.time())
            time.sleep(sleep_for)

    def confirm_blocks_and_transactions(self, logging_queue, unconfirmed_blocks_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "explorer-confirm")
        logger = logging.getLogger("explorer-confirm")

        # Calculate superepoch period from consensus constants
        superblock_period = self.consensus_constants.superblock_period
        checkpoints_period = self.consensus_constants.checkpoints_period

        # sleep until the next poll interval
        next_poll_interval = (int(time.time() / checkpoints_period) + 1) * checkpoints_period + 5
        sleep_for = max(0, next_poll_interval - time.time())
        time.sleep(sleep_for)

        unconfirmed_blocks = {}
        while True:
            next_poll_interval = (int(time.time() / checkpoints_period) + 1) * checkpoints_period + 5

            # Get TAPI epochs
            sql = "SELECT start_epoch, stop_epoch, bit FROM tapi"
            tapi_periods = self.confirm_blocks_database.sql_return_all(sql)

            # Fetch all unconfirmed epoch data from the queue
            while not unconfirmed_blocks_queue.empty():
                try:
                    # Fetch data in a non-blocking way so we can catch possible concurrency problems
                    epoch, block_hash = unconfirmed_blocks_queue.get(False)
                    unconfirmed_blocks[epoch] = block_hash
                except Empty:
                    break

            unconfirmed_blocks_str = ", ".join([str(epoch) for epoch in sorted(list(unconfirmed_blocks.keys()))])
            logger.info(f"Epochs which need confirmation: {unconfirmed_blocks_str}")

            # Start confirmation when the first unconfirmed block can be confirmed (2 x the superblock period)
            # Blocks x-20 to x-11 are all confirmed at epoch x
            epochs = sorted(unconfirmed_blocks.keys())
            if len(epochs) > 0 and epochs[0] + superblock_period * 2 <= epochs[-1]:
                blockchain = self.confirm_blocks_node.get_blockchain(epoch=epochs[0])
                if type(blockchain) is dict and "error" in blockchain:
                    logger.warning(f"Unable to fetch recent blocks: {blockchain['error']}")
                    blockchain = {}
                else:
                    blockchain = blockchain["result"]
                    blockchain = {epoch: block for epoch, block in blockchain}

                # If we succesfully fetched the blockchain, check which blocks we can confirm and which need to be deleted and / or replaced
                if blockchain != {}:
                    remove_epochs = []

                    # Check the unconfirmed blocks inserted by another process if they are confirmed by a superblock
                    start_confirm = time.time()
                    for epoch, block_hash_hex_str in sorted(unconfirmed_blocks.items()):
                        # Don't check blocks that are too new
                        if epoch >= epochs[-1] - superblock_period:
                            logger.info(f"Epoch {epoch} is too new to be confirmed")
                            break

                        # Block was removed from the chain (rolled back)
                        if not epoch in blockchain:
                            logger.info(f"Block {block_hash_hex_str} for epoch {epoch} is not part of the chain anymore and will be reverted")
                            self.confirm_blocks_database.revert_block(block_hash_hex_str, epoch)

                            # Track epochs to remove
                            remove_epochs.append(epoch)
                        # There is a different block at this epoch, our node was forked when inserting this block
                        elif epoch in blockchain and blockchain[epoch] != block_hash_hex_str:
                            logger.info(f"Block {block_hash_hex_str} for epoch {epoch} was part of a forked chain")
                            self.confirm_blocks_database.remove_block(block_hash_hex_str, epoch)

                            # Insert correct block
                            logger.info(f"Inserting replacement block {blockchain[epoch]} for epoch {epoch}")
                            block = self.confirm_blocks_node.get_block(blockchain[epoch])
                            # Break and retry fetching the blockchain from this point at the next epoch
                            if type(block) is dict and "error" in block:
                                logger.warning(f"Unable to fetch block {blockchain[epoch]} for epoch {epoch}: {block['error']}")
                                break
                            block = block["result"]
                            self.insert_block(self.confirm_blocks_database, blockchain[epoch], block, epoch, tapi_periods)

                            # Track epochs to remove
                            if "confirmed" in block and block["confirmed"] == True:
                                remove_epochs.append(epoch)
                        # Block is still part of the chain, check if it was confirmed
                        else:
                            # Get block and check possible error conditions
                            block = self.confirm_blocks_node.get_block(block_hash_hex_str)
                            if type(block) is dict and "error" in block:
                                logger.warning(f"Unable to fetch block {block_hash_hex_str} for epoch {epoch}: {block['error']}")
                                # We do not have any (synced) nodes available to query, do not proceed with trying to confirm blocks
                                if "reason" in block and block["reason"] in ("no available nodes found", "no synced nodes found"):
                                    logger.warning("No available or synced nodes found, breaking and retrying confirmations next epoch")
                                # Unknown error when fetching the block
                                else:
                                    logger.error(f"Error for get_block was: {block}")
                                break
                            block = block["result"]

                            # If a block is confirmed, confirm it in the database
                            if "confirmed" in block and block["confirmed"] == True:
                                logger.info(f"Block {block_hash_hex_str} for epoch {epoch} can be confirmed")
                                self.confirm_blocks_database.confirm_block(block_hash_hex_str, epoch)

                                # Track epochs to remove
                                remove_epochs.append(epoch)

                    # Remove the blocks from the unconfirmed tracking dictionary
                    for epoch in remove_epochs:
                        del unconfirmed_blocks[epoch]

                    logger.info(f"Confirming blocks took {time.time() - start_confirm}")

            sleep_for = max(0, next_poll_interval - time.time())
            time.sleep(sleep_for)

    def insert_pending_transactions(self, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "explorer-pending")
        logger = logging.getLogger("explorer-pending")

        # sleep until the next poll interval
        next_poll_interval = (int(time.time() / self.pending_interval) + 1) * self.pending_interval
        sleep_for = max(0, next_poll_interval - time.time())
        time.sleep(sleep_for)

        mapped_data_requests = {}
        mapped_value_transfers = {}

        while True:
            current_time = time.time()
            timestamp = int(current_time / self.pending_interval) * self.pending_interval
            next_poll_interval = (int(current_time / self.pending_interval) + 1) * self.pending_interval

            transactions_pool = self.insert_pending_node.get_mempool()
            # If all nodes are busy retry in short bursts to get the request through
            while "error" in transactions_pool:
                logger.warning(f"Unable to fetch pending transaction: {transactions_pool['error']}")
                if transactions_pool["reason"] == "no synced nodes found":
                    time.sleep(60)
                    transactions_pool = self.insert_pending_node.get_mempool()
                else:
                    time.sleep(1)
                    transactions_pool = self.insert_pending_node.get_mempool()

            transactions_pool = transactions_pool["result"]

            # Something went wrong when fetching the transaction pool, sleep for some time and restart the loop
            if "data_request" not in transactions_pool or "value_transfer" not in transactions_pool:
                sleep_for = max(0, next_poll_interval - time.time())
                time.sleep(sleep_for)
                continue

            logger.info(f"Mempool: {len(transactions_pool['data_request'])} data requests, {len(transactions_pool['value_transfer'])} value transfers")

            data_requests = {}
            mapped_transactions, queried_transactions = 0, 0
            for transaction in transactions_pool["data_request"]:
                if transaction in mapped_data_requests:
                    data_request_fee, data_request_size = mapped_data_requests[transaction]
                    mapped_transactions += 1
                else:
                    data_request = DataRequest(consensus_constants=self.consensus_constants, database_config=self.db_config, node_config=self.node_config, logging_queue = self.logging_queue)
                    data_request.set_transaction(txn_hash=transaction)
                    txn_details = data_request.process_transaction("explorer")

                    queried_transactions += 1

                    if txn_details == {}:
                        continue

                    data_request_fee = txn_details["fee"]
                    data_request_size = txn_details["weight"]

                    mapped_data_requests[transaction] = (data_request_fee, data_request_size)

                # Round the fee to the nearest integer but handle the actual zero-fee transactions and round-to-zero fee transactions differently
                if data_request_fee == 0:
                    witness_fee_per_unit = 0
                elif round(data_request_fee / data_request_size) == 0:
                    witness_fee_per_unit = 1
                else:
                    witness_fee_per_unit = round(data_request_fee / data_request_size)

                # Create a histogram
                if witness_fee_per_unit in data_requests:
                    data_requests[witness_fee_per_unit] += 1
                else:
                    data_requests[witness_fee_per_unit] = 1

                # If less than 3 seconds are left until the next interval, break out of the loop and leave the transactions for the next iteration
                if time.time() + 3 > next_poll_interval:
                    logger.warning("Too many data requests to process, leaving some to process in the next iteration")
                    break

            unprocessed_data_requests = len(transactions_pool["data_request"]) - mapped_transactions - queried_transactions
            logger.info(f"Processed data requests: {mapped_transactions} mapped, {queried_transactions} fetched, {unprocessed_data_requests} left")

            value_transfers = {}
            mapped_transactions, queried_transactions = 0, 0
            for transaction in transactions_pool["value_transfer"]:
                if transaction in mapped_value_transfers:
                    value_transfer_fee, value_transfer_size = mapped_value_transfers[transaction]
                    mapped_transactions += 1
                else:
                    value_transfer = ValueTransfer(consensus_constants=self.consensus_constants, database_config=self.db_config, node_config=self.node_config, logging_queue=self.logging_queue)
                    value_transfer.set_transaction(txn_hash=transaction)
                    txn_details = value_transfer.process_transaction("explorer")

                    queried_transactions += 1

                    if txn_details == {}:
                        continue

                    value_transfer_fee = txn_details["fee"]
                    value_transfer_size = txn_details["weight"]

                    mapped_value_transfers[transaction] = (value_transfer_fee, value_transfer_size)

                # Round the fee to the nearest integer but handle the actual zero-fee transactions and round-to-zero fee transactions differently
                if value_transfer_fee == 0:
                    vtt_fee_per_unit = 0
                elif round(value_transfer_fee / value_transfer_size) == 0:
                    vtt_fee_per_unit = 1
                else:
                    vtt_fee_per_unit = round(value_transfer_fee / value_transfer_size)

                # Create a histogram
                if vtt_fee_per_unit in value_transfers:
                    value_transfers[vtt_fee_per_unit] += 1
                else:
                    value_transfers[vtt_fee_per_unit] = 1

                # If less than 3 seconds are left until the next interval, break out of the loop and leave the transactions for the next iteration
                if time.time() + 3 > next_poll_interval:
                    logger.warning("Too many value transfers to process, leaving some for the next iteration")
                    break

            unprocessed_value_transfers = len(transactions_pool["value_transfer"]) - mapped_transactions - queried_transactions
            logger.info(f"Processed value transfers: {mapped_transactions} mapped, {queried_transactions} fetched, {unprocessed_value_transfers} left")

            if len(data_requests) > 0:
                data_requests_lst = sorted(data_requests.items())
                data_requests_fee_lst = [dr[0] for dr in data_requests_lst]
                data_requests_num_lst = [dr[1] for dr in data_requests_lst]
                self.insert_pending_database.insert_pending_data_request_txns(timestamp, data_requests_fee_lst, data_requests_num_lst)

            if len(value_transfers) > 0:
                value_transfers_lst = sorted(value_transfers.items())
                value_transfers_fee_lst = [vt[0] for vt in value_transfers_lst]
                value_transfers_num_lst = [vt[1] for vt in value_transfers_lst]
                self.insert_pending_database.insert_pending_value_transfer_txns(timestamp, value_transfers_fee_lst, value_transfers_num_lst)

            # Clean the map with already processed transactions
            cleaned_data_requests = 0
            for transaction in list(mapped_data_requests.keys()):
                if transaction not in transactions_pool["data_request"]:
                    cleaned_data_requests += 1
                    del mapped_data_requests[transaction]

            cleaned_value_transfers = 0
            for transaction in list(mapped_value_transfers.keys()):
                if transaction not in transactions_pool["value_transfer"]:
                    cleaned_value_transfers += 1
                    del mapped_value_transfers[transaction]

            logger.info(f"Cleanup of maps: removed {cleaned_data_requests} data requests ({len(mapped_data_requests)} left) and {cleaned_value_transfers} value transfers ({len(mapped_value_transfers)} left)")

            sleep_for = max(0, next_poll_interval - time.time())
            time.sleep(sleep_for)

def select_logging_level(level):
    if level.lower() == "debug":
        return logging.DEBUG
    elif level.lower() == "info":
        return logging.INFO
    elif level.lower() == "warning":
        return logging.WARNING
    elif level.lower() == "error":
        return logging.ERROR
    elif level.lower() == "critical":
        return logging.CRITICAL

def configure_logging_listener(config):
    root = logging.getLogger()

    logging.Formatter.converter = time.gmtime

    # Add header formatting of the log message
    formatter = logging.Formatter("[%(levelname)-8s] [%(asctime)s] [%(name)-16s] %(message)s", datefmt="%Y/%m/%d %H:%M:%S")

    log_file_name = config["explorer"]["log"]["log_file"]
    level_file = select_logging_level(config["explorer"]["log"]["level_file"])
    level_stdout = select_logging_level(config["explorer"]["log"]["level_stdout"])

    # Get log file parts
    dirname = os.path.dirname(log_file_name)
    basename = os.path.basename(log_file_name)
    filename, extension = os.path.splitext(basename)
    # Move the existing log
    if os.path.exists(log_file_name):
        today = datetime.date.today()
        shutil.move(log_file_name, os.path.join(dirname, f"{filename}.{today.strftime('%Y%m%d')}{extension}"))

    # Add file handler
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file_name, when="D", utc=True)
    # Date suffix should not contain dashes
    file_handler.suffix = "%Y%m%d"
    file_handler.extMatch = re.compile(r"^\d{8}$")
    # Put the date timestamp between the filename and the extension
    file_handler.namer = lambda name: os.path.join(os.path.dirname(name), os.path.basename(name).replace(extension, "") + extension)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level_file)
    root.addHandler(file_handler)

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level_stdout)
    root.addHandler(console_handler)

def logging_listener(config, queue):
    configure_logging_listener(config)

    while True:
        try:
            record = queue.get()
            if record == None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except EOFError:
            break
        except KeyboardInterrupt:
            continue

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    # Load config file
    config = toml.load(options.config_file)

    # Start logging process
    logging_queue = Queue()
    listener_process = Process(target=logging_listener, args=(config, logging_queue))
    listener_process.start()

    # Create explorer
    explorer = BlockExplorer(config, logging_queue)

    # Create queue to pass data about unconfirmed blocks
    unconfirmed_blocks_queue = Queue()

    # This process will query the node and fetch all new blocks
    insert_process = Process(target=explorer.insert_blocks_and_transactions, args=(logging_queue, unconfirmed_blocks_queue))
    # This process will query the node and confirm all new blocks
    confirm_process = Process(target=explorer.confirm_blocks_and_transactions, args=(logging_queue, unconfirmed_blocks_queue))
    # This process will query the node and fetch pending transactions from the memory pool
    pending_process = Process(target=explorer.insert_pending_transactions, args=(logging_queue,))

    # Catch ctrl+c
    def signal_handler(*args):
        # Terminate all processes
        insert_process.terminate()
        confirm_process.terminate()
        pending_process.terminate()
        explorer.terminate()

        # End the logging process
        logging_queue.put(None)
        # Sleep 1 second to make sure everything ended
        time.sleep(1)

        listener_process.terminate()
        # Sleep 1 second to make sure everything ended
        time.sleep(1)

        # Exit cleanly
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    insert_process.start()
    confirm_process.start()
    pending_process.start()

if __name__ == "__main__":
    main()
