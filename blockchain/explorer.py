#!/usr/bin/python3

import datetime
import logging
import logging.handlers
import optparse
import os
import re
import shutil
import signal
import sys
import time
from multiprocessing import Process, Queue
from queue import Empty

import toml

from blockchain.objects.block import Block
from blockchain.transactions.data_request import DataRequest
from blockchain.transactions.value_transfer import ValueTransfer
from blockchain.witnet_database import WitnetDatabase
from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode
from util.common_functions import calculate_current_epoch
from util.common_sql import sql_last_confirmed_block
from util.socket_manager import SocketManager


class BlockExplorer(object):
    def __init__(self, config, log_queue):
        error_retry = config["explorer"]["error_retry"]

        self.mempool_interval = config["explorer"]["mempool_interval"]

        # Set up logger
        self.configure_logging_process(log_queue, "explorer")
        self.logger = logging.getLogger("explorer")

        # Set up logging queue for logging from different processes
        self.log_queue = log_queue

        # Get configuration to connect to the node pool
        self.node_config = config["node-pool"]

        # Create nodes to connect to the node pool
        self.insert_blocks_node = WitnetNode(
            self.node_config,
            timeout=30,
            log_queue=self.log_queue,
            log_label="node-insert",
        )
        self.confirm_blocks_node = WitnetNode(
            self.node_config,
            timeout=30,
            log_queue=self.log_queue,
            log_label="node-confirm",
        )
        self.insert_pending_node = WitnetNode(
            self.node_config,
            timeout=30,
            log_queue=self.log_queue,
            log_label="node-pending",
        )

        # Get consensus constants
        self.consensus_constants = ConsensusConstants(
            config=config, error_retry=error_retry
        )

        # Get configuration to connect to the database
        self.database_config = config["database"]

        # Create database objects
        self.insert_blocks_database = WitnetDatabase(
            self.database_config, log_queue=self.log_queue, log_label="db-insert"
        )
        self.confirm_blocks_database = WitnetDatabase(
            self.database_config, log_queue=self.log_queue, log_label="db-confirm"
        )
        self.mempool_database = WitnetDatabase(
            self.database_config, log_queue=self.log_queue, log_label="db-pending"
        )

        # Get configuration to connect to the address caching server
        self.addresses_config = config["api"]["caching"]["scripts"]["addresses"]

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def terminate(self):
        self.insert_blocks_database.terminate()
        self.confirm_blocks_database.terminate()
        self.mempool_database.terminate()
        self.logger.info("Terminating explorer")

    def insert_block(self, database, block_hash_hex_str, block, epoch, tapi_periods):
        # Create block object and parse it to a JSON object
        block = Block(
            self.consensus_constants,
            block_hash=block_hash_hex_str,
            log_queue=self.log_queue,
            database_config=self.database_config,
            block=block,
            tapi_periods=tapi_periods,
            node_config=self.node_config,
        )
        block_json = block.process_block("explorer")

        # Insert block
        database.insert_block(block_json)

        # Insert transactions
        self.insert_transactions(database, block_json, epoch)

        addresses = block.process_addresses()
        database.insert_addresses(addresses)

        # Finalize insertions and updates on every block
        database.finalize(epoch)

        return block_json

    def update_cached_views(self, block_json, logger, caching_server):
        epoch = block_json["details"]["epoch"]

        # Update the blocks mined view for the miner using the caching server
        miner = block_json["transactions"]["mint"]["miner"]
        request = {
            "method": "update",
            "epoch": epoch,
            "function": "blocks",
            "addresses": [miner],
            "id": 1,
        }
        self.try_send_request(logger, caching_server, request)

        # Update the mint transaction cached view for the addresses which received (part of) the mint transaction using the caching server
        mint_addresses = block_json["transactions"]["mint"]["output_addresses"]
        request = {
            "method": "update",
            "epoch": epoch,
            "function": "mints",
            "addresses": mint_addresses,
            "id": 2,
        }
        self.try_send_request(logger, caching_server, request)

        # Update all value transfer cached views for all addresses involved in value transfers
        value_transfer_addresses = set()
        for value_transfer in block_json["transactions"]["value_transfer"]:
            value_transfer_addresses.update(set(value_transfer["input_addresses"]))
            value_transfer_addresses.update(value_transfer["output_addresses"])
        if len(value_transfer_addresses) > 0:
            request = {
                "method": "update",
                "epoch": epoch,
                "function": "value-transfers",
                "addresses": list(value_transfer_addresses),
                "id": 3,
            }
            self.try_send_request(logger, caching_server, request)

        # Update the data requests solved cached view for all addresses in all tallies
        tally_addresses = set()
        for tally in block_json["transactions"]["tally"]:
            tally_addresses.update(tally["output_addresses"])
            tally_addresses.update(tally["error_addresses"])
            tally_addresses.update(tally["liar_addresses"])
        if len(tally_addresses) > 0:
            request = {
                "method": "update",
                "epoch": epoch,
                "function": "data-requests-solved",
                "addresses": list(tally_addresses),
                "id": 4,
            }
            self.try_send_request(logger, caching_server, request)

        # Update the data requests created cached view for all addresses in all data requests
        data_request_addresses = set()
        for data_request in block_json["transactions"]["data_request"]:
            data_request_addresses.update(set(data_request["input_addresses"]))
        if len(data_request_addresses) > 0:
            request = {
                "method": "update",
                "epoch": epoch,
                "function": "data-requests-created",
                "addresses": list(data_request_addresses),
                "id": 5,
            }
            self.try_send_request(logger, caching_server, request)

        # Update the utxos for all addresses which were involved in a UTXO consuming / generating transaction
        utxo_addresses = set()
        utxo_addresses.update(
            set(block_json["transactions"]["mint"]["output_addresses"])
        )
        utxo_addresses.update(value_transfer_addresses)
        utxo_addresses.update(data_request_addresses)
        for commit in block_json["transactions"]["commit"]:
            utxo_addresses.add(commit["address"])
        utxo_addresses.update(tally_addresses)
        if len(utxo_addresses) > 0:
            request = {
                "method": "update",
                "epoch": epoch,
                "function": "utxos",
                "addresses": list(utxo_addresses),
                "id": 6,
            }
            self.try_send_request(logger, caching_server, request)

    def insert_transactions(self, database, block_json, epoch):
        # Insert mint transaction
        database.insert_mint_txn(block_json["transactions"]["mint"], epoch)

        # Insert value transfer transactions
        for txn_details in block_json["transactions"]["value_transfer"]:
            database.insert_value_transfer_txn(txn_details, epoch)

        # Insert data request transactions
        for txn_details in block_json["transactions"]["data_request"]:
            database.insert_data_request_txn(txn_details, epoch)

        # Insert commit transactions
        for txn_details in block_json["transactions"]["commit"]:
            database.insert_commit_txn(txn_details, epoch)

        # Insert reveal transactions
        for txn_details in block_json["transactions"]["reveal"]:
            database.insert_reveal_txn(txn_details, epoch)

        # Insert tally transactions
        for txn_details in block_json["transactions"]["tally"]:
            database.insert_tally_txn(txn_details, epoch)

    def insert_blocks_and_transactions(self, log_queue, unconfirmed_blocks_queue):
        # Set up logger
        self.configure_logging_process(log_queue, "explorer-insert")
        logger = logging.getLogger("explorer-insert")

        # Get some consensus constants
        checkpoints_period = self.consensus_constants.checkpoints_period

        # Connect to the addresses caching server
        caching_server = SocketManager(
            self.addresses_config["host"],
            self.addresses_config["port"],
            self.addresses_config["default_timeout"],
        )

        logger.info("Querying database for last confirmed block")
        # Get the last block we inserted into the database
        data = self.insert_blocks_database.sql_return_one(sql_last_confirmed_block)
        if not data:
            last_block_hash, last_epoch = "", -1
            logger.info("Last confirmed block was at epoch 0")
        else:
            last_block_hash = data[0].hex()
            last_epoch = data[1]
            logger.info(
                f"Last confirmed block was {last_block_hash} at epoch {last_epoch}"
            )
        # If we are adding the first block, initialize last_block_hash with the bootstrap_hash
        if last_block_hash == "":
            last_block_hash = self.consensus_constants.bootstrap_hash

        # sleep until the next poll interval
        next_poll_interval = (
            int(time.time() / checkpoints_period) + 1
        ) * checkpoints_period + 1
        sleep_for = max(0, next_poll_interval - time.time())
        logger.info(f"Waiting {int(sleep_for)}s until the start of the next epoch")
        time.sleep(sleep_for)

        # Infinite loop
        while True:
            next_poll_interval = (
                int(time.time() / checkpoints_period) + 1
            ) * checkpoints_period + 1

            # Get TAPI periods
            tapi_periods = self.get_tapi_periods(self.insert_blocks_database)

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
                    logger.warning(
                        f"Unable to fetch block {block_hash_hex_str}: {block['error']}"
                    )
                    break
                block = block["result"]

                # Insert block
                block_json = self.insert_block(
                    self.insert_blocks_database,
                    block_hash_hex_str,
                    block,
                    epoch,
                    tapi_periods,
                )

                # Update all cached views
                self.update_cached_views(block_json, logger, caching_server)

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

    def confirm_blocks_and_transactions(self, log_queue, unconfirmed_blocks_queue):
        # Set up logger
        self.configure_logging_process(log_queue, "explorer-confirm")
        logger = logging.getLogger("explorer-confirm")

        # Calculate superepoch period from consensus constants
        superblock_period = self.consensus_constants.superblock_period
        checkpoints_period = self.consensus_constants.checkpoints_period

        # Connect to the addresses caching server
        caching_server = SocketManager(
            self.addresses_config["host"],
            self.addresses_config["port"],
            self.addresses_config["default_timeout"],
        )

        # sleep until the next poll interval
        next_poll_interval = (
            int(time.time() / checkpoints_period) + 1
        ) * checkpoints_period + 5
        sleep_for = max(0, next_poll_interval - time.time())
        time.sleep(sleep_for)

        unconfirmed_blocks = {}
        while True:
            next_poll_interval = (
                int(time.time() / checkpoints_period) + 1
            ) * checkpoints_period + 5

            # Get TAPI epochs
            tapi_periods = self.get_tapi_periods(self.confirm_blocks_database)

            # Fetch all unconfirmed epoch data from the queue
            while not unconfirmed_blocks_queue.empty():
                try:
                    # Fetch data in a non-blocking way so we can catch possible concurrency problems
                    epoch, block_hash = unconfirmed_blocks_queue.get(False)
                    unconfirmed_blocks[epoch] = block_hash
                except Empty:
                    break

            unconfirmed_blocks_str = ", ".join(
                [str(epoch) for epoch in sorted(list(unconfirmed_blocks.keys()))]
            )
            logger.info(f"Epochs which need confirmation: {unconfirmed_blocks_str}")

            # Start confirmation when the first unconfirmed block can be confirmed (2 x the superblock period)
            # Blocks x-20 to x-11 are all confirmed at epoch x
            epochs = sorted(unconfirmed_blocks.keys())
            if len(epochs) > 0 and epochs[0] + superblock_period * 2 <= epochs[-1]:
                blockchain = self.confirm_blocks_node.get_blockchain(epoch=epochs[0])
                if type(blockchain) is dict and "error" in blockchain:
                    logger.warning(
                        f"Unable to fetch recent blocks: {blockchain['error']}"
                    )
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
                        if epoch not in blockchain:
                            logger.info(
                                f"Block {block_hash_hex_str} for epoch {epoch} is not part of the chain anymore and will be reverted"
                            )
                            self.confirm_blocks_database.revert_block(
                                block_hash_hex_str, epoch
                            )

                            # Update cached views
                            request = {"method": "revert", "epoch": epoch, "id": 1}
                            self.try_send_request(logger, caching_server, request)

                            # Track epochs to remove
                            remove_epochs.append(epoch)
                        # There is a different block at this epoch, our node was forked when inserting this block
                        elif (
                            epoch in blockchain
                            and blockchain[epoch] != block_hash_hex_str
                        ):
                            logger.info(
                                f"Block {block_hash_hex_str} for epoch {epoch} was part of a forked chain"
                            )
                            self.confirm_blocks_database.remove_block(
                                block_hash_hex_str, epoch
                            )

                            # Insert correct block
                            logger.info(
                                f"Inserting replacement block {blockchain[epoch]} for epoch {epoch}"
                            )
                            block = self.confirm_blocks_node.get_block(
                                blockchain[epoch]
                            )
                            # Break and retry fetching the blockchain from this point at the next epoch
                            if type(block) is dict and "error" in block:
                                logger.warning(
                                    f"Unable to fetch block {blockchain[epoch]} for epoch {epoch}: {block['error']}"
                                )
                                break
                            block = block["result"]
                            block_json = self.insert_block(
                                self.confirm_blocks_database,
                                blockchain[epoch],
                                block,
                                epoch,
                                tapi_periods,
                            )

                            # Update all cached views
                            request = {"method": "revert", "epoch": epoch, "id": 1}
                            self.try_send_request(logger, caching_server, request)
                            self.update_cached_views(block_json, logger, caching_server)

                            # Track epochs to remove
                            if "confirmed" in block and block["confirmed"]:
                                remove_epochs.append(epoch)
                        # Block is still part of the chain, check if it was confirmed
                        else:
                            # Get block and check possible error conditions
                            block = self.confirm_blocks_node.get_block(
                                block_hash_hex_str
                            )
                            if type(block) is dict and "error" in block:
                                logger.warning(
                                    f"Unable to fetch block {block_hash_hex_str} for epoch {epoch}: {block['error']}"
                                )
                                # We do not have any (synced) nodes available to query, do not proceed with trying to confirm blocks
                                if "reason" in block and block["reason"] in (
                                    "no available nodes found",
                                    "no synced nodes found",
                                ):
                                    logger.warning(
                                        "No available or synced nodes found, breaking and retrying confirmations next epoch"
                                    )
                                # Unknown error when fetching the block
                                else:
                                    logger.error(f"Error for get_block was: {block}")
                                break
                            block = block["result"]

                            # If a block is confirmed, confirm it in the database
                            if "confirmed" in block and block["confirmed"]:
                                logger.info(
                                    f"Block {block_hash_hex_str} for epoch {epoch} can be confirmed"
                                )
                                self.confirm_blocks_database.confirm_block(
                                    block_hash_hex_str, epoch
                                )

                                # Update cached views
                                request = {"method": "confirm", "epoch": epoch, "id": 1}
                                self.try_send_request(logger, caching_server, request)

                                # Track epochs to remove
                                remove_epochs.append(epoch)

                    # Remove the blocks from the unconfirmed tracking dictionary
                    for epoch in remove_epochs:
                        del unconfirmed_blocks[epoch]

                    logger.info(f"Confirming blocks took {time.time() - start_confirm}")

            sleep_for = max(0, next_poll_interval - time.time())
            time.sleep(sleep_for)

    def insert_mempool_transactions(self, log_queue):
        # Set up logger
        self.configure_logging_process(log_queue, "explorer-pending")
        logger = logging.getLogger("explorer-pending")

        # sleep until the next poll interval
        next_poll_interval = (
            int(time.time() / self.mempool_interval) + 1
        ) * self.mempool_interval
        sleep_for = max(0, next_poll_interval - time.time())
        time.sleep(sleep_for)

        mapped_data_requests = {}
        mapped_value_transfers = {}

        while True:
            current_time = time.time()
            timestamp = (
                int(current_time / self.mempool_interval) * self.mempool_interval
            )
            next_poll_interval = (
                int(current_time / self.mempool_interval) + 1
            ) * self.mempool_interval

            current_epoch = self.insert_pending_node.get_current_epoch()
            if current_epoch == 0:
                current_epoch = calculate_current_epoch(
                    self.consensus_constants.checkpoint_zero_timestamp,
                    self.consensus_constants.checkpoints_period,
                )

            transactions_pool = self.insert_pending_node.get_mempool()
            # If all nodes are busy retry in short bursts to get the request through
            while "error" in transactions_pool:
                logger.warning(
                    f"Unable to fetch pending transaction: {transactions_pool['error']}"
                )
                if transactions_pool["reason"] == "no synced nodes found":
                    time.sleep(60)
                    transactions_pool = self.insert_pending_node.get_mempool()
                else:
                    time.sleep(1)
                    transactions_pool = self.insert_pending_node.get_mempool()

            transactions_pool = transactions_pool["result"]

            # Something went wrong when fetching the transaction pool, sleep for some time and restart the loop
            if (
                "data_request" not in transactions_pool
                or "value_transfer" not in transactions_pool
            ):
                sleep_for = max(0, next_poll_interval - time.time())
                time.sleep(sleep_for)
                continue

            logger.info(
                f"Mempool: {len(transactions_pool['data_request'])} data requests, {len(transactions_pool['value_transfer'])} value transfers"
            )

            mapped_transactions, queried_transactions = 0, 0
            data_request = DataRequest(
                self.consensus_constants,
                logger=logger,
                database_config=self.database_config,
                node_config=self.node_config,
            )
            for transaction in transactions_pool["data_request"]:
                if transaction in mapped_data_requests:
                    data_request_fee, data_request_size = mapped_data_requests[
                        transaction
                    ]
                    mapped_transactions += 1
                else:
                    try:
                        data_request.set_transaction(transaction, current_epoch)
                        txn_details = data_request.process_transaction("explorer")
                    except ValueError:
                        continue

                    queried_transactions += 1

                    mapped_data_requests[transaction] = (
                        txn_details["miner_fee"],
                        txn_details["weight"],
                    )

                # If less than 3 seconds are left until the next interval, break out of the loop and leave the remaining transactions for the next iteration
                if time.time() + 3 > next_poll_interval:
                    logger.warning(
                        "Too many data requests to process, leaving some to process in the next iteration"
                    )
                    break

            unprocessed_data_requests = (
                len(transactions_pool["data_request"])
                - mapped_transactions
                - queried_transactions
            )
            logger.info(
                f"Processed data requests: {mapped_transactions} mapped, {queried_transactions} fetched, {unprocessed_data_requests} left"
            )

            mapped_transactions, queried_transactions = 0, 0
            value_transfer = ValueTransfer(
                self.consensus_constants,
                logger=logger,
                database_config=self.database_config,
                node_config=self.node_config,
            )
            for transaction in transactions_pool["value_transfer"]:
                if transaction in mapped_value_transfers:
                    value_transfer_fee, value_transfer_size = mapped_value_transfers[
                        transaction
                    ]
                    mapped_transactions += 1
                else:
                    try:
                        value_transfer.set_transaction(transaction, current_epoch)
                        txn_details = value_transfer.process_transaction("explorer")
                    except ValueError:
                        continue

                    queried_transactions += 1

                    mapped_value_transfers[transaction] = (
                        txn_details["fee"],
                        txn_details["weight"],
                    )

                # If less than 3 seconds are left until the next interval, break out of the loop and leave the remaining transactions for the next iteration
                if time.time() + 3 > next_poll_interval:
                    logger.warning(
                        "Too many value transfers to process, leaving some for the next iteration"
                    )
                    break

            unprocessed_value_transfers = (
                len(transactions_pool["value_transfer"])
                - mapped_transactions
                - queried_transactions
            )
            logger.info(
                f"Processed value transfers: {mapped_transactions} mapped, {queried_transactions} fetched, {unprocessed_value_transfers} left"
            )

            if len(mapped_data_requests) > 0:
                data_requests_fee = [dr[0] for dr in mapped_data_requests.values()]
                data_requests_weight = [dr[1] for dr in mapped_data_requests.values()]
                self.mempool_database.insert_mempool_data_requests(
                    timestamp, data_requests_fee, data_requests_weight
                )

            if len(mapped_value_transfers) > 0:
                value_transfers_fee = [vt[0] for vt in mapped_value_transfers.values()]
                value_transfers_weight = [
                    vt[1] for vt in mapped_value_transfers.values()
                ]
                self.mempool_database.insert_mempool_value_transfers(
                    timestamp, value_transfers_fee, value_transfers_weight
                )

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

            logger.info(
                f"Cleanup of maps: removed {cleaned_data_requests} data requests ({len(mapped_data_requests)} left) and {cleaned_value_transfers} value transfers ({len(mapped_value_transfers)} left)"
            )

            sleep_for = max(0, next_poll_interval - time.time())
            time.sleep(sleep_for)

    def try_send_request(self, logger, caching_server, request):
        try:
            caching_server.send_request(request)
        except ConnectionRefusedError:
            logger.warning(
                f"Could not send {request['method']} request to address caching server"
            )
            try:
                caching_server.recreate_socket()
                caching_server.send_request(request)
            except ConnectionRefusedError:
                logger.warning(
                    f"Could not recreate socket, will try again next {request['method']} request"
                )

    def get_tapi_periods(self, database):
        # Get TAPI periods
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
        return database.sql_return_all(sql)


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
    formatter = logging.Formatter(
        "[%(levelname)-8s] [%(asctime)s] [%(name)-16s] %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

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
        shutil.move(
            log_file_name,
            os.path.join(dirname, f"{filename}.{today.strftime('%Y%m%d')}{extension}"),
        )

    # Add file handler
    file_handler = logging.handlers.TimedRotatingFileHandler(
        log_file_name, when="D", utc=True
    )
    # Date suffix should not contain dashes
    file_handler.suffix = "%Y%m%d"
    file_handler.extMatch = re.compile(r"^\d{8}$")
    # Put the date timestamp between the filename and the extension
    file_handler.namer = lambda name: os.path.join(
        os.path.dirname(name), os.path.basename(name).replace(extension, "") + extension
    )
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
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except EOFError:
            break
        except KeyboardInterrupt:
            continue


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "--config-file", type="string", default="explorer.toml", dest="config_file"
    )
    options, args = parser.parse_args()

    # Load config file
    config = toml.load(options.config_file)

    # Start logging process
    log_queue = Queue()
    listener_process = Process(target=logging_listener, args=(config, log_queue))
    listener_process.start()

    # Create explorer
    explorer = BlockExplorer(config, log_queue)

    # Create queue to pass data about unconfirmed blocks
    unconfirmed_blocks_queue = Queue()

    # This process will query the node and fetch all new blocks
    insert_process = Process(
        target=explorer.insert_blocks_and_transactions,
        args=(log_queue, unconfirmed_blocks_queue),
    )
    # This process will query the node and confirm all new blocks
    confirm_process = Process(
        target=explorer.confirm_blocks_and_transactions,
        args=(log_queue, unconfirmed_blocks_queue),
    )
    # This process will query the node and fetch pending transactions from the memory pool
    pending_process = Process(
        target=explorer.insert_mempool_transactions, args=(log_queue,)
    )

    # Catch ctrl+c
    def signal_handler(*args):
        # Terminate all processes
        insert_process.terminate()
        confirm_process.terminate()
        pending_process.terminate()
        explorer.terminate()

        # End the logging process
        log_queue.put(None)
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
