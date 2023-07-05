import base64
import io
import logging
import logging.handlers
import os
import pylibmc
import psycopg2
import time
import toml

from flask import send_file

from PIL import Image

from app.cache import cache

from blockchain.witnet_database import WitnetDatabase

from caching.network_stats import aggregate_nodes

from engine.trs import TRS

from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode

from objects.address import Address
from objects.block import Block
from objects.blockchain import Blockchain
from objects.data_request_history import DataRequestHistory
from objects.data_request_report import DataRequestReport
from objects.transaction_pool import TransactionPool

from transactions.data_request import DataRequest
from transactions.commit import Commit
from transactions.tally import Tally
from transactions.mint import Mint
from transactions.value_transfer import ValueTransfer

from util.helper_functions import sanitize_address
from util.helper_functions import sanitize_input
from util.memcached import calculate_timeout
from util.socket_manager import SocketManager

class NodeManager(object):
    def __init__(self, config, log_queue):
        error_retry = config["api"]["error_retry"]

        if config["api"]["cache_server"] == "memcached":
            self.cache_config = config["api"]["caching"]
        else:
            assert False, "Need to specify a caching instance"

        self.plot_dir = self.cache_config["plot_directory"]

        # Set up logger
        self.log_queue = log_queue
        self.configure_logging_process(self.log_queue, "node-manager")
        self.logger = logging.getLogger("node-manager")

        # Save full configuration
        self.config = config

        # Get configuration to connect to the node pool
        self.node_config = config["node-pool"]

        # Create witnet node
        self.witnet_node = WitnetNode(self.node_config, log_queue=self.log_queue, log_label="node-api")

        # Get consensus constants
        self.consensus_constants = ConsensusConstants(self.node_config, error_retry=error_retry, log_queue=self.log_queue, log_label="node-consensus")

        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period

        # Get configuration to connect to the database
        self.database_config = config["database"]

        # Connect to database
        self.witnet_database = WitnetDatabase(self.database_config, log_queue=self.log_queue, log_label="db-api")

        # Create a couple of objects once
        self.blockchain = Blockchain(self.database_config, self.node_config, self.consensus_constants, self.log_queue)
        self.transaction_pool = TransactionPool(self.database_config, self.log_queue)

        self.TRS = TRS(config["engine"]["json_file"], False, db_config=self.database_config, logger=self.logger)

        config = self.config["api"]["caching"]["scripts"]["addresses"]
        self.address_caching_server = SocketManager(config["host"], config["port"], config["default_timeout"])

    ########################
    #   Helper functions   #
    ########################

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def is_transaction_pending(self, hash_value):
        transactions_pool = self.witnet_node.get_mempool()
        if "error" in transactions_pool:
            return {
                "txn_hash": hash_value,
                "status": "error",
                "message": "Sorry, I could not fetch the pending transactions",
                "txn_time": 0,
                "block_hash": "",
            }
        transactions_pool = transactions_pool["result"]

        message = ""
        if hash_value in transactions_pool["data_request"]:
            status = "pending"
            txn_type = "data request transaction"
        elif hash_value in transactions_pool["value_transfer"]:
            status = "pending"
            txn_type = "value transfer transaction"
        else:
            status = "error"
            txn_type = "unknown"
            message = "Sorry, I could not find this transaction hash"

        return {
            "txn_hash": hash_value,
            "status": status,
            "type": txn_type,
            "message": message,
            "txn_time": 0,
            "block_hash": "",
        }

    def pretty_hash_type(self, hash_type):
        return hash_type.replace("_", " ").replace("txn", "transaction")

    def get_ids_to_addresses(self):
        sql = """
            SELECT
                address,
                id
            FROM
                addresses
        """
        addresses = self.witnet_database.sql_return_all(sql)

        # Transform list of data to dictionary
        ids_to_addresses = {}
        if addresses:
            for address, address_id in addresses:
                ids_to_addresses[address_id] = address

        return ids_to_addresses

    def translate_address_ids(self, id_value):
        # Fetch an id to address mapping
        ids_to_addresses = self.get_ids_to_addresses()

        # Translate address ids to addresses
        top_100_mapped = []
        for address_id, value in id_value:
            if address_id not in ids_to_addresses:
                self.logger.warning(f"Could not find address value for id {address_id}")
                top_100_mapped.append([address_id, value])
            else:
                top_100_mapped.append([ids_to_addresses[address_id], value])

        return top_100_mapped

    def try_send_request(self, request):
        try:
            self.address_caching_server.send_request(request)
        except ConnectionRefusedError:
            self.logger.warning(f"Could not send {request['method']} request to address caching server")
            try:
                self.address_caching_server.recreate_socket()
                self.address_caching_server.send_request(request)
            except ConnectionRefusedError:
                self.logger.warning(f"Could not recreate socket, will try again next {request['method']} request")

    #######################################################
    #   API endpoint functions which can employ caching   #
    #######################################################

    def get_hash(self, hash_value, simple, start, stop, amount):
        self.logger.info(f"get_hash({hash_value}, {simple}, {start}, {stop}, {amount})")

        if hash_value.startswith("0x"):
            self.logger.warning(f"Invalid value for hash: {hash_value}")
            return {"status": "error", "message": "A hexadecimal transaction hash should not start with 0x"}

        if len(hash_value) != 64:
            self.logger.warning(f"Invalid hash length ({len(hash_value)}): {hash_value}")
            return {"status": "error", "message": "Incorrect hexadecimal transaction hash length"}

        if not sanitize_input(hash_value, "hexadecimal"):
            self.logger.warning(f"Invalid value for hash: {hash_value}")
            return {"status": "error", "message": "Transaction hash is not a hexadecimal value"}

        if not sanitize_input(simple, "bool"):
            self.logger.warning(f"Invalid value for simple: {simple}")
            return {"status": "error", "message": "Value supplied for the simple parameter is not a boolean"}

        if amount not in range(1, 1000):
            self.logger.warning(f"Value of amount is bigger than 1000")
            return {"status": "error", "message": "Value of the amount parameter should be smaller than 1000"}

        hashed_item = cache.get(hash_value)
        if hashed_item:
            self.logger.info(f"Found a {self.pretty_hash_type(hashed_item['type'])} with hash '{hash_value}' in memcached cache")
            return hashed_item

        sql = "SELECT type FROM hashes WHERE hash=%s" % psycopg2.Binary(bytearray.fromhex(hash_value))
        result = self.witnet_database.sql_return_one(sql)
        if result:
            hash_type = result[0]
        # Check if the transaction is in the mempool and pending execution
        else:
            return self.is_transaction_pending(hash_value)

        self.logger.info(f"Could not find {self.pretty_hash_type(hash_type)} with hash '{hash_value}' in memcached cache")

        if hash_type == "block":
            # Fetch block from a node
            block = Block(self.consensus_constants, block_hash=hash_value, log_queue=self.log_queue, database_config=self.database_config, node_config=self.node_config)
            json_block = block.process_block("api")
            if json_block["details"]["confirmed"]:
                try:
                    # First cache the block with its hash as a key
                    cache.set(hash_value, json_block, timeout=calculate_timeout(self.cache_config["scripts"]["blocks"]["timeout"]))
                    # Second cache the block hash with the block epoch as key
                    block_epoch = json_block["details"]["epoch"]
                    cache.set(block_epoch, hash_value, timeout=calculate_timeout(self.cache_config["scripts"]["blocks"]["timeout"]))
                    self.logger.info(f"Added block with hash '{hash_value}' to the memcached cache")
                except pylibmc.TooBig as e:
                    self.logger.warning(f"Could not save block '{hash_value}' in the memcached instance because its size exceeded 1MB")
            else:
                self.logger.info(f"Did not add unconfirmed block with hash '{hash_value}' to the memcached cache")
            return json_block

        if hash_type == "mint_txn":
            # Create mint transaction and get the details from the database
            mint = Mint(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config)
            mint_txn = mint.get_transaction_from_database(hash_value)
            if mint_txn["confirmed"]:
                self.logger.info(f"Added mint transaction with hash '{hash_value}' to the memcached cache")
                cache.set(hash_value, mint_txn, timeout=calculate_timeout(self.cache_config["views"]["hash"]["timeout"]))
            else:
                self.logger.info(f"Did not add unconfirmed mint transaction with hash '{hash_value}' to the memcached cache")
            return mint_txn

        if hash_type == "value_transfer_txn":
            # Create value transfer transaction and get the details from the database
            value_transfer = ValueTransfer(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config)
            value_transfer_txn = value_transfer.get_transaction_from_database(hash_value)
            if value_transfer_txn["confirmed"]:
                self.logger.info(f"Added value transfer transaction with hash '{hash_value}' to the memcached cache")
                cache.set(hash_value, value_transfer_txn, timeout=calculate_timeout(self.cache_config["views"]["hash"]["timeout"]))
            else:
                self.logger.info(f"Did not add unconfirmed value transfer transaction with hash '{hash_value}' to the memcached cache")
            return value_transfer_txn

        if hash_type in ("data_request_txn", "commit_txn", "reveal_txn", "tally_txn"):
            # Only return a single transaction, don't build a DataRequestReport
            if simple:
                # Caching simple transactions probably does not make sense
                # This endpoint is currently only used by the light wallet to fetch UTXO data, which is a once-only fetch
                if hash_type == "data_request_txn":
                    data_request = DataRequest(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config, node_config=self.node_config)
                    transaction = data_request.get_transaction_from_database(hash_value)
                elif hash_type == "commit_txn":
                    commit = Commit(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config, node_config=self.node_config)
                    transaction = commit.get_transaction_from_database(hash_value)
                elif hash_type == "tally_txn":
                    tally = Tally(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config, node_config=self.node_config)
                    transaction = tally.get_transaction_from_database(hash_value)
                return transaction
            # Create data request report for this hash
            else:
                data_request = DataRequestReport(hash_type, hash_value, self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config)

                # If the hash type is a commit, reveal or tally transaction, get the matching data request hash
                # Cached data request reports for a data request hash lookup will already have been returned
                data_request_report = None
                data_request_hash = hash_value
                if hash_type in ("commit_txn", "reveal_txn", "tally_txn"):
                    data_request_hash = data_request.get_data_request_hash()
                    data_request_report = cache.get(data_request_hash)

                if not data_request_report:
                    data_request_report = data_request.get_report()
                    # Only cache data request reports with a confirmed tally transaction
                    if data_request_report["tally_txn"] and data_request_report["tally_txn"]["confirmed"]:
                        self.logger.info(f"Added data request report with hash '{data_request_hash}' to the memcached cache")
                        # Cache data request report based on the data request hash
                        cache.set(data_request_hash, data_request_report, timeout=calculate_timeout(self.cache_config["scripts"]["data_request_reports"]["timeout"]))
                    else:
                        if not data_request_report["tally_txn"]:
                            self.logger.info(f"Did not add incomplete data request report with hash '{data_request_hash}' to the memcached cache")
                        else:
                            self.logger.info(f"Did not add unconfirmed data request report with hash '{data_request_hash}' to the memcached cache")
                else:
                    self.logger.info(f"Found a data request report for a {self.pretty_hash_type(hash_type)} with data request hash '{data_request_hash}' in memcached cache")
                return data_request_report

        if hash_type in ("RAD_bytes_hash", "DRO_bytes_hash"):
            # Create data request history
            data_request_history = DataRequestHistory(self.consensus_constants, self.log_queue, self.database_config)
            # Data request histories change constantly, so we cannot employ simple hash-based caching only
            return data_request_history.get_history(hash_type, hash_value, start, stop, amount)

        return {"status": "error", "message": "Unknown hash type"}

    def get_epoch(self, epoch):
        self.logger.info(f"get_epoch({epoch})")

        if len(str(epoch)) > 8:
            self.logger.warning(f"Invalid epoch length ({len(str(epoch))})")
            return {"error": "invalid epoch length"}

        if not sanitize_input(str(epoch), "numeric"):
            self.logger.warning(f"Invalid value for epoch: {epoch}")
            return {"error": "argument is not a numeric value"}

        if self.witnet_database.get_last_block(confirmed=False)[1] + 1920 < epoch:
            return {
                "type": "RnV0dXJlIGVwb2No",
                "value": "SXQgaXMgcnVtb3VyZWQgaGUgV2l0bmV0IG9yYWNsZSBpcyBzbyBhZHZhbmNlZCBpdCBjYW4gbG9vayBpbnRvIHRoZSBmdXR1cmUuIFNhZGx5IHRoaXMgZXhwbG9yZXIgY2Fubm90Lg==",
            }

        # Epoch cached items return block hash
        cached_block_hash = cache.get(epoch)
        if cached_block_hash:
            # Fetch the actual block based on the block hash
            cached_block = cache.get(cached_block_hash)
            if cached_block:
                self.logger.info(f"Found block {epoch} with hash {cached_block_hash} in memcached cache")
                return cached_block

        # Fetch block from a node
        block = Block(self.consensus_constants, block_epoch=epoch, log_queue=self.log_queue, database_config=self.database_config, node_config=self.node_config)
        json_block = block.process_block("api")
        if "error" in json_block:
            return json_block

        # Attempt to cache the block
        block_hash = json_block["details"]["block_hash"]
        if json_block["details"]["confirmed"]:
            try:
                # First, cache the actual block with the hash as key
                cache.set(block_hash, json_block, timeout=calculate_timeout(self.cache_config["scripts"]["blocks"]["timeout"]))
                # Second, cache the block hash with the block epoch as key
                cache.set(epoch, block_hash, timeout=calculate_timeout(self.cache_config["scripts"]["blocks"]["timeout"]))
                self.logger.info(f"Added block {epoch} with hash {block_hash} to the memcached cache")
            except pylibmc.TooBig as e:
                self.logger.warning(f"Could not save block {epoch} with hash {block_hash} in the memcached instance because its size exceeded 1MB")
        else:
            self.logger.info(f"Did not add unconfirmed block {epoch} with hash {block_hash} to the memcached cache")

        return json_block

    def get_home(self, key):
        self.logger.info(f"get_home({key})")

        if key not in (
            "full",
            "blocks_minted",
            "blocks_minted_reward",
            "blocks_missing",
            "blocks_missing_reward",
            "current_locked_supply",
            "current_time",
            "current_unlocked_supply",
            "epoch",
            "in_flight_requests",
            "locked_wits_by_requests",
            "maximum_supply",
            "current_supply",
            "total_supply",
            "supply_burned_lies",
        ):
            return {"error": "invalid key for home API endpoint"}

        cache_key = f"home_{key}"
        home = cache.get(cache_key)
        if not home:
            self.logger.error(f"Could not find '{cache_key}' in memcached cache")
        else:
            self.logger.info(f"Found '{cache_key}' in memcached cache")

        return home

    def get_blockchain(self, action, block):
        self.logger.info(f"get_blockchain({action}, {block})")

        if action not in ("init", "append", "prepend"):
            self.logger.warning(f"Invalid value for action: {action}")
            return {"error": "invalid action, valid actions are 'init', 'append', 'prepend'"}
        # integers are sanitized already by Flask argument parsing

        if action == "init":
            cache_key = f"blockchain_init_{block}"
            blockchain_init = cache.get(cache_key)
            if not blockchain_init:
                self.logger.info(f"Could not find '{cache_key}' in memcached cache")
                blockchain_init = self.blockchain.get_blockchain_details(action, block, -1, -1)
                cache.set(cache_key, blockchain_init, timeout=calculate_timeout(self.cache_config["views"]["blockchain"]["timeout"]))
            else:
                self.logger.info(f"Found '{cache_key}' in memcached cache")
            return blockchain_init

        if action == "append":
            if block >= 0:
                num, start, stop = 0, block, -1
            else:
                num, start, stop = block, -1, -1
            cache_key = f"blockchain_append_{block}"
            blockchain_append = cache.get(cache_key)
            if not blockchain_append:
                self.logger.info(f"Could not find '{cache_key}' in memcached cache")
                blockchain_append = self.blockchain.get_blockchain_details(action, num, start, stop)
                cache.set(cache_key, blockchain_append, timeout=calculate_timeout(self.cache_config["views"]["blockchain"]["timeout"]))
            else:
                self.logger.info(f"Found '{cache_key}' in memcached cache")
            return blockchain_append

        if action == "prepend":
            start = max(0, block - 50)
            cache_key = f"blockchain_append_{start}"
            blockchain_prepend = cache.get(cache_key)
            if not blockchain_prepend:
                self.logger.info(f"Could not find '{cache_key}' in memcached cache")
                blockchain_prepend = self.blockchain.get_blockchain_details(action, 0, start, block)
                cache.set(cache_key, blockchain_prepend, timeout=calculate_timeout(self.cache_config["views"]["blockchain"]["timeout"]))
            else:
                self.logger.info(f"Found '{cache_key}' in memcached cache")
            return blockchain_prepend

    def get_reputation_list(self, epoch):
        self.logger.info(f"get_reputation_list({epoch})")

        if epoch == "":
            reputation = cache.get(f"reputation")
            if not reputation:
                self.logger.error(f"Could not find 'reputation' in memcached cache")
            else:
                self.logger.info(f"Found 'reputation' in memcached cache")
        else:
            if not sanitize_input(epoch, "numeric"):
                self.logger.warning(f"Invalid value for epoch: {epoch}")
                return {"error": "epoch is not a numerical value"}

            epoch = int(epoch)
            found_epoch, trs, total_reputation = self.TRS.get_trs(epoch)

            reputation = {
                "reputation": trs,
                "total_reputation": total_reputation,
                "last_updated": self.start_time + (epoch + 1) * self.epoch_period
            }

            self.logger.info(f"Found historical TRS for epoch {epoch} at epoch {found_epoch}")

        return reputation

    def get_balance_list(self, start, stop):
        self.logger.info(f"get_balance_list({start}, {stop})")

        # integer arguments are sanitized already by Flask argument parsing
        if stop - start > 1000:
            return {"error": "cannot fetch more than 1000 entries from the balance list at once"}

        if start % 1000 != 0 or stop % 1000 != 0:
            return {"error": "start and stop parameters should be multiples of thousands"}

        balance_list_part = cache.get(f"balance-list_{start}-{stop}")
        balances_sum = cache.get(f"balance-list_sum")
        last_updated = cache.get(f"balance-list_updated")
        if not balance_list_part:
            self.logger.error(f"Could not find 'balance-list_{start}_{stop}' in memcached cache")
            balance_list = {
                "balance_list": [],
                "balances_sum": 0,
                "last_updated": 0,
            }
        else:
            self.logger.info(f"Found 'balance-list_{start}_{stop}' in memcached cache")
            balance_list = {
                "balance_list": balance_list_part,
                "balances_sum": balances_sum if balances_sum else 0,
                "last_updated": last_updated if last_updated else 0,
            }

        return balance_list

    def get_network(self, key, start_epoch=None, stop_epoch=None):
        self.logger.info(f"get_network({key}, {start_epoch}, {stop_epoch})")

        key_mapping = {
            "list-rollbacks": "network_list-rollbacks",
            "num-unique-miners": "network_miners",
            "num-unique-data-request-solvers": "network_data-request-solvers",
            "top-100-miners": "network_miners",
            "top-100-data-request-solvers": "network_data-request-solvers",
            "percentile-staking-balances": "network_percentile-staking-balances",
            "histogram-data-requests": "network_data-requests",
            "histogram-data-request-composition": "network_data-requests",
            "histogram-data-request-witness": "network_data-requests",
            "histogram-data-request-lie-rate": "network_lie-rates",
            "histogram-supply-burn-rate": "network_burn-rate",
            "histogram-data-request-collateral": "network_data-requests",
            "histogram-data-request-reward": "network_data-requests",
            "histogram-trs-data": "network_trs-data",
            "histogram-value-transfers": "network_value-transfers",
        }

        last_updated_timestamp = cache.get("network_last-updated")
        if not last_updated_timestamp:
            last_updated_timestamp = 0

        aggregation_epochs = self.config["api"]["caching"]["scripts"]["network_stats"]["aggregation_epochs"]

        # ARS and TRS balances do not require a start and stop epoch
        if key == "percentile-staking-balances":
            response = cache.get(key_mapping[key])
            if response == None:
                return {"error": "could not fetch balance percentiles, try again later"}
            return {
                "last_updated": last_updated_timestamp,
                "aggregation": aggregation_epochs,
                "values": response["percentiles"],
                "data": response,
            }
        else:
            # If below keys are requested and no epochs are defined, return the statistics for the whole network lifetime
            if key in (
                "num-unique-miners",
                "num-unique-data-request-solvers",
                "top-100-miners",
                "top-100-data-request-solvers",
            ) and start_epoch == None and stop_epoch == None:
                data = cache.get(f"network_{key}")
                if data == None:
                    return {"error": f"could not fetch {key}, try again later"}
                else:
                    if key.startswith("top-100"):
                        data = self.translate_address_ids(data)
                    return {
                        "last_updated": last_updated_timestamp,
                        "aggregation": aggregation_epochs,
                        "data": data,
                    }

            # If rollbacks are requested without epochs, return the 100 most recent
            if key == "list-rollbacks" and start_epoch == None and stop_epoch == None:
                data = cache.get(f"network_{key}")
                if data == None:
                    return {"error": f"could not fetch {key}, try again later"}
                else:
                    return {
                        "last_updated": last_updated_timestamp,
                        "aggregation": aggregation_epochs,
                        "data": data[:100],
                    }

            # A different statistic is requested and / or a start or stop epoch are defined

            # If we need to calculate a start or stop epoch, fetch the latest confirmed one from the database
            _, last_confirmed_epoch = self.witnet_database.get_last_block()

            # Start epoch given: use the aggregation period to floor it
            if start_epoch != None:
                start_epoch = int(int(start_epoch) / aggregation_epochs) * aggregation_epochs
            # No start epoch given: use the aggregation period surrounding the last confirmed epoch
            else:
                # Return the last 60 aggregation periods worth of data
                # This is roughly one month at the default aggregation period of 1000 epochs
                if key.startswith("histogram-"):
                    start_epoch = int(last_confirmed_epoch / aggregation_epochs - 59) * aggregation_epochs
                # Return only the last aggregation period's data
                else:
                    start_epoch = int(last_confirmed_epoch / aggregation_epochs) * aggregation_epochs

            # Stop epoch given: use the aggregation period to ceil it
            if stop_epoch != None:
                if int(stop_epoch) > last_confirmed_epoch:
                    stop_epoch = int(last_confirmed_epoch / aggregation_epochs + 1) * aggregation_epochs
                else:
                    stop_epoch = int(int(stop_epoch) / aggregation_epochs + 1) * aggregation_epochs
            # No stop epoch given: use the aggregation period surrounding the last confirmed epoch
            else:
                stop_epoch = int(last_confirmed_epoch / aggregation_epochs + 1) * aggregation_epochs

            # If the parameters were passed in the wrong order (or partially calculated), swap them
            if start_epoch > stop_epoch:
                start_epoch, stop_epoch = stop_epoch, start_epoch

            # Need at least a difference of one day worth of epochs between the start and stop epoch
            if start_epoch == stop_epoch:
                start_epoch -= aggregation_epochs

            # The list-rollbacks key requires special handling since it is not saved as periodic data
            if key == "list-rollbacks":
                rollbacks = cache.get(key_mapping[key])
                if rollbacks == None:
                    return {"error": f"could not fetch all data for {key} between epochs {start_epoch} and {stop_epoch}"}
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": [start_epoch, stop_epoch],
                    "data": [r for r in rollbacks if r[2] >= start_epoch and r[1] <= stop_epoch],
                }

            # Fetch all required data
            period_start, data = [], []
            for start in range(start_epoch, stop_epoch, aggregation_epochs):
                temp_data = cache.get(f"{key_mapping[key]}_{start}_{start + aggregation_epochs}")
                period_start.append(start)
                data.append(temp_data)

            # Remove the last item from the list if it was None
            # This happens when the caching process did not run yet while the aggregation_epochs boundary was crossed
            if data[-1] == None:
                period_start = period_start[:-1]
                data = data[:-1]

            # If any other item is None, return an error
            if any(d == None for d in data):
                return {"error": f"could not fetch all data for {key} between epochs {start_epoch} and {stop_epoch}"}

            # Aggregate depending on the requested key
            if key in ("num-unique-miners", "top-100-miners", "num-unique-data-request-solvers", "top-100-data-request-solvers"):
                num_unique, top_100 = aggregate_nodes(data)
                if key in ("num-unique-miners", "num-unique-data-request-solvers"):
                    return {
                        "last_updated": last_updated_timestamp,
                        "aggregation": aggregation_epochs,
                        "period": [start_epoch, stop_epoch],
                        "data": num_unique,
                    }
                else:
                    top_100_mapped = self.translate_address_ids(top_100)
                    return {
                        "last_updated": last_updated_timestamp,
                        "aggregation": aggregation_epochs,
                        "period": [start_epoch, stop_epoch],
                        "data": top_100_mapped,
                    }

            if key == "histogram-data-requests":
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "data": [[d[0], d[1]] for d in data],
                }

            if key == "histogram-data-request-composition":
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "data": [[d[0], d[2], d[3], d[4]] for d in data],
                }

            if key == "histogram-data-request-witness":
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "values": sorted(list(set(int(k) for d in data for k in d[5].keys()))),
                    "data": [d[5] for d in data],
                }

            if key == "histogram-data-request-reward":
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "values": sorted(list(set(int(k) for d in data for k in d[6].keys()))),
                    "data": [d[6] for d in data],
                }

            if key == "histogram-data-request-collateral":
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "values": sorted(list(set(int(k) for d in data for k in d[7].keys()))),
                    "data": [d[7] for d in data],
                }

            if key == "histogram-supply-burn-rate":
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "data": [[d[0] / 1E9, d[1] / 1E9] for d in data],
                }

            if key in ("histogram-value-transfers", "histogram-trs-data", "histogram-data-request-lie-rate"):
                return {
                    "last_updated": last_updated_timestamp,
                    "aggregation": aggregation_epochs,
                    "period": period_start,
                    "data": data,
                }

    def get_mempool(self, key):
        self.logger.info(f"get_mempool({key})")

        if key not in (
            "live",
            "history",
        ):
            self.logger.warning(f"Invalid key for mempool API endpoint requested")
            return {"error": "invalid key for mempool API endpoint requested"}

        if key == "live":
            mempool = self.witnet_node.get_mempool()
            if "result" in mempool:
                self.logger.info(f"Fetched live mempool from node")
                mempool = mempool["result"]
            else:
                self.logger.warning(f"Could not fetch the live mempool")
                mempool = {"error": "could not fetch the live mempool"}
        else:
            mempool = cache.get(f"mempool")
            if not mempool:
                self.logger.info(f"Could not find 'mempool' in memcached cache")
                mempool = self.transaction_pool.get_historical_mempool()
                cache.set(f"mempool", mempool, timeout=calculate_timeout(self.cache_config["views"]["pending"]["timeout"]))
            else:
                self.logger.info(f"Found 'mempool' in memcached cache")

        return mempool

    def get_tapi(self, return_all_tapis):
        self.logger.info(f"get_tapi({return_all_tapis})")

        all_tapis = {}
        # Get auxiliary variable which defines the known TAPI's
        tapis_cached = cache.get("tapis-cached")
        if tapis_cached:
            for counter in tapis_cached:
                # Fetch TAPI details from cache
                tapi = cache.get(f"tapi-{counter}")
                if not tapi:
                    self.logger.error(f"Could not find 'tapi-{counter}' in memcached cache")
                    all_tapis[counter] = {"error": "Could not find TAPI details"}
                    continue

                # Skip unactivated TAPIs unless requested otherwise
                if not return_all_tapis and not tapi["activated"]:
                    self.logger.debug(f"Not sending 'tapi-{counter}' as it was not activated")
                    continue

                # Save TAPI details
                self.logger.debug(f"Found 'tapi-{counter}' in memcached cache")
                all_tapis[counter] = tapi

                # Serialize TAPI plot to bytes (if found)
                tapi_plot = os.path.join(self.plot_dir, f"tapi-{counter}.png")
                if os.path.exists(tapi_plot):
                    plot = Image.open(tapi_plot, mode="r")
                    byte_arr = io.BytesIO()
                    plot.save(byte_arr, format="PNG")
                    encoded_plot = base64.encodebytes(byte_arr.getvalue()).decode("ascii")
                    all_tapis[counter]["plot"] = encoded_plot
                elif tapi["current_epoch"] < tapi["start_epoch"]:
                    all_tapis[counter]["plot"] = "The TAPI did not start yet"
                else:
                    all_tapis[counter]["plot"] = "Could not find TAPI plot"

        if all_tapis == {}:
            self.logger.info(f"No TAPI's found in memcached cache")

        self.logger.info(f"Returning TAPI's {', '.join(str(key) for key in all_tapis.keys())}")
        return all_tapis

    def get_status(self):
        self.logger.info(f"get_status()")

        status = cache.get(f"status")
        if not status:
            self.logger.info(f"Could not find 'status' in memcached cache")

            node_pool_status = self.witnet_node.get_sync_status()

            current_node_epoch = 0
            if type(node_pool_status) is dict and "error" in node_pool_status:
                node_pool_status = node_pool_status["error"]
                node_pool_message = "Could not fetch status of the node pool"
            else:
                node_pool_status = node_pool_status["result"]
                current_node_epoch = node_pool_status["current_epoch"]
                node_pool_message = "Fetched node pool status correctly"

            last_confirmed_block_hash, last_confirmed_epoch = self.witnet_database.get_last_block()
            last_unconfirmed_block_hash, last_unconfirmed_epoch = self.witnet_database.get_last_block(confirmed=False)

            database_message = "Explorer backend seems healthy"

            # Check if the last confirmed epoch the block before the previous superepoch
            superblock_period = self.consensus_constants.superblock_period
            expected_confirmed_epoch = (int(last_unconfirmed_epoch / superblock_period) * superblock_period) - superblock_period - 1
            if current_node_epoch != 0 and last_confirmed_epoch < expected_confirmed_epoch:
                database_message = "The network has probably rolled back a superepoch"

            # More than 100 unconfirmed blocks have elapsed, maybe the backend crashed
            if current_node_epoch != 0 and current_node_epoch > last_confirmed_epoch + 100:
                database_message = "The backend has probably crashed, please warn drcpu"

            # We did not (yet) insert a block for the previous epoch, did the explorer crash?
            if current_node_epoch != 0 and current_node_epoch - 2 > last_unconfirmed_epoch:
                database_message = "The backend has probably crashed, please warn drcpu"

            status = {
                "node_pool": node_pool_status,
                "database_last_confirmed": [last_confirmed_block_hash, last_confirmed_epoch],
                "database_last_unconfirmed": [last_unconfirmed_block_hash, last_unconfirmed_epoch],
                "database_message": database_message,
                "node_pool_message": node_pool_message,
            }

            cache.set(f"status", status, timeout=calculate_timeout(self.cache_config["views"]["status"]["timeout"]))
        else:
            self.logger.info(f"Found 'status' in memcached cache")

        return status

    def get_address(self, address_value, tab, limit, epoch):
        self.logger.info(f"get_address({address_value}, {tab}, {limit}, {epoch})")

        if not sanitize_address(address_value):
            self.logger.warning(f"Invalid value for address: {address_value}")
            return {"error": "invalid address"}
        if not tab in ("details", "value_transfers", "blocks", "data_requests_solved", "data_requests_launched", "reputation"):
            self.logger.warning(f"Invalid value for tab: {tab}")
            return {"error": "tab value is not valid"}
        # integers are sanitized already by Flask argument parsing

        address = Address(address_value, self.config, self.consensus_constants, logging_queue=self.log_queue)

        self.try_send_request({"method": "track", "addresses": [address_value], "id": 1})

        if tab == "details":
            address.connect_to_database()
            return address.get_details()
        elif tab == "value_transfers":
            # Try to fetch the result from the cache
            cached_value_transfers = cache.get(f"{address_value}_value-transfers")
            # Return cached version if found (fast)
            if cached_value_transfers:
                self.logger.info(f"Found value transfers for {address_value} in cache")
                return cached_value_transfers
            # Query the database and build the requested view (slow)
            else:
                self.logger.info(f"Did not find value transfers for {address_value} in cache, querying database")
                address.connect_to_database()
                return address.get_value_transfers(limit, epoch)
        elif tab == "blocks":
            # Try to fetch the result from the cache
            cached_blocks = cache.get(f"{address_value}_blocks")
            # Return cached version if found (fast)
            if cached_blocks:
                self.logger.info(f"Found blocks for {address_value} in cache")
                return cached_blocks
            # Query the database and build the requested view (slow)
            else:
                self.logger.info(f"Did not find blocks for {address_value} in cache, querying database")
                address.connect_to_database()
                return address.get_blocks(limit, epoch)
        elif tab == "data_requests_solved":
            # Try to fetch the result from the cache
            cached_data_requests_solved = cache.get(f"{address_value}_data-requests-solved")
            # Return cached version if found (fast)
            if cached_data_requests_solved:
                self.logger.info(f"Found data requests solved for {address_value} in cache")
                return cached_data_requests_solved
            # Query the database and build the requested view (slow)
            else:
                self.logger.info(f"Did not find data requests solved for {address_value} in cache, querying database")
                address.connect_to_database()
                return address.get_data_requests_solved(limit, epoch)
        elif tab == "data_requests_launched":
            # Try to fetch the result from the cache
            cached_data_requests_launched = cache.get(f"{address_value}_data-requests-launched")
            # Return cached version if found (fast)
            if cached_data_requests_launched:
                self.logger.info(f"Found data requests launched for {address_value} in cache")
                return cached_data_requests_launched
            # Query the database and build the requested view (slow)
            else:
                self.logger.info(f"Did not find data requests launched for {address_value} in cache, querying database")
                address.connect_to_database()
                return address.get_data_requests_launched(limit, epoch)
        elif tab == "reputation":
            reputation_plot = os.path.join(self.plot_dir, f"{address_value}.png")
            # Check if the reputation plot exists
            if os.path.exists(reputation_plot):
                st = os.stat(reputation_plot)
                # Found a recently generated reputation plot, return it
                if time.time() - st.st_mtime < 3600:
                    return send_file(reputation_plot, mimetype="image/png")
                # Reputation plot is older than an hour and is likely incorrect, the user should reload the page later
                else:
                    return {"error": "No reputation plot generated yet, try again later."}
            # Could not find reputation plot despite already launching a generation request, the user should reload the page later
            else:
                return {"error": "No reputation plot generated yet, try again later."}

    def get_priority(self, priority_type="all"):
        self.logger.info(f"get_priority({priority_type})")

        valid_priority_types = ("all", "drt", "vtt")
        if priority_type not in valid_priority_types:
            self.logger.warning(f"Invalid value for priority type: {priority_type}")
            return {"error": f"invalid priority type, valid types are: {', '.join(valid_priority_types)}"}

        priority = cache.get(f"priority")
        if not priority:
            self.logger.info(f"Could not find 'priority' in memcached cache")
            priority = self.witnet_node.get_priority()
            if "result" in priority:
                priority = priority["result"]
                cache.set(f"priority", priority, timeout=calculate_timeout(self.cache_config["views"]["priority"]["timeout"]))
            else:
                self.logger.warning(f"Transaction priority returned invalid result, cannot cache it")
        else:
            self.logger.info(f"Found 'priority' in memcached cache")

        if priority_type == "all":
            return priority
        else:
            return {key: priority[key] for key in priority.keys() if key.startswith(priority_type)}

    #####################################################################
    #   API endpoint functions which should not employ direct caching   #
    #####################################################################

    def get_utxos(self, address):
        self.logger.info(f"get_utxos({address})")

        if not sanitize_address(address):
            self.logger.warning(f"Invalid value for address: {address}")
            return {"error": "invalid address"}

        self.try_send_request({"method": "track", "addresses": [address], "id": 2})

        utxos = cache.get(f"{address}_utxos")
        if utxos:
            self.logger.info(f"Found UTXOs for {address} in cache")
            return utxos

        utxos = self.witnet_node.get_utxos(address)
        if "result" in utxos:
            return utxos["result"]
        else:
            return utxos

    def get_address_info(self, address):
        self.logger.info(f"get_address_info({address})")

        if not sanitize_address(address):
            self.logger.warning(f"Invalid value for address: {address}")
            return {"error": "invalid address"}

        sql = """
            SELECT
                address,
                label,
                active,
                block,
                mint,
                value_transfer,
                data_request,
                commit,
                reveal,
                tally
            FROM
                addresses
            WHERE
                address='%s'
        """ % address
        address_data = self.witnet_database.sql_return_one(sql)
        if address_data:
            return {
                "address": address_data[0],
                "label": address_data[1] if address_data[1] else "",
                "active": address_data[2],
                "block": address_data[3],
                "mint": address_data[4],
                "value_transfer": address_data[5],
                "data_request": address_data[6],
                "commit": address_data[7],
                "reveal": address_data[8],
                "tally": address_data[9]
            }
        else:
            return {
                "error": "could not find address"
            }

    def get_address_labels(self):
        self.logger.info(f"get_address_labels()")

        sql = """
            SELECT
                address,
                label
            FROM
                addresses
            WHERE
                label IS NOT NULL
        """
        addresses = self.witnet_database.sql_return_all(sql)
        if addresses:
            return {address[0]: address[1] for address in addresses}
        else:
            return {}

    def send_vtt(self, vtt, test):
        self.logger.info(f"send_vtt({vtt}, {test})")
        return self.witnet_node.send_vtt(vtt, test)
