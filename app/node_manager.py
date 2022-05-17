import logging
import logging.handlers
import psycopg2
import toml

from app.cache import cache

from blockchain.witnet_database import WitnetDatabase

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

class NodeManager(object):
    def __init__(self, config, log_queue):
        error_retry = config["api"]["error_retry"]

        if config["api"]["cache_server"] == "memcached":
            self.cache_config = config["api"]["caching"]
        else:
            assert False, "Need to specify a caching instance"

        # Set up logger
        self.log_queue = log_queue
        self.configure_logging_process(self.log_queue, "node-manager")
        self.logger = logging.getLogger("node-manager")

        # Get configuration to connect to the node pool
        self.node_config = config["node-pool"]
        socket_host, socket_port = self.node_config["host"], self.node_config["port"]

        # Create witnet node
        self.witnet_node = WitnetNode(socket_host, socket_port, 15, log_queue=self.log_queue, log_label="node-api")

        # Get consensus constants
        self.consensus_constants = ConsensusConstants(socket_host, socket_port, error_retry, log_queue=self.log_queue, log_label="node-consensus")

        # Get configuration to connect to the database
        self.database_config = config["database"]
        db_user, db_name, db_pass = self.database_config["user"], self.database_config["name"], self.database_config["password"]

        # Connect to database
        self.witnet_database = WitnetDatabase(db_user, db_name, db_pass, log_queue=self.log_queue, log_label="db-api")

        # Create a couple of objects once
        self.blockchain = Blockchain(self.database_config, self.node_config, self.consensus_constants, self.log_queue)
        self.transaction_pool = TransactionPool(self.database_config, self.log_queue)

    ########################
    #   Helper functions   #
    ########################

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def sanitize_input(self, input_value, required_type):
        if required_type == "bool":
            return input_value in (True, False)
        elif required_type == "hexadecimal":
            try:
                int(input_value, 16)
                return True
            except ValueError:
                return False
        elif required_type == "alpha":
            return input_value.isalpha()
        elif required_type == "alphanumeric":
            return input_value.isalnum()
        return False

    def is_transaction_pending(self, hash_value):
        transactions_pool = self.witnet_node.get_mempool()
        if "error" in transactions_pool:
            return {
                "txn_hash": hash_value,
                "status": "Could not fetch pending transactions",
                "type": "unknown transaction type",
                "txn_time": 0,
                "block_hash": "",
            }
        transactions_pool = transactions_pool["result"]

        if hash_value in transactions_pool["data_request"]:
            status = "pending"
            txn_type = "data_request_txn"
        elif hash_value in transactions_pool["value_transfer"]:
            status = "pending"
            txn_type = "value_transfer_txn"
        else:
            status = "unknown hash"
            txn_type = "unknown"

        return {
            "txn_hash": hash_value,
            "status": status,
            "type": txn_type,
            "txn_time": 0,
            "block_hash": "",
        }

    def pretty_hash_type(self, hash_type):
        return hash_type.replace("_", " ").replace("txn", "transaction")

    #######################################################
    #   API endpoint functions which can employ caching   #
    #######################################################

    def get_hash(self, hash_value, simple, start, stop, amount):
        self.logger.info(f"get_hash({hash_value}, {simple}, {start}, {stop}, {amount})")

        if hash_value.startswith("0x"):
            self.logger.warning(f"Invalid value for hash: {hash_value}")
            return {"error": "hexadecimal hash should not start with 0x"}

        if len(hash_value) != 64:
            self.logger.warning(f"Invalid hash length ({len(hash_value)}): {hash_value}")
            return {"error": "incorrect hexadecimal hash length"}

        if not self.sanitize_input(hash_value, "hexadecimal"):
            self.logger.warning(f"Invalid value for hash: {hash_value}")
            return {"error": "hash is not a hexadecimal value"}

        if not self.sanitize_input(simple, "bool"):
            self.logger.warning(f"Invalid value for simple: {simple}")
            return {"error": "simple is not a boolean value"}

        if amount not in range(1, 1000):
            self.logger.warning(f"Value of amount is bigger than 1000")
            return {"error": "value of amount is bigger than 1000"}

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
            block = Block(hash_value, self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config, node_config=self.node_config)
            json_block = block.process_block("api")
            if json_block["details"]["confirmed"]:
                self.logger.info(f"Added block with hash '{hash_value}' to the memcached cache")
                cache.set(hash_value, json_block, timeout=self.cache_config["scripts"]["block"]["timeout"])
            else:
                self.logger.info(f"Did not add unconfirmed block with hash '{hash_value}' to the memcached cache")
            return json_block

        if hash_type == "mint_txn":
            # Create mint transaction and get the details from the database
            mint = Mint(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config)
            mint_txn = mint.get_transaction_from_database(hash_value)
            if mint_txn["confirmed"]:
                self.logger.info(f"Added mint transaction with hash '{hash_value}' to the memcached cache")
                cache.set(hash_value, mint_txn, timeout=self.cache_config["views"]["hash"]["timeout"])
            else:
                self.logger.info(f"Did not add unconfirmed mint transaction with hash '{hash_value}' to the memcached cache")
            return mint_txn

        if hash_type == "value_transfer_txn":
            # Create value transfer transaction and get the details from the database
            value_transfer = ValueTransfer(self.consensus_constants, log_queue=self.log_queue, database_config=self.database_config)
            value_transfer_txn = value_transfer.get_transaction_from_database(hash_value)
            if value_transfer_txn["confirmed"]:
                self.logger.info(f"Added value transfer transaction with hash '{hash_value}' to the memcached cache")
                cache.set(hash_value, value_transfer_txn, timeout=self.cache_config["views"]["hash"]["timeout"])
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
                        cache.set(data_request_hash, data_request_report, timeout=self.cache_config["scripts"]["data_request_reports"]["timeout"])
                    else:
                        if not data_request_report["tally_txn"]:
                            self.logger.info(f"Did not add incomplete data request report with hash '{data_request_hash}' to the memcached cache")
                        else:
                            self.logger.info(f"Did not add unconfirmed data request report with hash '{data_request_hash}' to the memcached cache")
                else:
                    self.logger.info(f"Found a data request report for a {self.pretty_hash_type(hash_type)} with data request hash '{data_request_hash}' in memcached cache")
                return data_request_report

        if hash_type in ("RAD_bytes_hash", "data_request_bytes_hash"):
            # Create data request history
            data_request_history = DataRequestHistory(self.consensus_constants, self.log_queue, self.database_config)
            # Data request histories change constantly, so we cannot employ simple hash-based caching only
            return data_request_history.get_history(hash_type, hash_value, start, stop, amount)

        return {"error": "unknown hash type"}

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
                cache.set(cache_key, blockchain_init, timeout=self.cache_config["views"]["blockchain"]["timeout"])
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
                cache.set(cache_key, blockchain_append, timeout=self.cache_config["views"]["blockchain"]["timeout"])
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
                cache.set(cache_key, blockchain_prepend, timeout=self.cache_config["views"]["blockchain"]["timeout"])
            else:
                self.logger.info(f"Found '{cache_key}' in memcached cache")
            return blockchain_prepend

    def get_reputation_list(self):
        self.logger.info("get_reputation_list()")

        reputation = cache.get(f"reputation")
        if not reputation:
            self.logger.error(f"Could not find 'reputation' in memcached cache")
        else:
            self.logger.info(f"Found 'reputation' in memcached cache")

        return reputation

    def get_richlist(self, start, stop):
        self.logger.info(f"get_richlist({start}, {stop})")

        # integer arguments are sanitized already by Flask argument parsing
        if stop - start > 1000:
            return {"error": "cannot fetch more than 1000 entries from the richlist at once"}

        if start % 1000 != 0 or stop % 1000 != 0:
            return {"error": "start and stop parameters should be thousands"}

        richlist_part = cache.get(f"richlist_{start}-{stop}")
        balances_sum = cache.get(f"richlist_balances-sum")
        last_updated = cache.get(f"richlist_last-updated")
        if not richlist_part:
            self.logger.error(f"Could not find 'richlist_{start}_{stop}' in memcached cache")
            richlist = {
                "richlist": [],
                "balances_sum": 0,
                "last_updated": 0,
            }
        else:
            self.logger.info(f"Found 'richlist_{start}_{stop}' in memcached cache")
            richlist = {
                "richlist": richlist_part,
                "balances_sum": balances_sum if balances_sum else 0,
                "last_updated": last_updated if last_updated else 0,
            }

        return richlist

    def get_network(self):
        self.logger.info("get_network()")

        network = cache.get(f"network")
        if not network:
            self.logger.error(f"Could not find 'network' in memcached cache")
            network = {
                "rollback_rows": [],
                "unique_miners": 0,
                "unique_dr_solvers": 0,
                "top_100_miners": [],
                "top_100_dr_solvers": [],
                "last_updated": 0
            }
        else:
            self.logger.info(f"Found 'network' in memcached cache")

        return network

    def get_mempool_transactions(self):
        self.logger.info("get_mempool_transactions()")

        mempool_transactions = cache.get(f"mempool_transactions")
        if not mempool_transactions:
            self.logger.info(f"Could not find 'mempool_transactions' in memcached cache")
            mempool_transactions = self.transaction_pool.get_mempool_transactions()
            cache.set(f"mempool_transactions", mempool_transactions, timeout=self.cache_config["views"]["pending"]["timeout"])
        else:
            self.logger.info(f"Found 'mempool_transactions' in memcached cache")

        return mempool_transactions

    def init_tapi(self):
        self.logger.info(f"init_tapi()")

        counter = 1
        all_tapis = {}
        while True:
            tapi = cache.get(f"tapi-{counter}")
            if not tapi:
                break
            self.logger.info(f"Found 'tapi-{counter}' in memcached cache")
            all_tapis[counter] = tapi
            counter += 1
        if all_tapis == {}:
            self.logger.info(f"No tapis found in memcached cache")

        return all_tapis

    def update_tapi(self):
        self.logger.info(f"update_tapi()")

        counter = 1
        updated_tapis = {}
        while True:
            tapi = cache.get(f"tapi-{counter}")
            if not tapi:
                break
            self.logger.info(f"Found 'tapi-{counter}' in memcached cache")
            if tapi[counter]["active"]:
                updated_tapis[counter] = tapi
            counter += 1
        if updated_tapis == {}:
            self.logger.info(f"No active or future tapis found in memcached cache")

        return updated_tapis

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

            cache.set(f"status", status, timeout=self.cache_config["views"]["status"]["timeout"])
        else:
            self.logger.info(f"Found 'status' in memcached cache")

        return status

    ##############################################################
    #   API endpoint functions which should not employ caching   #
    ##############################################################

    def get_address(self, address_value, tab, limit, epoch):
        self.logger.info(f"get_address({address_value}, {tab}, {limit}, {epoch})")

        if not self.sanitize_input(address_value, "alphanumeric"):
            self.logger.warning(f"Invalid value for address: {address_value}")
            return {"error": "address is not an alphanumeric value"}
        if not tab in ("details", "value_transfers", "blocks", "data_requests_solved", "data_requests_launched"):
            self.logger.warning(f"Invalid value for tab: {tab}")
            return {"error": "tab value is not valid"}
        # integers are sanitized already by Flask argument parsing

        address = Address(address_value, self.database_config, self.node_config, self.consensus_constants, self.log_queue)

        if tab == "details":
            return address.get_details()
        elif tab == "value_transfers":
            return address.get_value_transfers(limit, epoch)
        elif tab == "blocks":
            return address.get_blocks(limit, epoch)
        elif tab == "data_requests_solved":
            return address.get_data_requests_solved(limit, epoch)
        elif tab == "data_requests_launched":
            return address.get_data_requests_launched(limit, epoch)

    def get_utxos(self, address):
        self.logger.info(f"get_utxos({address})")

        if not self.sanitize_input(address, "alphanumeric"):
            self.logger.warning(f"Invalid value for address: {address}")
            return {"error": "address is not an alphanumeric value"}

        utxos = self.witnet_node.get_utxos(address)
        if "result" in utxos:
            return utxos["result"]
        else:
            return utxos

    def send_vtt(self, vtt, test):
        self.logger.info(f"send_vtt({vtt}, {test})")
        return self.witnet_node.send_vtt(vtt, test)
