import logging
import logging.handlers
import psycopg2
import toml

from blockchain.witnet_database import WitnetDatabase

from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode

from pages.blockchain import Blockchain
from pages.home import Home
from pages.network import Network
from pages.reputation_list import ReputationList
from pages.rich_list import RichList
from pages.tapi import Tapi
from pages.transaction_pool import TransactionPool

from objects.address import Address
from objects.block import Block
from objects.data_request_history import DataRequestHistory
from objects.data_request_report import DataRequestReport
from transactions.data_request import DataRequest
from transactions.commit import Commit
from transactions.tally import Tally

from transactions.mint import Mint
from transactions.value_transfer import ValueTransfer

class NodeManager(object):
    def __init__(self, config, logging_queue):
        # Set up logger
        self.logging_queue = logging_queue
        self.configure_logging_process(self.logging_queue, "node-manager")
        self.logger = logging.getLogger("node-manager")

        self.config = config

        self.node_config = self.config["api"]["node"]

        # Get consensus constants
        socket_host = self.node_config["host"]
        socket_port = self.node_config["port"]
        error_retry = self.node_config["error_retry"]
        self.consensus_constants = ConsensusConstants(socket_host, socket_port, error_retry, self.logging_queue, "node-consensus")

        # Connect to database
        self.db_config = self.config["api"]["database"]
        db_user = self.db_config["user"]
        db_name = self.db_config["name"]
        db_pass = self.db_config["password"]
        self.witnet_database = WitnetDatabase(db_user, db_name, db_pass, self.logging_queue, "db-api")

        # Create witnet node
        socket_host = self.node_config["host"]
        socket_port = self.node_config["port"]
        self.witnet_node = WitnetNode(socket_host, socket_port, 15, self.logging_queue, "node-api")

        # Create a couple of objects once
        self.blockchain = Blockchain(self.db_config, self.node_config, self.consensus_constants, self.logging_queue)
        self.home = Home(self.db_config, self.node_config, self.consensus_constants, self.logging_queue)
        self.network = Network(self.db_config, self.consensus_constants, self.logging_queue)
        self.reputation_list = ReputationList(self.node_config, self.logging_queue)
        self.rich_list = RichList(self.node_config, self.db_config, self.logging_queue)
        self.tapi = Tapi(self.db_config, self.consensus_constants, self.logging_queue)
        self.transaction_pool = TransactionPool(self.db_config, self.logging_queue)

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

    def get_hash(self, hash_value, simple, start, stop, amount):
        self.logger.info(f"Get hash {hash_value}")

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

        sql = "SELECT type FROM hashes WHERE hash=%s" % psycopg2.Binary(bytearray.fromhex(hash_value))
        result = self.witnet_database.sql_return_one(sql)
        if result:
            hash_type = result[0]
        else:
            # If the value transfer is in the mempool, it is pending execution
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

        if hash_type == "block":
            # Fetch block from a node
            block = Block(hash_value, self.consensus_constants, self.logging_queue, db_config=self.db_config, node_config=self.node_config)

            # Process data
            json_block = block.process_block("api")
            json_block["number_of_commits"] = len(json_block["commit_txns"])
            json_block["number_of_reveals"] = len(json_block["reveal_txns"])

            # Group commits per data request
            commits_for_data_request = {}
            for commit in json_block["commit_txns"]:
                if not commit["data_request_txn_hash"] in commits_for_data_request:
                    commits_for_data_request[commit["data_request_txn_hash"]] = {
                        "collateral": commit["collateral"],
                        "txn_address": [],
                        "txn_hash": [],
                    }
                commits_for_data_request[commit["data_request_txn_hash"]]["txn_address"].append(commit["txn_address"])
                commits_for_data_request[commit["data_request_txn_hash"]]["txn_hash"].append(commit["txn_hash"])
            json_block["commit_txns"] = commits_for_data_request

            # Group commits per data request
            reveals_for_data_request = {}
            for reveal in json_block["reveal_txns"]:
                if not reveal["data_request_txn_hash"] in reveals_for_data_request:
                    reveals_for_data_request[reveal["data_request_txn_hash"]] = {
                        "reveal_translation": [],
                        "success": [],
                        "txn_address": [],
                        "txn_hash": [],
                    }
                reveals_for_data_request[reveal["data_request_txn_hash"]]["reveal_translation"].append(reveal["reveal_translation"])
                reveals_for_data_request[reveal["data_request_txn_hash"]]["success"].append(1 if reveal["success"] else 0)
                reveals_for_data_request[reveal["data_request_txn_hash"]]["txn_address"].append(reveal["txn_address"])
                reveals_for_data_request[reveal["data_request_txn_hash"]]["txn_hash"].append(reveal["txn_hash"])
            json_block["reveal_txns"] = reveals_for_data_request

            del json_block["tapi_accept"]

            return json_block
        elif hash_type == "mint_txn":
            # Create mint transaction and get the details from the database
            mint = Mint(self.consensus_constants, self.logging_queue, database_config=self.db_config)
            return mint.get_transaction_from_database(hash_value)
        elif hash_type == "value_transfer_txn":
            # Create value transfer transaction and get the details from the database
            value_transfer = ValueTransfer(self.consensus_constants, self.logging_queue, database_config=self.db_config)
            return value_transfer.get_transaction_from_database(hash_value)
        elif hash_type in ("data_request_txn", "commit_txn", "reveal_txn", "tally_txn"):
            if simple:
                if hash_type == "data_request_txn":
                    data_request = DataRequest(self.consensus_constants, self.logging_queue, database_config=self.db_config, node_config=self.node_config)
                    transaction = data_request.get_transaction_from_database(hash_value)
                elif hash_type == "commit_txn":
                    commit = Commit(self.consensus_constants, self.logging_queue, database_config=self.db_config, node_config=self.node_config)
                    transaction = commit.get_transaction_from_database(hash_value)
                elif hash_type == "tally_txn":
                    tally = Tally(self.consensus_constants, self.logging_queue, database_config=self.db_config, node_config=self.node_config)
                    transaction = tally.get_transaction_from_database(hash_value)
                return transaction
            # Create data request report from this hash
            else:
                data_request_report = DataRequestReport(hash_type, hash_value, self.consensus_constants, self.logging_queue, self.db_config)
                return data_request_report.get_report()
        elif hash_type in ("RAD_bytes_hash", "data_request_bytes_hash"):
            data_request_history = DataRequestHistory(self.consensus_constants, self.logging_queue, database_config=self.db_config)
            return data_request_history.get_history(hash_type, hash_value, start, stop, amount)
        else:
            return {"error": "unknown hash type"}

    def get_address(self, address_value, tab, limit, epoch):
        self.logger.info(f"Fetch address data for {address_value}, {tab}")

        if not self.sanitize_input(address_value, "alphanumeric"):
            self.logger.warning(f"Invalid value for address: {address_value}")
            return {"error": "address is not an alphanumeric value"}
        if not tab in ("details", "value_transfers", "blocks", "data_requests_solved", "data_requests_launched"):
            self.logger.warning(f"Invalid value for tab: {tab}")
            return {"error": "tab value is not valid"}
        # integers are sanitized already by Flask argument parsing

        address = Address(address_value, self.db_config, self.node_config, self.consensus_constants, self.logging_queue)

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

    def get_blockchain(self, action, block):
        self.logger.info(f"Get blockchain {action}, {block}")

        if not self.sanitize_input(action, "alpha"):
            self.logger.warning(f"Invalid value for action: {action}")
            return {"error": "action is not an alpha value"}
        # integers are sanitized already by Flask argument parsing

        if action == "init":
            return self.blockchain.get_blockchain_details(action, block, -1, -1)
        elif action == "append":
            if block >= 0:
                return self.blockchain.get_blockchain_details(action, 0, block, -1)
            else:
                return self.blockchain.get_blockchain_details(action, block, -1, -1)
        elif action == "prepend":
            start = max(0, block - 50)
            return self.blockchain.get_blockchain_details(action, 0, start, block)
        else:
            return {"error": "unknown action type"}

    def get_home(self, key):
        self.logger.info("Get home")
        # key is already sanitized one level above in home() / supply_info()
        return self.home.get_home(key)

    def get_reputation_list(self):
        self.logger.info("Get reputation list")
        return self.reputation_list.get_reputation_list()

    def get_rich_list(self, start, stop):
        self.logger.info(f"Get rich list from {start} to {stop}")
        # integers are sanitized already by Flask argument parsing
        return self.rich_list.get_rich_list(start, stop)

    def get_network(self):
        self.logger.info("Get network")
        return self.network.get_network_stats()

    def get_pending_transactions(self):
        self.logger.info("Get pending transactions")
        return self.transaction_pool.get_pending_transactions()

    def get_utxos(self, address):
        self.logger.info(f"Get utxos for {address}")

        if not self.sanitize_input(address, "alphanumeric"):
            self.logger.warning(f"Invalid value for address: {address}")
            return {"error": "address is not an alphanumeric value"}

        utxos = self.witnet_node.get_utxos(address)
        if "result" in utxos:
            return utxos["result"]
        else:
            return utxos

    def init_tapi(self):
        return self.tapi.init_tapi()

    def update_tapi(self):
        return self.tapi.update_tapi()

    def send_vtt(self, vtt, test):
        return self.witnet_node.send_vtt(vtt, test)

    def get_status(self):
        self.logger.info(f"Get status of the explorer")

        message = "Explorer backend seems healthy"

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

        database_message = "Backend seems healthy"

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

        return {
            "node_pool": node_pool_status,
            "database_last_confirmed": [last_confirmed_block_hash, last_confirmed_epoch],
            "database_last_unconfirmed": [last_unconfirmed_block_hash, last_unconfirmed_epoch],
            "database_message": database_message,
            "node_pool_message": node_pool_message,
        }
