import logging
import logging.handlers
import time

from blockchain.witnet_database import WitnetDatabase

from node.witnet_node import WitnetNode

from transactions.mint import Mint
from transactions.value_transfer import ValueTransfer
from transactions.data_request import DataRequest
from transactions.commit import Commit
from transactions.reveal import Reveal
from transactions.tally import Tally

class Block(object):
    def __init__(self, consensus_constants, block_hash="", block_epoch=-1, logger=None, log_queue=None, database=None, database_config=None, block=None, tapi_periods=[], node_config=None):
        self.block_hash = block_hash
        self.block_epoch = block_epoch

        self.consensus_constants = consensus_constants
        self.collateral_minimum = consensus_constants.collateral_minimum
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period
        self.superblock_period = consensus_constants.superblock_period

        # Set up logger
        if logger:
            self.logger = logger
        elif log_queue:
            self.log_queue = log_queue
            self.configure_logging_process(log_queue, "block")
            self.logger = logging.getLogger("block")
        else:
            self.logger = None

        if database:
            self.witnet_database = database
        elif database_config:
            db_user = database_config["user"]
            db_name = database_config["name"]
            db_pass = database_config["password"]
            self.witnet_database = WitnetDatabase(db_user, db_name, db_pass, logger=self.logger)
        else:
            self.witnet_database = None

        self.node_config = node_config

        self.current_epoch = (int(time.time()) - self.start_time) // self.epoch_period

        if block == None:
            self.block = self.get_block()
        else:
            self.block = block

        self.tapi_periods = tapi_periods

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def get_block(self):
        # No block hash specified, check if we can fetch it based on a block epoch
        if self.block_hash == "":
            # Log and return warnings if necessary
            if self.block_epoch == -1:
                return self.return_block_error("No block hash or block epoch specified")
            if not self.witnet_database:
                return self.return_block_error("No database found to fetch block hash")

            # Fetch block hash from the database
            sql = """
                SELECT
                    block_hash,
                    epoch
                FROM blocks
                WHERE
                    epoch=%s
            """ % self.block_epoch
            block_hash = self.witnet_database.sql_return_one(sql)

            if block_hash:
                self.block_hash = block_hash[0].hex()
            else:
                return self.return_block_error(f"Could not find block for epoch {self.block_epoch}")

        # Connect to node pool
        witnet_node = WitnetNode(self.node_config, logger=self.logger)

        block = witnet_node.get_block(self.block_hash)
        if type(block) is dict and "error" in block:
            self.logger.warning(f"Unable to fetch block {self.block_hash}: {block}")
            return block

        return block["result"]

    def process_block(self, call_from):
        assert call_from == "explorer" or call_from == "api"

        if "reason" in self.block:
            return {
                "type": "block",
                "error": self.block["reason"],
            }

        self.process_details()

        self.block = {
            "type": "block",
            "details": {
                "block_hash": self.block_hash,
                "epoch": self.block_epoch,
                "time": self.start_time + (self.block_epoch + 1) * self.epoch_period,
                "dr_weight": self.dr_weight,
                "vt_weight": self.vt_weight,
                "block_weight": self.block_weight,
                "confirmed": self.confirmed,
                "status": self.status,
            },
            "mint_txn": self.process_mint_txn(),
            "value_transfer_txns": self.process_value_transfer_txns(call_from),
            "data_request_txns": self.process_data_request_txns(call_from),
            "commit_txns": self.process_commit_txns(call_from),
            "reveal_txns": self.process_reveal_txns(call_from),
            "tally_txns": self.process_tally_txns(call_from),
            "tapi_accept": self.process_tapi_signals(),
        }

        if call_from == "api":
            self.process_block_for_api()

        return self.block

    def process_block_for_api(self):
        # Add number of commits and reveals
        self.block["number_of_commits"] = len(self.block["commit_txns"])
        self.block["number_of_reveals"] = len(self.block["reveal_txns"])

        # Group commits per data request
        commits_for_data_request = {}
        for commit in self.block["commit_txns"]:
            if not commit["data_request_txn_hash"] in commits_for_data_request:
                commits_for_data_request[commit["data_request_txn_hash"]] = {
                    "collateral": commit["collateral"],
                    "txn_address": [],
                    "txn_hash": [],
                }
            commits_for_data_request[commit["data_request_txn_hash"]]["txn_address"].append(commit["txn_address"])
            commits_for_data_request[commit["data_request_txn_hash"]]["txn_hash"].append(commit["txn_hash"])
        self.block["commit_txns"] = commits_for_data_request

        # Group reveals per data request
        reveals_for_data_request = {}
        for reveal in self.block["reveal_txns"]:
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
        self.block["reveal_txns"] = reveals_for_data_request

        del self.block["tapi_accept"]

    def process_details(self):
        try:
            self.block_epoch = self.block["block_header"]["beacon"]["checkpoint"]
        except KeyError:
            self.logger.error(f"Unable to process block: {self.block}")

        self.dr_weight = self.block["dr_weight"]
        self.vt_weight = self.block["vt_weight"]
        self.block_weight = self.block["block_weight"]

        self.confirmed = self.block["confirmed"]
        if self.confirmed:
            self.status = "confirmed"
        else:
            if self.block_epoch > self.current_epoch - 2 * self.superblock_period:
                self.status = "mined"
            else:
                self.status = "reverted"

    def process_mint_txn(self):
        txn_hash = self.block["txns_hashes"]["mint"]
        json_txn = self.block["txns"]["mint"]
        block_signature = self.block["block_sig"]["public_key"]
        mint = Mint(self.consensus_constants, logger=self.logger)
        mint.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
        return mint.process_transaction(block_signature)

    def process_value_transfer_txns(self, call_from):
        value_transfer_txns = []
        if len(self.block["txns_hashes"]["value_transfer"]) > 0:
            value_transfer = ValueTransfer(self.consensus_constants, logger=self.logger, database=self.witnet_database, node_config=self.node_config)
            for i, (txn_hash, txn_weight) in enumerate(zip(self.block["txns_hashes"]["value_transfer"], self.block["txns_weights"]["value_transfer"])):
                json_txn = self.block["txns"]["value_transfer_txns"][i]
                value_transfer.set_transaction(txn_hash=txn_hash, txn_weight=txn_weight, json_txn=json_txn)
                value_transfer_txns.append(value_transfer.process_transaction(call_from))
        return value_transfer_txns

    def process_data_request_txns(self, call_from):
        data_request_transactions = []
        if len(self.block["txns_hashes"]["data_request"]) > 0:
            data_request = DataRequest(self.consensus_constants, logger=self.logger, database=self.witnet_database, node_config=self.node_config)
            for i, (txn_hash, txn_weight) in enumerate(zip(self.block["txns_hashes"]["data_request"], self.block["txns_weights"]["data_request"])):
                json_txn = self.block["txns"]["data_request_txns"][i]
                data_request.set_transaction(txn_hash=txn_hash, txn_weight=txn_weight, json_txn=json_txn)
                data_request_transactions.append(data_request.process_transaction(call_from))
        return data_request_transactions

    def process_commit_txns(self, call_from):
        commit_transactions = []
        if len(self.block["txns_hashes"]["commit"]) > 0:
            commit = Commit(self.consensus_constants, logger=self.logger, database=self.witnet_database, node_config=self.node_config)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["commit"]):
                json_txn = self.block["txns"]["commit_txns"][i]
                commit.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
                commit_transactions.append(commit.process_transaction(call_from))
        return commit_transactions

    def process_reveal_txns(self, call_from):
        reveal_transactions = []
        if len(self.block["txns_hashes"]["reveal"]) > 0:
            reveal = Reveal(self.consensus_constants, logger=self.logger)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["reveal"]):
                json_txn = self.block["txns"]["reveal_txns"][i]
                reveal.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
                reveal_transactions.append(reveal.process_transaction(call_from))
        return reveal_transactions

    def process_tally_txns(self, call_from):
        tally_transactions = []
        if len(self.block["txns_hashes"]["tally"]) > 0:
            tally = Tally(self.consensus_constants, logger=self.logger)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["tally"]):
                json_txn = self.block["txns"]["tally_txns"][i]
                tally.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
                tally_transactions.append(tally.process_transaction(call_from))
        return tally_transactions

    def process_tapi_signals(self):
        is_tapi, tapi_bit = False, -1
        for start_epoch, stop_epoch, bit in self.tapi_periods:
            if self.block_epoch >= start_epoch and self.block_epoch <= stop_epoch:
                is_tapi = True
                tapi_bit = bit
                break

        if is_tapi:
            tapi_signal = self.block["block_header"]["signals"]
            tapi_accept = (tapi_signal & (1 << tapi_bit)) != 0
            return tapi_accept
        else:
            return None

    def return_block_error(self, message):
        self.logger.warning(message)
        return {
            "type": "block",
            "error": message.lower(),
        }