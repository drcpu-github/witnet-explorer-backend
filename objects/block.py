import logging
import logging.handlers
import time

from util.database_manager import DatabaseManager

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
            self.database = database
        elif database_config:
            self.database = DatabaseManager(database_config, logger=self.logger)
        else:
            self.database = None

        if database_config:
            self.database_config = database_config

        self.node_config = node_config

        self.current_epoch = (int(time.time()) - self.start_time) // self.epoch_period

        if block == None:
            self.block = self.get_block()
        else:
            self.block = block
        self.block_json = None

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
            if not self.database:
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
            block_hash = self.database.sql_return_one(sql)

            if block_hash:
                self.block_hash = block_hash[0].hex()
            else:
                return self.return_block_error(f"Could not find block for epoch {self.block_epoch}")

        # Connect to node pool
        witnet_node = WitnetNode(self.node_config, logger=self.logger)

        block = witnet_node.get_block(self.block_hash)
        if type(block) is dict and "error" in block:
            if self.logger:
                self.logger.warning(f"Unable to fetch block {self.block_hash}: {block}")
            return block

        return block["result"]

    def process_block(self, call_from):
        assert call_from == "explorer" or call_from == "api"

        if "error" in self.block:
            return {
                "type": "block",
                "status": "error",
                "error": self.block["error"],
            }

        if "reason" in self.block:
            return {
                "type": "block",
                "status": "error",
                "error": self.block["reason"],
            }

        self.process_details()

        self.block_json = {
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
            "tapi_signals": self.process_tapi_signals(),
        }

        if call_from == "api":
            self.process_block_for_api()

        return self.block_json

    def process_block_for_api(self):
        # Add number of commits and reveals
        self.block_json["number_of_commits"] = len(self.block_json["commit_txns"])
        self.block_json["number_of_reveals"] = len(self.block_json["reveal_txns"])

        # Group commits per data request
        commits_for_data_request = {}
        for commit in self.block_json["commit_txns"]:
            if not commit["data_request"] in commits_for_data_request:
                commits_for_data_request[commit["data_request"]] = {
                    "collateral": commit["collateral"],
                    "txn_address": [],
                    "txn_hash": [],
                }
            commits_for_data_request[commit["data_request"]]["txn_address"].append(commit["txn_address"])
            commits_for_data_request[commit["data_request"]]["txn_hash"].append(commit["txn_hash"])
        self.block_json["commit_txns"] = commits_for_data_request

        # Group reveals per data request
        reveals_for_data_request = {}
        for reveal in self.block_json["reveal_txns"]:
            if not reveal["data_request"] in reveals_for_data_request:
                reveals_for_data_request[reveal["data_request"]] = {
                    "reveal_translation": [],
                    "success": [],
                    "txn_address": [],
                    "txn_hash": [],
                }
            reveals_for_data_request[reveal["data_request"]]["reveal_translation"].append(reveal["reveal_translation"])
            reveals_for_data_request[reveal["data_request"]]["success"].append(1 if reveal["success"] else 0)
            reveals_for_data_request[reveal["data_request"]]["txn_address"].append(reveal["txn_address"])
            reveals_for_data_request[reveal["data_request"]]["txn_hash"].append(reveal["txn_hash"])
        self.block_json["reveal_txns"] = reveals_for_data_request

        del self.block_json["tapi_signals"]

    def process_details(self):
        try:
            self.block_epoch = self.block["block_header"]["beacon"]["checkpoint"]
        except KeyError:
            if self.logger:
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
        txn_epoch = self.block_epoch
        json_txn = self.block["txns"]["mint"]
        block_signature = self.block["block_sig"]["public_key"]
        mint = Mint(self.consensus_constants, logger=self.logger)
        mint.set_transaction(txn_hash=txn_hash, txn_epoch=txn_epoch, json_txn=json_txn)
        return mint.process_transaction(block_signature)

    def process_value_transfer_txns(self, call_from):
        value_transfer_txns = []
        if len(self.block["txns_hashes"]["value_transfer"]) > 0:
            txn_epoch = self.block_epoch
            value_transfer = ValueTransfer(self.consensus_constants, logger=self.logger, database=self.database, node_config=self.node_config)
            for i, (txn_hash, txn_weight) in enumerate(zip(self.block["txns_hashes"]["value_transfer"], self.block["txns_weights"]["value_transfer"])):
                json_txn = self.block["txns"]["value_transfer_txns"][i]
                value_transfer.set_transaction(txn_hash=txn_hash, txn_epoch=txn_epoch, txn_weight=txn_weight, json_txn=json_txn)
                value_transfer_txns.append(value_transfer.process_transaction(call_from))
        return value_transfer_txns

    def process_data_request_txns(self, call_from):
        data_request_transactions = []
        if len(self.block["txns_hashes"]["data_request"]) > 0:
            txn_epoch = self.block_epoch
            data_request = DataRequest(self.consensus_constants, logger=self.logger, database_config=self.database_config, node_config=self.node_config)
            for i, (txn_hash, txn_weight) in enumerate(zip(self.block["txns_hashes"]["data_request"], self.block["txns_weights"]["data_request"])):
                json_txn = self.block["txns"]["data_request_txns"][i]
                data_request.set_transaction(txn_hash=txn_hash, txn_epoch=txn_epoch, txn_weight=txn_weight, json_txn=json_txn)
                data_request_transactions.append(data_request.process_transaction(call_from))
        return data_request_transactions

    def process_commit_txns(self, call_from):
        commit_transactions = []
        if len(self.block["txns_hashes"]["commit"]) > 0:
            txn_epoch = self.block_epoch
            commit = Commit(self.consensus_constants, logger=self.logger, database=self.database, node_config=self.node_config)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["commit"]):
                json_txn = self.block["txns"]["commit_txns"][i]
                commit.set_transaction(txn_hash=txn_hash, txn_epoch=txn_epoch, json_txn=json_txn)
                commit_transactions.append(commit.process_transaction(call_from))
        return commit_transactions

    def process_reveal_txns(self, call_from):
        reveal_transactions = []
        if len(self.block["txns_hashes"]["reveal"]) > 0:
            txn_epoch = self.block_epoch
            reveal = Reveal(self.consensus_constants, logger=self.logger)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["reveal"]):
                json_txn = self.block["txns"]["reveal_txns"][i]
                reveal.set_transaction(txn_hash=txn_hash, txn_epoch=txn_epoch, json_txn=json_txn)
                reveal_transactions.append(reveal.process_transaction(call_from))
        return reveal_transactions

    def process_tally_txns(self, call_from):
        tally_transactions = []
        if len(self.block["txns_hashes"]["tally"]) > 0:
            txn_epoch = self.block_epoch
            tally = Tally(self.consensus_constants, logger=self.logger)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["tally"]):
                json_txn = self.block["txns"]["tally_txns"][i]
                tally.set_transaction(txn_hash=txn_hash, txn_epoch=txn_epoch, json_txn=json_txn)
                tally_transactions.append(tally.process_transaction(call_from))
        return tally_transactions

    def process_tapi_signals(self):
        is_tapi = False
        for start_epoch, stop_epoch, bit in self.tapi_periods:
            if self.block_epoch >= start_epoch and self.block_epoch <= stop_epoch:
                is_tapi = True
                break

        if is_tapi:
            return self.block["block_header"]["signals"]
        else:
            return None

    def process_addresses(self):
        address_dict = {}

        if self.block_json == None:
            self.process_block("explorer")

        # Add block miner
        address_dict[self.block_json["mint_txn"]["miner"]] = [1, 0, 0, 0, 0, 0, 0]

        # Add all addresses from the mint transaction
        for address in self.block_json["mint_txn"]["output_addresses"]:
            if address not in address_dict:
                address_dict[address] = [0, 1, 0, 0, 0, 0, 0]
            else:
                address_dict[address][1] += 1

        # Add all addresses which are used as value transfer inputs or outputs
        for value_transfer in self.block_json["value_transfer_txns"]:
            for address in set(value_transfer["input_addresses"]):
                if address not in address_dict:
                    address_dict[address] = [0, 0, 1, 0, 0, 0, 0]
                else:
                    address_dict[address][2] += 1
            for address in value_transfer["true_output_addresses"]:
                if address not in address_dict:
                    address_dict[address] = [0, 0, 1, 0, 0, 0, 0]
                else:
                    address_dict[address][2] += 1

        # Add all addresses which are used as inputs in a data request
        for data_request_txn in self.block_json["data_request_txns"]:
            for address in set(data_request_txn["input_addresses"]):
                if address not in address_dict:
                    address_dict[address] = [0, 0, 0, 1, 0, 0, 0]
                else:
                    address_dict[address][3] += 1

        # Add the address which is used as an input in a commit
        for commit in self.block_json["commit_txns"]:
            address = commit["txn_address"]
            if address not in address_dict:
                address_dict[address] = [0, 0, 0, 0, 1, 0, 0]
            else:
                address_dict[address][4] += 1

        # Add the address of a reveal
        for reveal in self.block_json["reveal_txns"]:
            address = reveal["txn_address"]
            if address not in address_dict:
                address_dict[address] = [0, 0, 0, 0, 0, 1, 0]
            else:
                address_dict[address][5] += 1

        # Add all addresses involved in a tally
        for tally in self.block_json["tally_txns"]:
            address_set = set(tally["output_addresses"]) | set(tally["error_addresses"]) | set(tally["liar_addresses"])
            for address in address_set:
                if address not in address_dict:
                    address_dict[address] = [0, 0, 0, 0, 0, 0, 1]
                else:
                    address_dict[address][6] += 1

        return [[address, self.block_epoch] + address_dict[address] for address in address_dict]

    def return_block_error(self, message):
        if self.logger:
            self.logger.warning(message)
        return {
            "type": "block",
            "error": message.lower(),
        }