import logging
import logging.handlers
import time

from node.witnet_node import WitnetNode

from transactions.mint import Mint
from transactions.value_transfer import ValueTransfer
from transactions.data_request import DataRequest
from transactions.commit import Commit
from transactions.reveal import Reveal
from transactions.tally import Tally

class Block(object):
    def __init__(self, block_hash, consensus_constants, logging_queue, db_config=None, block=None, tapi_periods=[], node_config=None):
        self.block_hash = block_hash

        self.consensus_constants = consensus_constants
        self.collateral_minimum = consensus_constants.collateral_minimum
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period
        self.superblock_period = consensus_constants.superblock_period

        # Set up logger
        self.configure_logging_process(logging_queue, "block")
        self.logger = logging.getLogger("block")
        self.logging_queue = logging_queue

        self.db_config = db_config
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
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def get_block(self):
        # Connect to node pool
        socket_host = self.node_config["host"]
        socket_port = self.node_config["port"]
        self.witnet_node = WitnetNode(socket_host, socket_port, 15, self.logging_queue, "node-block")

        block = self.witnet_node.get_block(self.block_hash)
        if type(block) is dict and "error" in block:
            self.logger.warning(f"Unable to fetch block {self.block_hash}: {block}")

        return block["result"]

    def process_block(self, call_from):
        assert call_from == "explorer" or call_from == "api"

        if "reason" in self.block:
            return {
                "type": "block",
                "error": self.block["reason"],
            }

        self.process_details()

        return {
            "type": "block",
            "details": {
                "block_hash": self.block_hash,
                "epoch": self.block_epoch,
                "time": self.start_time + (self.block_epoch + 1) * self.epoch_period,
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

    def process_details(self):
        try:
            self.block_epoch = self.block["block_header"]["beacon"]["checkpoint"]
        except KeyError:
            self.logger.error(f"Unable to process block: {self.block}")

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
        mint = Mint(self.consensus_constants, self.logging_queue)
        mint.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
        return mint.process_transaction(block_signature)

    def process_value_transfer_txns(self, call_from):
        value_transfer_txns = []
        if len(self.block["txns_hashes"]["value_transfer"]) > 0:
            value_transfer = ValueTransfer(self.consensus_constants, self.logging_queue, database_config=self.db_config, node_config=self.node_config)
            for i, (txn_hash, txn_weight) in enumerate(zip(self.block["txns_hashes"]["value_transfer"], self.block["txns_weights"]["value_transfer"])):
                json_txn = self.block["txns"]["value_transfer_txns"][i]
                value_transfer.set_transaction(txn_hash=txn_hash, txn_weight=txn_weight, json_txn=json_txn)
                value_transfer_txns.append(value_transfer.process_transaction(call_from))
        return value_transfer_txns

    def process_data_request_txns(self, call_from):
        data_request_transactions = []
        if len(self.block["txns_hashes"]["data_request"]) > 0:
            data_request = DataRequest(self.consensus_constants, self.logging_queue, database_config=self.db_config, node_config=self.node_config)
            for i, (txn_hash, txn_weight) in enumerate(zip(self.block["txns_hashes"]["data_request"], self.block["txns_weights"]["data_request"])):
                json_txn = self.block["txns"]["data_request_txns"][i]
                data_request.set_transaction(txn_hash=txn_hash, txn_weight=txn_weight, json_txn=json_txn)
                data_request_transactions.append(data_request.process_transaction(call_from))
        return data_request_transactions

    def process_commit_txns(self, call_from):
        commit_transactions = []
        if len(self.block["txns_hashes"]["commit"]) > 0:
            commit = Commit(self.consensus_constants, self.logging_queue, database_config=self.db_config, node_config=self.node_config)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["commit"]):
                json_txn = self.block["txns"]["commit_txns"][i]
                commit.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
                commit_transactions.append(commit.process_transaction(call_from))
        return commit_transactions

    def process_reveal_txns(self, call_from):
        reveal_transactions = []
        if len(self.block["txns_hashes"]["reveal"]) > 0:
            reveal = Reveal(self.consensus_constants, self.logging_queue)
            for i, txn_hash in enumerate(self.block["txns_hashes"]["reveal"]):
                json_txn = self.block["txns"]["reveal_txns"][i]
                reveal.set_transaction(txn_hash=txn_hash, json_txn=json_txn)
                reveal_transactions.append(reveal.process_transaction(call_from))
        return reveal_transactions

    def process_tally_txns(self, call_from):
        tally_transactions = []
        if len(self.block["txns_hashes"]["tally"]) > 0:
            tally = Tally(self.consensus_constants, self.logging_queue)
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