import logging
import logging.handlers
import time

from blockchain.config import BlockchainConfig
from blockchain.transactions.commit import Commit
from blockchain.transactions.data_request import DataRequest
from blockchain.transactions.mint import Mint
from blockchain.transactions.reveal import Reveal
from blockchain.transactions.stake import Stake
from blockchain.transactions.tally import Tally
from blockchain.transactions.unstake import Unstake
from blockchain.transactions.value_transfer import ValueTransfer
from node.witnet_node import WitnetNode
from schemas.component.block_schema import BlockForApi, BlockForExplorer
from util.blockchain_functions import calculate_timestamp_from_epoch
from util.database_manager import DatabaseManager


class Block(object):
    def __init__(
        self,
        block=None,
        block_hash="",
        block_epoch=-1,
        logger=None,
        log_queue=None,
        database=None,
        tapi_periods=None,
        witnet_node=None,
    ):
        self.block = block
        self.block_hash = block_hash
        self.block_epoch = block_epoch

        self.consensus_constants = BlockchainConfig.consensus_constants
        self.collateral_minimum = self.consensus_constants.collateral_minimum
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period
        self.superblock_period = self.consensus_constants.superblock_period

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
        else:
            self.database = DatabaseManager(logger=self.logger)

        # Connect to node pool
        if witnet_node:
            self.witnet_node = witnet_node
        else:
            self.witnet_node = WitnetNode(logger=self.logger)

        self.current_epoch = (int(time.time()) - self.start_time) // self.epoch_period

        # Attempt to create the block
        if block is None:
            self.block = self.get_block()
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

            # Fetch block hash from the database
            sql = """
                SELECT
                    block_hash,
                    epoch
                FROM
                    blocks
                WHERE
                    epoch=%s
            """
            block_hash = self.database.sql_return_one(
                sql, parameters=[self.block_epoch]
            )

            if block_hash:
                self.block_hash = block_hash[0].hex()
            else:
                blockchain = self.witnet_node.get_blockchain(
                    epoch=self.block_epoch, num_blocks=1
                )
                if "error" in blockchain:
                    return self.return_block_error(
                        f"Could not find block for epoch {blockchain}"
                    )
                self.block_hash = blockchain["result"][0][1]

        block = self.witnet_node.get_block(self.block_hash)
        if "error" in block:
            if self.logger:
                self.logger.warning(f"Unable to fetch block {self.block_hash}: {block}")
            return self.return_block_error(
                f"Could not fetch block from node: {block['error']}"
            )

        return block["result"]

    def process_block(self, call_from):
        assert call_from == "explorer" or call_from == "api"

        if "error" in self.block:
            return self.block

        self.process_details()

        self.block_json = {
            "details": {
                "hash": self.block_hash,
                "epoch": self.block_epoch,
                "timestamp": calculate_timestamp_from_epoch(self.block_epoch),
                "data_request_weight": self.dr_weight,
                "value_transfer_weight": self.vt_weight,
                "stake_weight": self.st_weight,
                "unstake_weight": self.ut_weight,
                "weight": self.block_weight,
                "confirmed": self.confirmed,
                "reverted": self.reverted,
            },
            "transactions": {
                "mint": self.process_mint_txn(call_from),
                "value_transfer": self.process_value_transfer_txns(call_from),
                "data_request": self.process_data_request_txns(call_from),
                "commit": self.process_commit_txns(call_from),
                "reveal": self.process_reveal_txns(call_from),
                "tally": self.process_tally_txns(call_from),
                "stake": self.process_stake_txns(call_from),
                "unstake": self.process_unstake_txns(call_from),
            },
        }

        self.block_json["details"]["txns_fees"] = self.calculate_txns_fees()

        if call_from == "explorer":
            self.block_json["tapi"] = self.process_tapi_signals()
            return BlockForExplorer().load(self.block_json)

        if call_from == "api":
            self.process_block_for_api()
            return BlockForApi().load(self.block_json)

    def process_block_for_api(self):
        transactions = self.block_json["transactions"]

        # Add number of commits and reveals
        self.block_json["transactions"]["number_of_commits"] = len(
            transactions["commit"]
        )
        self.block_json["transactions"]["number_of_reveals"] = len(
            transactions["reveal"]
        )

        # Group commits per data request
        commits_for_data_request = {}
        for commit in transactions["commit"]:
            if not commit["data_request"] in commits_for_data_request:
                commits_for_data_request[commit["data_request"]] = []
            commits_for_data_request[commit["data_request"]].append(commit)
        self.block_json["transactions"]["commit"] = commits_for_data_request

        # Group reveals per data request
        reveals_for_data_request = {}
        for reveal in transactions["reveal"]:
            if not reveal["data_request"] in reveals_for_data_request:
                reveals_for_data_request[reveal["data_request"]] = []
            reveals_for_data_request[reveal["data_request"]].append(reveal)
        self.block_json["transactions"]["reveal"] = reveals_for_data_request

    def process_details(self):
        try:
            self.block_epoch = self.block["block_header"]["beacon"]["checkpoint"]
        except KeyError:
            if self.logger:
                self.logger.error(f"Unable to process block: {self.block}")

        self.dr_weight = self.block["dr_weight"]
        self.vt_weight = self.block["vt_weight"]
        self.st_weight = self.block["st_weight"] if "st_weight" in self.block else 0
        self.ut_weight = self.block["ut_weight"] if "ut_weight" in self.block else 0
        self.block_weight = self.block["block_weight"]

        self.confirmed = self.block["confirmed"]
        if (
            not self.confirmed
            and self.block_epoch < self.current_epoch - 2 * self.superblock_period
        ):
            self.reverted = True
        else:
            self.reverted = False

    def process_mint_txn(self, call_from):
        txn_hash = self.block["txns_hashes"]["mint"]
        json_txn = self.block["txns"]["mint"]
        block_signature = self.block["block_sig"]["public_key"]
        mint = Mint(
            database=self.database,
            logger=self.logger,
            witnet_node=self.witnet_node,
        )
        mint.set_transaction(txn_hash, self.block_epoch, json_txn=json_txn)
        return mint.process_transaction(block_signature, call_from)

    def process_value_transfer_txns(self, call_from):
        value_transfer_txns = []
        if len(self.block["txns_hashes"]["value_transfer"]) > 0:
            value_transfer = ValueTransfer(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, (txn_hash, txn_weight) in enumerate(
                zip(
                    self.block["txns_hashes"]["value_transfer"],
                    self.block["txns_weights"]["value_transfer"],
                )
            ):
                json_txn = self.block["txns"]["value_transfer_txns"][i]
                value_transfer.set_transaction(
                    txn_hash, self.block_epoch, txn_weight=txn_weight, json_txn=json_txn
                )
                value_transfer_txns.append(
                    value_transfer.process_transaction(call_from)
                )
        return value_transfer_txns

    def process_data_request_txns(self, call_from):
        data_request_transactions = []
        if len(self.block["txns_hashes"]["data_request"]) > 0:
            data_request = DataRequest(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, (txn_hash, txn_weight) in enumerate(
                zip(
                    self.block["txns_hashes"]["data_request"],
                    self.block["txns_weights"]["data_request"],
                )
            ):
                json_txn = self.block["txns"]["data_request_txns"][i]
                data_request.set_transaction(
                    txn_hash, self.block_epoch, txn_weight=txn_weight, json_txn=json_txn
                )
                data_request_transactions.append(
                    data_request.process_transaction(call_from)
                )
        return data_request_transactions

    def process_commit_txns(self, call_from):
        commit_transactions = []
        if len(self.block["txns_hashes"]["commit"]) > 0:
            commit = Commit(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, txn_hash in enumerate(self.block["txns_hashes"]["commit"]):
                json_txn = self.block["txns"]["commit_txns"][i]
                commit.set_transaction(txn_hash, self.block_epoch, json_txn=json_txn)
                commit_transactions.append(commit.process_transaction(call_from))
        return commit_transactions

    def process_reveal_txns(self, call_from):
        reveal_transactions = []
        if len(self.block["txns_hashes"]["reveal"]) > 0:
            reveal = Reveal(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, txn_hash in enumerate(self.block["txns_hashes"]["reveal"]):
                json_txn = self.block["txns"]["reveal_txns"][i]
                reveal.set_transaction(txn_hash, self.block_epoch, json_txn=json_txn)
                reveal_transactions.append(reveal.process_transaction(call_from))
        return reveal_transactions

    def process_tally_txns(self, call_from):
        tally_transactions = []
        if len(self.block["txns_hashes"]["tally"]) > 0:
            tally = Tally(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, txn_hash in enumerate(self.block["txns_hashes"]["tally"]):
                json_txn = self.block["txns"]["tally_txns"][i]
                tally.set_transaction(txn_hash, self.block_epoch, json_txn=json_txn)
                tally_transactions.append(tally.process_transaction(call_from))
        return tally_transactions

    def process_stake_txns(self, call_from):
        stake_transactions = []
        if (
            "stake" in self.block["txns_hashes"]
            and len(self.block["txns_hashes"]["stake"]) > 0
        ):
            stake = Stake(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, (txn_hash, txn_weight) in enumerate(
                zip(
                    self.block["txns_hashes"]["stake"],
                    self.block["txns_weights"]["stake"],
                )
            ):
                json_txn = self.block["txns"]["stake_txns"][i]
                stake.set_transaction(
                    txn_hash, self.block_epoch, txn_weight=txn_weight, json_txn=json_txn
                )
                stake_transactions.append(stake.process_transaction(call_from))
        return stake_transactions

    def process_unstake_txns(self, call_from):
        unstake_transactions = []
        if (
            "unstake" in self.block["txns_hashes"]
            and len(self.block["txns_hashes"]["unstake"]) > 0
        ):
            unstake = Unstake(
                database=self.database,
                logger=self.logger,
                witnet_node=self.witnet_node,
            )
            for i, (txn_hash, txn_weight) in enumerate(
                zip(
                    self.block["txns_hashes"]["unstake"],
                    self.block["txns_weights"]["unstake"],
                )
            ):
                json_txn = self.block["txns"]["unstake_txns"][i]
                unstake.set_transaction(
                    txn_hash, self.block_epoch, txn_weight=txn_weight, json_txn=json_txn
                )
                unstake_transactions.append(unstake.process_transaction(call_from))
        return unstake_transactions

    def calculate_txns_fees(self):
        txns_fees = 0

        transactions = self.block_json["transactions"]
        for value_transfer in transactions["value_transfer"]:
            txns_fees += value_transfer["fee"]
        for data_request in transactions["data_request"]:
            txns_fees += data_request["miner_fee"]
        for commit in transactions["commit"]:
            txns_fees += commit["fee"]
        for reveal in transactions["reveal"]:
            txns_fees += reveal["fee"]
        for stake in transactions["stake"]:
            txns_fees += stake["fee"]
        for unstake in transactions["unstake"]:
            txns_fees += unstake["fee"]

        return txns_fees

    def process_tapi_signals(self):
        is_tapi = False
        if self.tapi_periods:
            for start_epoch, stop_epoch, _ in self.tapi_periods:
                if self.block_epoch >= start_epoch and self.block_epoch <= stop_epoch:
                    is_tapi = True
                    break

        if is_tapi:
            return self.block["block_header"]["signals"]
        else:
            return None

    def process_addresses(self, as_dict=False):
        address_dict = {}

        transactions = self.block_json["transactions"]

        # Add block miner
        address_dict[transactions["mint"]["miner"]] = [1, 0, 0, 0, 0, 0, 0, 0, 0]

        # Add all addresses from the mint transaction
        for address in transactions["mint"]["output_addresses"]:
            if address not in address_dict:
                address_dict[address] = [0, 1, 0, 0, 0, 0, 0, 0, 0]
            else:
                address_dict[address][1] += 1

        # Add all addresses which are used as value transfer inputs or outputs
        for value_transfer in transactions["value_transfer"]:
            for address in set(value_transfer["input_addresses"]):
                if address not in address_dict:
                    address_dict[address] = [0, 0, 1, 0, 0, 0, 0, 0, 0]
                else:
                    address_dict[address][2] += 1
            true_output_addresses = set(value_transfer["output_addresses"]) - set(
                value_transfer["input_addresses"]
            )
            for address in true_output_addresses:
                if address not in address_dict:
                    address_dict[address] = [0, 0, 1, 0, 0, 0, 0, 0, 0]
                else:
                    address_dict[address][2] += 1

        # Add all addresses which are used as inputs in a data request
        for data_request_txn in transactions["data_request"]:
            for address in set(data_request_txn["input_addresses"]):
                if address not in address_dict:
                    address_dict[address] = [0, 0, 0, 1, 0, 0, 0, 0, 0]
                else:
                    address_dict[address][3] += 1

        # Add the address which is used as an input in a commit
        for commit in transactions["commit"]:
            address = commit["address"]
            if address not in address_dict:
                address_dict[address] = [0, 0, 0, 0, 1, 0, 0, 0, 0]
            else:
                address_dict[address][4] += 1

        # Add the address of a reveal
        for reveal in transactions["reveal"]:
            address = reveal["address"]
            if address not in address_dict:
                address_dict[address] = [0, 0, 0, 0, 0, 1, 0, 0, 0]
            else:
                address_dict[address][5] += 1

        # Add all addresses involved in a tally
        for tally in transactions["tally"]:
            address_set = (
                set(tally["output_addresses"])
                | set(tally["error_addresses"])
                | set(tally["liar_addresses"])
            )
            for address in address_set:
                if address not in address_dict:
                    address_dict[address] = [0, 0, 0, 0, 0, 0, 1, 0, 0]
                else:
                    address_dict[address][6] += 1

        # Add all addresses which are used as stake inputs or change output
        for stake in transactions["stake"]:
            input_addresses = set(stake["input_addresses"])
            for address in input_addresses:
                if address not in address_dict:
                    address_dict[address] = [0, 0, 0, 0, 0, 0, 0, 1, 0]
                else:
                    address_dict[address][7] += 1
            change_address = stake["change_address"]
            if change_address is not None and change_address not in input_addresses:
                if change_address not in address_dict:
                    address_dict[change_address] = [0, 0, 0, 0, 0, 0, 0, 1, 0]
                else:
                    address_dict[change_address][7] += 1

        # Add all addresses which are used in unstake transactions
        for unstake in transactions["unstake"]:
            withdrawer = unstake["withdrawer"]
            if withdrawer not in address_dict:
                address_dict[withdrawer] = [0, 0, 0, 0, 0, 0, 0, 0, 1]
            else:
                address_dict[withdrawer][8] += 1

        if as_dict:
            return address_dict
        else:
            return [
                [address, self.block_epoch] + address_dict[address]
                for address in address_dict
            ]

    def return_block_error(self, message):
        if self.logger:
            self.logger.warning(message)
        return {
            "type": "block",
            "error": message.lower(),
        }
