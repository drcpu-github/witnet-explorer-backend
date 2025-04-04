import logging
import logging.handlers
import time

from psycopg.sql import SQL, Identifier

from blockchain.config import BlockchainConfig
from blockchain.objects.wip import WIP
from mockups.database import MockDatabase
from mockups.witnet_node import MockWitnetNode
from node.witnet_node import WitnetNode
from util.address_generator import AddressGenerator
from util.data_transformer import re_sql
from util.database_manager import DatabaseManager
from util.protobuf_encoder import ProtobufEncoder


class Transaction(object):
    def __init__(
        self,
        database=None,
        logger=None,
        witnet_node=None,
    ):
        self.consensus_constants = BlockchainConfig.consensus_constants
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period
        self.collateral_minimum = self.consensus_constants.collateral_minimum

        # Get the network type
        network_type = BlockchainConfig.config["environment"]["network"]

        # Connect to the database
        if database is not None:
            self.database = database
        else:
            if network_type == "pytest":
                self.database = MockDatabase()
            else:
                self.database = DatabaseManager(
                    logger=logger, custom_types=["utxo", "filter"]
                )

        # Set up logger
        if logger:
            self.logger = logger
        else:
            self.logger = None

        # If a witnet node pool connection was passed through, save it here for later use
        self.witnet_node = None
        if witnet_node is not None:
            self.witnet_node = witnet_node
        # Creating a mockup node is not expensive
        elif network_type == "pytest":
            self.witnet_node = MockWitnetNode()
        # Defer creating a node pool connection until we need it in get_transaction_from_node

        # Create address generator
        address_prefix = None
        if network_type == "mainnet":
            address_prefix = "wit"
        elif network_type in ("pytest", "testnet"):
            address_prefix = "twit"
        assert address_prefix, "Need to properly set the network type"
        self.address_generator = AddressGenerator(address_prefix)

        # Create Protobuf encoder
        if network_type in ("mainnet", "testnet"):
            self.protobuf_encoder = ProtobufEncoder(WIP(database=self.database))
        elif network_type == "pytest":
            self.protobuf_encoder = ProtobufEncoder(WIP(mockup=True))
        assert self.protobuf_encoder, "Need to properly set the network type"

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def set_transaction(self, txn_hash, txn_epoch, txn_weight=0, json_txn=None):
        self.txn_hash = txn_hash

        self.txn_details = {}
        self.txn_details["hash"] = txn_hash
        self.txn_details["epoch"] = txn_epoch
        if txn_weight != 0:
            self.txn_details["weight"] = txn_weight

        self.json_txn = json_txn
        if self.json_txn is None:
            self.json_txn = self.get_transaction_from_node(txn_hash)
            if "error" in self.json_txn:
                raise ValueError(self.json_txn["error"])
            if self.json_txn["weight"] != 0:
                self.txn_details["weight"] = self.json_txn["weight"]

        if self.protobuf_encoder:
            self.protobuf_encoder.set_transaction(self.json_txn)

    def calculate_addresses(self, signatures):
        addresses = []
        for signature in signatures:
            public_key = signature["public_key"]
            address = self.address_generator.signature_to_address(
                public_key["compressed"], public_key["bytes"]
            )
            addresses.append(address)
        return addresses

    def get_inputs(self, txn_inputs):
        input_utxos, input_values = [], []
        for txn_input in txn_inputs:
            # Get the transaction and output index from the output pointer
            input_hash = txn_input["output_pointer"].split(":")[0]
            input_index = int(txn_input["output_pointer"].split(":")[1])

            hash_bytes = bytearray.fromhex(input_hash)
            input_utxos.append((hash_bytes, input_index))

            # Try to find the transaction input value in the database
            outputs = None
            sql = """
                SELECT
                    type
                FROM
                    hashes
                WHERE
                    hash=%s
                LIMIT 1
            """
            hash_type = self.database.sql_return_one(sql, parameters=[hash_bytes])
            if hash_type:
                # Build SQL statement
                sql = """
                    SELECT
                        {column_name}
                    FROM
                        {table_name}
                    WHERE
                        txn_hash=%s
                    LIMIT 1
                """
                if hash_type[0] in (
                    "data_request_txn",
                    "commit_txn",
                ):
                    assert input_index == 0, "Unexpectedly found a non-zero input index"
                    sql = SQL(re_sql(sql)).format(
                        column_name=Identifier("output_value"),
                        table_name=Identifier(f"{hash_type[0]}s"),
                    )
                elif hash_type[0] == "stake_txn":
                    assert input_index == 0, "Unexpectedly found a non-zero input index"
                    sql = SQL(re_sql(sql)).format(
                        column_name=Identifier("change_value"),
                        table_name=Identifier(f"{hash_type[0]}s"),
                    )
                elif hash_type[0] == "unstake_txn":
                    assert input_index == 0, "Unexpectedly found a non-zero input index"
                    sql = SQL(re_sql(sql)).format(
                        column_name=Identifier("unstake_value"),
                        table_name=Identifier(f"{hash_type[0]}s"),
                    )
                else:
                    sql = SQL(re_sql(sql)).format(
                        column_name=Identifier("output_values"),
                        table_name=Identifier(f"{hash_type[0]}s"),
                    )

                outputs = self.database.sql_return_one(sql, parameters=[hash_bytes])
                if outputs:
                    if type(outputs[0]) is int:
                        input_values.append(outputs[0])
                    else:
                        input_values.append(outputs[0][input_index])

            # Fall back: transaction not found in database, fetch it from the node
            if not outputs:
                if self.logger:
                    self.logger.info(
                        f"Could not find input {txn_input['output_pointer']} for transaction {self.txn_hash} in database"
                    )
                # Get the transaction
                input_txn = self.get_transaction_from_node(input_hash)
                if "error" in input_txn:
                    if self.logger:
                        self.logger.error(
                            f"Could not fetch all inputs for transaction: {input_txn['error']}"
                        )
                    return [], []

                # Figure out the transaction type as the parsing depends on that
                txn_type = list(input_txn["transaction"].keys())[0]
                if txn_type in ("Tally", "Mint"):
                    outputs = input_txn["transaction"][txn_type]["outputs"]
                    # Append the correct output to the list of input_values
                    input_values.append(outputs[input_index]["value"])
                elif txn_type in ("DataRequest", "Commit", "ValueTransfer"):
                    outputs = input_txn["transaction"][txn_type]["body"]["outputs"]
                    # Append the correct output to the list of input_values
                    input_values.append(outputs[input_index]["value"])
                elif txn_type == "Stake":
                    output = input_txn["transaction"][txn_type]["body"]["change"]
                    # The stake output is not an array
                    input_values.append(output["value"])
                elif txn_type == "Unstake":
                    output = input_txn["transaction"][txn_type]["body"]["withdrawal"]
                    # The unstake output is not an array
                    input_values.append(output["value"])
                else:
                    if self.logger:
                        self.logger.error(
                            "Unexpected transaction type when querying ValueTransfer inputs"
                        )

        return input_utxos, input_values

    def get_outputs(self, txn_outputs):
        output_addresses, output_values, timelocks = [], [], []
        for out in txn_outputs:
            output_addresses.append(out["pkh"])
            output_values.append(out["value"])
            timelocks.append(out["time_lock"])

        return output_addresses, output_values, timelocks

    def get_transaction_from_node(self, txn_hash):
        # Connect to the witnet node pool if no connection exists yet
        if self.witnet_node is None:
            self.witnet_node = WitnetNode(logger=self.logger)

        transaction = self.witnet_node.get_transaction(txn_hash)
        while "error" in transaction:
            # All our nodes in the pool were busy, retry as soon as possible
            if transaction["reason"] == "no available nodes found":
                if self.logger:
                    self.logger.warning("No available nodes found")
                time.sleep(1)
                transaction = self.witnet_node.get_transaction(txn_hash)
            # No synced nodes: give them some time to sync again and retry
            elif transaction["reason"] == "no synced nodes found":
                if self.logger:
                    self.logger.warning("No synced nodes found")
                time.sleep(60)
                transaction = self.witnet_node.get_transaction(txn_hash)
            elif transaction["reason"].startswith("Timed out after"):
                if self.logger:
                    self.logger.warning(
                        "Fetching input transaction timed out, retrying in one second"
                    )
                time.sleep(1)
                transaction = self.witnet_node.get_transaction(txn_hash)
            # Another error, do not retry
            else:
                if self.logger:
                    self.logger.error(
                        f"Failed to get transaction: {transaction['error']}"
                    )
                return transaction

        return transaction["result"]
