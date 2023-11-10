import logging
import logging.handlers
import psycopg2
import time

from node.witnet_node import WitnetNode

from objects.wip import WIP

from util.address_generator import AddressGenerator
from util.database_manager import DatabaseManager
from util.protobuf_encoder import ProtobufEncoder
from util.radon_translator import RadonTranslator

class Transaction(object):
    def __init__(self, consensus_constants, logger=None, database=None, database_config=None, node_config=None):
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period
        self.collateral_minimum = consensus_constants.collateral_minimum

        # Connect to the database
        if database:
            self.database = database
        elif database_config:
            self.database = DatabaseManager(database_config, logger=logger, custom_types=["utxo", "filter"])
        else:
            self.database = None

        # Save node pool config
        self.node_config = node_config

        # Set up logger
        if logger:
            self.logger = logger
        else:
            self.logger = None

        # Create address generator
        self.address_generator = AddressGenerator("wit")

        # Create Protobuf encoder
        self.protobuf_encoder = None
        if database_config != None:
            self.protobuf_encoder = ProtobufEncoder(WIP(database_config=database_config))

        # Create Radon translator
        self.translator = RadonTranslator()

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def set_transaction(self, txn_hash="", txn_epoch=0, txn_weight=0, json_txn=None):
        self.txn_hash = txn_hash

        self.txn_details = {}
        self.txn_details["txn_hash"] = txn_hash
        if txn_epoch > 0:
            self.txn_details["epoch"] = txn_epoch

        if json_txn:
            self.json_txn = json_txn
            self.txn_details["weight"] = txn_weight
        else:
            self.json_txn = self.get_transaction_from_node(txn_hash)
            if "error" in self.json_txn:
                self.json_txn = {}
                self.txn_details["weight"] = 0
            else:
                self.txn_details["weight"] = self.json_txn["weight"]

        if self.protobuf_encoder:
            self.protobuf_encoder.set_transaction(self.json_txn)

    def calculate_addresses(self, signatures):
        addresses = []
        for signature in signatures:
            public_key = signature["public_key"]
            address = self.address_generator.signature_to_address(public_key["compressed"], public_key["bytes"])
            addresses.append(address)
        return addresses

    def get_inputs(self, addresses, txn_inputs):
        assert self.database != None
        assert len(addresses) == len(txn_inputs)

        input_utxos, input_values = [], []
        for address, txn_input in zip(addresses, txn_inputs):
            # Get the transaction and output index from the output pointer
            input_hash = txn_input["output_pointer"].split(":")[0]
            input_index = int(txn_input["output_pointer"].split(":")[1])

            hash_bytes = bytearray.fromhex(input_hash)
            input_utxos.append((hash_bytes, input_index))

            # Try to find the transaction input value in the database
            outputs = None
            sql = "SELECT type FROM hashes WHERE hash=%s LIMIT 1" % psycopg2.Binary(hash_bytes)
            result = self.database.sql_return_one(sql)
            if result:
                sql = "SELECT output_values FROM %s WHERE txn_hash=%s LIMIT 1" % (result[0] + "s", psycopg2.Binary(hash_bytes))
                outputs = self.database.sql_return_one(sql)
                if outputs:
                    input_values.append(outputs[0][input_index])

            # Fall back: transaction not found in database, fetch it from the node
            if not outputs:
                self.logger.info(f"Could not find input {txn_input['output_pointer']} for transaction {self.txn_hash} in database")
                # Get the transaction
                input_txn = self.get_transaction_from_node(input_hash)
                if "error" in input_txn:
                    self.logger.error(f"Could not fetch all inputs for transaction: {input_txn['error']}")
                    return 0, [], [], []

                # Figure out the transaction type as the parsing depends on that
                transaction_type = list(input_txn["transaction"].keys())[0]
                if transaction_type in ("Tally", "Mint"):
                    outputs = input_txn["transaction"][transaction_type]["outputs"]
                    # Append the correct output to the list of input_values
                    input_values.append(outputs[input_index]["value"])
                elif list(input_txn["transaction"].keys())[0] in ("DataRequest", "Commit", "ValueTransfer"):
                    outputs = input_txn["transaction"][transaction_type]["body"]["outputs"]
                    # Append the correct output to the list of input_values
                    input_values.append(outputs[input_index]["value"])
                else:
                    self.logger.error("Unexpected transaction type when querying ValueTransfer inputs")

        return input_utxos, input_values

    def get_outputs(self, txn_outputs):
        output_addresses, output_values, timelocks = [], [], []
        for out in txn_outputs:
            output_addresses.append(out["pkh"])
            output_values.append(out["value"])
            timelocks.append(out["time_lock"])

        return output_addresses, output_values, timelocks

    def get_transaction_from_node(self, txn_hash):
        # Create connection to the node pool
        witnet_node = WitnetNode(self.node_config, logger=self.logger)

        transaction = witnet_node.get_transaction(txn_hash)
        while "error" in transaction:
            # All our nodes in the pool were busy, retry as soon as possible
            if transaction["reason"] == "no available nodes found":
                self.logger.warning("No available nodes found")
                time.sleep(1)
                transaction = witnet_node.get_transaction(txn_hash)
            # No synced nodes: give them some time to sync again and retry
            elif transaction["reason"] == "no synced nodes found":
                self.logger.warning("No synced nodes found")
                time.sleep(60)
                transaction = witnet_node.get_transaction(txn_hash)
            # Another error, do not retry
            else:
                self.logger.error(f"Failed to get transaction: {transaction['error']}")
                return transaction

        return transaction["result"]
