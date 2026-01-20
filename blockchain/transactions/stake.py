from blockchain.transactions.transaction import Transaction
from schemas.component.stake_schema import (
    StakeTransactionForApi,
    StakeTransactionForBlock,
    StakeTransactionForExplorer,
)
from util.blockchain_functions import calculate_timestamp_from_epoch


class Stake(Transaction):
    def process_transaction(self, call_from):
        # If we create a Stake from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["Stake"]

        # Calculate transaction addresses
        self.txn_details["input_addresses"] = self.calculate_addresses(
            self.json_txn["signatures"]
        )

        # Collect input details
        input_utxos, input_values = self.get_inputs(self.json_txn["body"]["inputs"])
        self.txn_details["input_values"] = input_values

        # Collect stake details
        stake_output = self.json_txn["body"]["output"]
        self.txn_details["validator"] = stake_output["key"]["validator"]
        self.txn_details["withdrawer"] = stake_output["key"]["withdrawer"]
        self.txn_details["stake_value"] = stake_output["value"]

        # Collect output details if there is a change output
        if self.json_txn["body"]["change"]:
            change_addresses, change_values, _ = self.get_outputs(
                [self.json_txn["body"]["change"]]
            )
            assert (
                len(change_addresses) == 1 and len(change_values) == 1
            ), f"Expected at most one output in stake transaction {self.txn_hash}"
            self.txn_details["change_address"] = change_addresses[0]
            self.txn_details["change_value"] = change_values[0]
        else:
            self.txn_details["change_address"] = None
            self.txn_details["change_value"] = None

        if self.json_txn["body"]["change"]:
            self.txn_details["fee"] = (
                sum(input_values) - sum(change_values) - self.txn_details["stake_value"]
            )
        else:
            self.txn_details["fee"] = (
                sum(input_values) - self.txn_details["stake_value"]
            )

        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos

            return StakeTransactionForExplorer().load(self.txn_details)

        if call_from == "api":
            self.txn_details["priority"] = max(
                1, int(self.txn_details["fee"] / self.txn_details["weight"])
            )
            self.txn_details["timestamp"] = calculate_timestamp_from_epoch(
                self.txn_details["epoch"]
            )

            # Delete fields not used in the frontend to display a block
            del self.txn_details["input_addresses"]
            del self.txn_details["input_values"]
            del self.txn_details["change_address"]
            del self.txn_details["change_value"]

            return StakeTransactionForBlock().load(self.txn_details)

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                stake_txns.input_addresses,
                stake_txns.input_values,
                stake_txns.change_address,
                stake_txns.change_value,
                stake_txns.weight,
                stake_txns.validator,
                stake_txns.withdrawer,
                stake_txns.stake_value
            FROM stake_txns
            LEFT JOIN blocks ON
                stake_txns.epoch=blocks.epoch
            WHERE
                stake_txns.txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(txn_hash)],
            custom_types=["utxo"],
        )

        if result:
            (
                block_hash,
                block_epoch,
                block_confirmed,
                block_reverted,
                input_addresses,
                input_values,
                change_address,
                change_value,
                weight,
                validator,
                withdrawer,
                stake_value,
            ) = result

            if change_value:
                txn_fee = sum(input_values) - change_value - stake_value
            else:
                txn_fee = sum(input_values) - stake_value
            # Get an integer value for the weighted fee
            txn_priority = max(1, int(txn_fee / weight))

            # Return all inputs separate
            address_value_utxos = []
            for address, value in zip(input_addresses, input_values):
                address_value_utxos.append(
                    {
                        "address": address,
                        "value": value,
                    }
                )

            txn_epoch = block_epoch
            txn_time = calculate_timestamp_from_epoch(block_epoch)

            return StakeTransactionForApi().load(
                {
                    "block": block_hash.hex(),
                    "hash": txn_hash,
                    "epoch": txn_epoch,
                    "inputs": address_value_utxos,
                    "change_address": change_address,
                    "change_value": change_value,
                    "validator": validator,
                    "withdrawer": withdrawer,
                    "stake_value": stake_value,
                    "fee": txn_fee,
                    "weight": weight,
                    "priority": txn_priority,
                    "timestamp": txn_time,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}
