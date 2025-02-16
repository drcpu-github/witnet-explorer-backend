from blockchain.transactions.transaction import Transaction
from schemas.component.unstake_schema import (
    UnstakeTransactionForApi,
    UnstakeTransactionForBlock,
    UnstakeTransactionForExplorer,
)
from util.blockchain_functions import calculate_timestamp_from_epoch


class Unstake(Transaction):
    def process_transaction(self, call_from):
        # If we create a Stake from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["Unstake"]

        # Collect unstake details
        self.txn_details["validator"] = self.json_txn["body"]["operator"]

        # Collect output details
        output_addresses, output_values, _ = self.get_outputs(
            [self.json_txn["body"]["withdrawal"]]
        )
        assert (
            len(output_addresses) == 1 and len(output_values) == 1
        ), f"Expected at most one output in unstake transaction {self.txn_hash}"
        self.txn_details["withdrawer"] = output_addresses[0]
        self.txn_details["unstake_value"] = output_values[0]

        self.txn_details["fee"] = self.json_txn["body"]["fee"]
        self.txn_details["nonce"] = self.json_txn["body"]["nonce"]

        if call_from == "explorer":
            return UnstakeTransactionForExplorer().load(self.txn_details)

        if call_from == "api":
            self.txn_details["priority"] = max(
                1, int(self.txn_details["fee"] / self.txn_details["weight"])
            )
            self.txn_details["timestamp"] = calculate_timestamp_from_epoch(
                self.txn_details["epoch"]
            )

            return UnstakeTransactionForBlock().load(self.txn_details)

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                unstake_txns.validator,
                unstake_txns.withdrawer,
                unstake_txns.unstake_value,
                unstake_txns.fee,
                unstake_txns.nonce,
                unstake_txns.weight
            FROM unstake_txns
            LEFT JOIN blocks ON
                unstake_txns.epoch=blocks.epoch
            WHERE
                unstake_txns.txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(txn_hash)],
        )

        if result:
            (
                block_hash,
                block_epoch,
                block_confirmed,
                block_reverted,
                validator,
                withdrawer,
                unstake_value,
                fee,
                nonce,
                weight,
            ) = result

            # Get an integer value for the weighted fee
            txn_priority = max(1, int(fee / weight))

            txn_epoch = block_epoch
            txn_time = calculate_timestamp_from_epoch(block_epoch)

            return UnstakeTransactionForApi().load(
                {
                    "block": block_hash.hex(),
                    "hash": txn_hash,
                    "epoch": txn_epoch,
                    "validator": validator,
                    "withdrawer": withdrawer,
                    "unstake_value": unstake_value,
                    "fee": fee,
                    "nonce": nonce,
                    "weight": weight,
                    "priority": txn_priority,
                    "timestamp": txn_time,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}
