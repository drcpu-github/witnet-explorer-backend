from blockchain.transactions.transaction import Transaction
from schemas.component.mint_schema import (
    MintTransactionForApi,
    MintTransactionForExplorer,
)


class Mint(Transaction):
    def process_transaction(self, block_signature):
        self.txn_details["miner"] = self.address_generator.signature_to_address(
            block_signature["compressed"], block_signature["bytes"]
        )

        # Collect output values
        output_addresses, output_values, _ = self.get_outputs(self.json_txn["outputs"])
        self.txn_details["output_addresses"] = output_addresses
        self.txn_details["output_values"] = output_values

        return MintTransactionForExplorer().load(self.txn_details)

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                mint_txns.miner,
                mint_txns.output_addresses,
                mint_txns.output_values,
                mint_txns.epoch
            FROM
                mint_txns
            LEFT JOIN
                blocks
            ON
                mint_txns.epoch=blocks.epoch
            WHERE
                txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(txn_hash)],
        )

        if result:
            (
                block_hash,
                block_confirmed,
                block_reverted,
                miner,
                output_addresses,
                output_values,
                epoch,
            ) = result

            block_hash = block_hash.hex()

            txn_epoch = epoch
            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            return MintTransactionForApi().load(
                {
                    "hash": txn_hash,
                    "block": block_hash,
                    "epoch": txn_epoch,
                    "timestamp": txn_time,
                    "miner": miner,
                    "output_addresses": output_addresses,
                    "output_values": output_values,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}
