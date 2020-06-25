import psycopg2

from transactions.transaction import Transaction

class Mint(Transaction):
    def process_transaction(self, block_signature):
        if self.json_txn == {}:
            return self.json_txn

        self.txn_details["miner"] = self.address_generator.signature_to_address(block_signature["compressed"], block_signature["bytes"])

        # Collect output values
        output_addresses, output_values, _ = self.get_outputs(self.json_txn["outputs"])
        self.txn_details["output_addresses"] = output_addresses
        self.txn_details["output_values"] = output_values

        return self.txn_details

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                mint_txns.output_addresses,
                mint_txns.output_values,
                mint_txns.epoch
            FROM mint_txns
            LEFT JOIN blocks ON
                mint_txns.epoch=blocks.epoch
            WHERE
                txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytearray.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            block_hash, block_confirmed, block_reverted, output_addresses, output_values, epoch = result

            block_hash = block_hash.hex()

            mint_outputs = list(zip(output_addresses, output_values))

            txn_epoch = epoch
            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            if block_confirmed:
                status = "confirmed"
            elif block_reverted:
                status = "reverted"
            else:
                status = "mined"
        else:
            block_hash = ""
            mint_outputs = []
            txn_epoch = ""
            txn_time = ""
            status = "transaction not found"

        return {
            "type": "mint_txn",
            "txn_hash": txn_hash,
            "block_hash": block_hash,
            "mint_outputs": mint_outputs,
            "txn_epoch": txn_epoch,
            "txn_time": txn_time,
            "status": status,
        }
