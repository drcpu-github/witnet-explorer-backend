from transactions.transaction import Transaction
from schemas.component.commit_schema import CommitTransactionForApi, CommitTransactionForBlock, CommitTransactionForDataRequest, CommitTransactionForExplorer

class Commit(Transaction):
    def process_transaction(self, call_from):
        # If we create a DataRequest from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["Commit"]

        # Calculate transaction addresses
        addresses = self.calculate_addresses(self.json_txn["signatures"])
        assert len(list(set(addresses))) == 1
        self.txn_details["address"] = addresses[0]

        # Data request transaction hash
        self.txn_details["data_request"] = self.json_txn["body"]["dr_pointer"]

        # Collect input / output details
        input_utxos, input_values = self.get_inputs(addresses * len(self.json_txn["body"]["collateral"]), self.json_txn["body"]["collateral"])
        _, output_values, _ = self.get_outputs(self.json_txn["body"]["outputs"])

        self.txn_details["collateral"] = sum(input_values) - sum(output_values)

        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos
            self.txn_details["input_values"] = input_values

            assert len(output_values) <= 1, "Unexpectedly found multiple output values"
            if len(output_values) == 1:
                self.txn_details["output_value"] = output_values[0]
            else:
                self.txn_details["output_value"] = None
            return CommitTransactionForExplorer().load(self.txn_details)
        else:
            return CommitTransactionForBlock().load(self.txn_details)

    def get_data_request_hash(self, txn_hash):
        sql = """
            SELECT
                data_request
            FROM
                commit_txns
            WHERE
                commit_txns.txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(sql, parameters=[bytearray.fromhex(txn_hash)])

        if result:
            return result[0].hex()
        else:
            return {"error": "transaction not found"}

    def get_commits_for_data_request(self, data_request_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                commit_txns.txn_hash,
                commit_txns.txn_address,
                commit_txns.epoch
            FROM
                commit_txns
            LEFT JOIN
                blocks
            ON
                commit_txns.epoch=blocks.epoch
            WHERE
                commit_txns.data_request=%s
            ORDER BY
                commit_txns.epoch
            DESC
        """
        results = self.database.sql_return_all(sql, parameters=[bytearray.fromhex(data_request_hash)])

        if results == None:
            return []

        commits = []
        found_confirmed, found_mined = False, False
        for commit in results:
            block_hash, block_confirmed, block_reverted, txn_hash, txn_address, epoch = commit

            timestamp = self.start_time + (epoch + 1) * self.epoch_period

            if block_confirmed:
                found_confirmed = True
            elif not block_reverted:
                found_mined = True

            # Do not append reverted reveals when there are newer mined or confirmed reveals
            # Note that this requires commit transaction to be sorted by epoch in descending order
            if (found_confirmed or found_mined) and block_reverted:
                continue

            # No block found for this commit, most likely it was reverted and deleted
            if block_hash is None:
                continue

            commits.append(
                {
                    "block": block_hash.hex(),
                    "hash": txn_hash.hex(),
                    "address": txn_address,
                    "epoch": epoch,
                    "timestamp": timestamp,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )

        return commits

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                commit_txns.txn_address,
                commit_txns.input_values,
                commit_txns.input_utxos,
                commit_txns.output_value,
                commit_txns.epoch
            FROM
                commit_txns
            LEFT JOIN
                blocks
            ON
                commit_txns.epoch=blocks.epoch
            WHERE
                txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(txn_hash)],
            custom_types=["utxo"],
        )

        if result:
            block_hash, block_confirmed, block_reverted, txn_address, input_values, input_utxos, output_value, epoch = result

            input_utxo_values = []
            for value, (input_txn, input_idx) in zip(input_values, input_utxos):
                input_utxo_values.append(
                    {
                        "address": txn_address,
                        "value": value,
                        "input_utxo": f"{input_txn.hex()}:{input_idx}",
                    }
                )

            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            return CommitTransactionForApi().load(
                {
                    "hash": txn_hash,
                    "block": block_hash.hex(),
                    "address": txn_address,
                    "input_utxos": input_utxo_values,
                    "output_value": output_value,
                    "epoch": epoch,
                    "timestamp": txn_time,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}
