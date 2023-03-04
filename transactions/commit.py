import psycopg2

from transactions.transaction import Transaction

class Commit(Transaction):
    def process_transaction(self, call_from):
        if self.json_txn == {}:
            return self.json_txn

        # If we create a DataRequest from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["Commit"]

        # Calculate transaction addresses
        addresses = self.calculate_addresses(self.json_txn["signatures"])
        assert len(list(set(addresses))) == 1
        self.txn_details["txn_address"] = addresses[0]

        # Collect input details
        input_utxos, input_values = self.get_inputs(addresses * len(self.json_txn["body"]["collateral"]), self.json_txn["body"]["collateral"])
        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos
            self.txn_details["input_values"] = input_values

        # Collect output details
        output_addresses, output_values, _ = self.get_outputs(self.json_txn["body"]["outputs"])
        if call_from == "explorer":
            self.txn_details["output_addresses"] = output_addresses
            self.txn_details["output_values"] = output_values

        self.txn_details["collateral"] = sum(input_values) - sum(output_values)

        # Data request transaction hash
        self.txn_details["data_request_txn_hash"] = self.json_txn["body"]["dr_pointer"]

        return self.txn_details

    def get_data_request_hash(self, txn_hash):
        sql = """
            SELECT
                data_request_txn_hash
            FROM commit_txns
            WHERE
                commit_txns.txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytes.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            return result[0].hex()
        else:
            return {
                "error": "could not find commit transaction"
            }

    def get_commits_for_data_request(self, data_request_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                commit_txns.txn_hash,
                commit_txns.txn_address,
                commit_txns.epoch
            FROM commit_txns
            LEFT JOIN blocks ON 
                commit_txns.epoch=blocks.epoch
            WHERE
                commit_txns.data_request_txn_hash=%s
            ORDER BY commit_txns.epoch DESC
        """ % psycopg2.Binary(bytes.fromhex(data_request_hash))
        results = self.witnet_database.sql_return_all(sql)

        commits = []
        found_confirmed, found_mined = False, False
        for commit in results:
            block_hash, block_confirmed, block_reverted, txn_hash, txn_address, epoch = commit

            timestamp = self.start_time + (epoch + 1) * self.epoch_period

            if block_confirmed:
                found_confirmed = True
                status = "confirmed"
            elif block_reverted:
                status = "reverted"
            else:
                found_mined = True
                status = "mined"

            # Do not append reverted reveals when there are newer mined or confirmed reveals
            if (found_confirmed or found_mined) and block_reverted:
                continue

            # No block found for this commit, most likely it was reverted and deleted
            if block_hash is None:
                continue

            commits.append({
                "block_hash": block_hash.hex(),
                "txn_hash": txn_hash.hex(),
                "txn_address": txn_address,
                "epoch": epoch,
                "time": timestamp,
                "status": status,
            })

        return commits

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.confirmed,
                blocks.reverted,
                commit_txns.txn_address,
                commit_txns.input_values,
                commit_txns.input_utxos,
                commit_txns.output_values,
                commit_txns.epoch
            FROM commit_txns
            LEFT JOIN blocks ON
                commit_txns.epoch=blocks.epoch
            WHERE
                txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytearray.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            block_confirmed, block_reverted, txn_address, input_values, input_utxos, output_values, epoch = result

            input_utxo_values = []
            for input_value, (input_txn, input_idx) in zip(input_values, input_utxos):
                input_utxo_values.append([input_value, bytearray(input_txn).hex(), input_idx])

            txn_epoch = epoch
            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            if block_confirmed:
                status = "confirmed"
            elif block_reverted:
                status = "reverted"
            else:
                status = "mined"
        else:
            txn_address = ""
            input_utxo_values = []
            output_values = []
            txn_epoch = ""
            txn_time = ""
            status = "transaction not found"

        return {
            "type": "commit_txn",
            "txn_hash": txn_hash,
            "input_utxos": input_utxo_values,
            "output_values": output_values,
            "txn_epoch": txn_epoch,
            "txn_time": txn_time,
            "status": status,
        }
