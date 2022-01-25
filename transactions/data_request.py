import cbor
import psycopg2

from transactions.transaction import Transaction

class DataRequest(Transaction):
    def process_transaction(self, call_from):
        if self.json_txn == {}:
            self.logger.warning(f"Empty transaction {self.txn_hash}")
            return self.json_txn

        # If we create a DataRequest from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["DataRequest"]
        self.data_request = self.json_txn["body"]["dr_output"]["data_request"]

        # Calculate transaction addresses
        self.txn_details["input_addresses"] = self.calculate_addresses(self.json_txn["signatures"])
        self.txn_details["unique_input_addresses"] = list(set(self.txn_details["input_addresses"]))

        # Collect input details
        input_utxos, input_values = self.get_inputs(self.txn_details["input_addresses"], self.json_txn["body"]["inputs"])
        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos
            self.txn_details["input_values"] = input_values

        # Collect output details
        output_addresses, output_values, _ = self.get_outputs(self.json_txn["body"]["outputs"])
        if call_from == "explorer":
            self.txn_details["output_addresses"] = output_addresses
            self.txn_details["output_values"] = output_values

        # Number of witnesses and the reward
        self.txn_details["witnesses"] = self.json_txn["body"]["dr_output"]["witnesses"]
        self.txn_details["witness_reward"] = self.json_txn["body"]["dr_output"]["witness_reward"]

        # Fee payed per commit and reveal to the miner
        self.txn_details["commit_and_reveal_fee"] = self.json_txn["body"]["dr_output"]["commit_and_reveal_fee"]

        # Minimum consensus percentage
        self.txn_details["consensus_percentage"] = self.json_txn["body"]["dr_output"]["min_consensus_percentage"]

        # Calculate total fee (payed to witnesses and mining nodes)
        # Total fee = number of witnesses multiplied by the reward + total number of commits and reveals multiplied by its fee + tally fee (1)
        self.txn_details["fee"] = sum(input_values) - sum(output_values)

        # Requested data request collateral
        self.txn_details["collateral"] = max(self.collateral_minimum, self.json_txn["body"]["dr_output"]["collateral"])

        # Process sources and scripts
        self.txn_details["txn_kind"] = None
        self.txn_details["urls"], self.txn_details["scripts"] = [], []
        for retrieve in self.data_request["retrieve"]:
            if not self.txn_details["txn_kind"]:
                self.txn_details["txn_kind"] = retrieve["kind"]
            else:
                assert self.txn_details["txn_kind"] == retrieve["kind"], "Unexpectedly found different data request kinds"

            self.txn_details["urls"].append(retrieve["url"])
            if call_from == "explorer":
                self.txn_details["scripts"].append(bytearray(retrieve["script"]))
            else:
                self.txn_details["scripts"].append(self.translate_script(retrieve["script"]))

        # Aggregate stage
        if len(self.data_request["aggregate"]["filters"]) > 0:
            if call_from == "explorer":
                self.txn_details["aggregate_filters"] = [(aggregate_filter["op"], bytearray(aggregate_filter["args"])) for aggregate_filter in self.data_request["aggregate"]["filters"]]
            else:
                self.txn_details["aggregate_filters"] = self.translate_filters([(aggregate_filter["op"], aggregate_filter["args"]) for aggregate_filter in self.data_request["aggregate"]["filters"]])
        else:
            self.txn_details["aggregate_filters"] = []

        if type(self.data_request["aggregate"]["reducer"]) is int:
            self.txn_details["aggregate_reducer"] = [self.data_request["aggregate"]["reducer"]]
        else:
            self.txn_details["aggregate_reducer"] = self.data_request["aggregate"]["reducer"]

        if call_from == "api":
            self.txn_details["aggregate_reducer"] = self.translate_reducer(self.txn_details["aggregate_reducer"])

        # Tally stage
        if len(self.data_request["tally"]["filters"]) > 0:
            if call_from == "explorer":
                self.txn_details["tally_filters"] = [(tally_filter["op"], bytearray(tally_filter["args"])) for tally_filter in self.data_request["tally"]["filters"]]
            else:
                self.txn_details["tally_filters"] = self.translate_filters([(tally_filter["op"], tally_filter["args"]) for tally_filter in self.data_request["tally"]["filters"]])
        else:
            self.txn_details["tally_filters"] = []

        if type(self.data_request["tally"]["reducer"]) is int:
            self.txn_details["tally_reducer"] = [self.data_request["tally"]["reducer"]]
        else:
            self.txn_details["tally_reducer"] = self.data_request["tally"]["reducer"]

        if call_from == "api":
            self.txn_details["tally_reducer"] = self.translate_reducer(self.txn_details["tally_reducer"])

        self.txn_details["RAD_bytes_hash"], self.txn_details["data_request_bytes_hash"] = self.get_bytecode_hashes()

        return self.txn_details

    def get_bytecode_hashes(self):
        RAD_bytes_hash, _ = self.protobuf_encoder.get_bytecode_RAD_request()
        data_request_bytes_hash, _ = self.protobuf_encoder.get_bytecode_data_request()
        return RAD_bytes_hash, data_request_bytes_hash

    def get_transaction_from_database(self, data_request_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                data_request_txns.txn_hash,
                data_request_txns.txn_kind,
                data_request_txns.input_addresses,
                data_request_txns.input_values,
                data_request_txns.input_utxos,
                data_request_txns.output_values,
                data_request_txns.witnesses,
                data_request_txns.witness_reward,
                data_request_txns.collateral,
                data_request_txns.consensus_percentage,
                data_request_txns.commit_and_reveal_fee,
                data_request_txns.weight,
                data_request_txns.urls,
                data_request_txns.scripts,
                data_request_txns.aggregate_filters,
                data_request_txns.aggregate_reducer,
                data_request_txns.tally_filters,
                data_request_txns.tally_reducer
            FROM data_request_txns
            LEFT JOIN blocks ON
                data_request_txns.epoch=blocks.epoch
            WHERE
                data_request_txns.txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytes.fromhex(data_request_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            block_hash, block_epoch, block_confirmed, block_reverted, txn_hash, txn_kind, input_addresses, input_values, input_utxos, output_values, witnesses, witness_reward, collateral, consensus_percentage, commit_and_reveal_fee, weight, urls, scripts, aggregate_filters, aggregate_reducer, tally_filters, tally_reducer = result

            block_hash = block_hash.hex()
            txn_hash = txn_hash.hex()

            addresses = list(set(input_addresses))

            input_utxo_values = []
            for input_value, (input_txn, input_idx) in zip(input_values, input_utxos):
                input_utxo_values.append([input_value, bytearray(input_txn).hex(), input_idx])

            txn_fee = sum(input_values) - sum(output_values)
            txn_weight = weight
            # Get an integer value for the weighted fee
            txn_priority = max(1, txn_fee // txn_weight)

            # Add urls and translate scripts
            txn_retrieve = []
            for url, script in zip(urls, scripts):
                txn_retrieve.append({
                    "url": url,
                    "script": self.translate_script(script)
                })

            # Translate aggregation stage
            txn_aggregate = self.translate_filters(aggregate_filters)
            if len(txn_aggregate) > 0:
                txn_aggregate += "."
            txn_aggregate += self.translate_reducer(aggregate_reducer)

            # Translate tally stage
            txn_tally = self.translate_filters(tally_filters)
            if len(txn_tally) > 0:
                txn_tally += "." 
            txn_tally += self.translate_reducer(tally_reducer)

            txn_epoch = block_epoch
            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            if block_confirmed:
                txn_status = "confirmed"
            elif block_reverted:
                txn_status = "reverted"
            else:
                txn_status = "mined"
        else:
            block_hash = ""
            txn_hash = ""
            txn_kind = ""
            addresses = []
            input_utxo_values = []
            txn_fee = 0
            txn_weight = 0
            txn_priority = 0
            witnesses = 0
            witness_reward = 0
            collateral = 0
            consensus_percentage = 0
            commit_and_reveal_fee = 0
            txn_retrieve = ""
            txn_aggregate = ""
            txn_tally = ""
            txn_epoch = 0
            txn_time = ""
            txn_status = "transaction not found"

        return {
            "type": "data_request_txn",
            "block_hash": block_hash,
            "txn_hash": txn_hash,
            "txn_kind": txn_kind,
            "addresses": addresses,
            "input_utxos": input_utxo_values,
            "fee": txn_fee,
            "weight": txn_weight,
            "priority": txn_priority,
            "witnesses": witnesses,
            "witness_reward": witness_reward,
            "collateral": collateral,
            "consensus_percentage": consensus_percentage,
            "commit_and_reveal_fee": commit_and_reveal_fee,
            "retrieve": txn_retrieve,
            "aggregate": txn_aggregate,
            "tally": txn_tally,
            "txn_epoch": txn_epoch,
            "txn_time": txn_time,
            "status": txn_status,
        }

    def translate_script(self, script):
        translation = ""
        for op in list(cbor.loads(bytearray(script))):
            if type(op) is int:
                translation += self.translator.hex2str(op, "opcode") + "()."
            else:
                translation += self.translator.hex2str(op[0], "opcode") + "(" + str(op[1]) + ")."
        return translation[:-1]

    def translate_filters(self, filters):
        translation = ""
        for filt, arg in filters:
            translation += "filter(" + self.translator.hex2str(filt, "filter")
            if len(arg) > 0:
                translation += ", " + str(cbor.loads(bytearray(arg))) + ")."
            else:
                translation += ")."
        return translation[:-1]

    def translate_reducer(self, reducers):
        translation = ""
        for r in reducers:
            translation += "reduce(" + self.translator.hex2str(r, "reducer") + ")."
        return translation[:-1]
