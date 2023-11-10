import re

import cbor

from schemas.component.data_request_schema import (
    DataRequestTransactionForApi,
    DataRequestTransactionForBlock,
    DataRequestTransactionForExplorer,
)
from transactions.transaction import Transaction
from util.helper_functions import calculate_priority


class DataRequest(Transaction):
    def process_transaction(self, call_from):
        # If we create a DataRequest from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["DataRequest"]
        self.data_request = self.json_txn["body"]["dr_output"]["data_request"]

        # Calculate transaction addresses
        self.txn_details["input_addresses"] = self.calculate_addresses(
            self.json_txn["signatures"]
        )

        # Collect input / output details
        input_utxos, input_values = self.get_inputs(
            self.txn_details["input_addresses"], self.json_txn["body"]["inputs"]
        )
        output_addresses, output_values, _ = self.get_outputs(
            self.json_txn["body"]["outputs"]
        )

        self.txn_details["witnesses"] = self.json_txn["body"]["dr_output"]["witnesses"]
        self.txn_details["witness_reward"] = self.json_txn["body"]["dr_output"][
            "witness_reward"
        ]
        self.txn_details["commit_and_reveal_fee"] = self.json_txn["body"]["dr_output"][
            "commit_and_reveal_fee"
        ]
        self.txn_details["consensus_percentage"] = self.json_txn["body"]["dr_output"][
            "min_consensus_percentage"
        ]
        self.txn_details["collateral"] = max(
            self.collateral_minimum, self.json_txn["body"]["dr_output"]["collateral"]
        )

        # Calculate fees (payed to witnesses and mining nodes)
        assert len(output_values) <= 1, "Unexpectedly found multiple output values"
        output_value = 0
        if len(output_values) == 1:
            output_value = output_values[0]
        dro_fee, miner_fee = self.calculate_fees(
            self.txn_details["witnesses"],
            self.txn_details["witness_reward"],
            self.txn_details["commit_and_reveal_fee"],
            input_values,
            output_value,
        )
        self.txn_details["dro_fee"] = dro_fee
        self.txn_details["miner_fee"] = miner_fee

        # Get the aggregate and tally reducer
        if isinstance(self.data_request["aggregate"]["reducer"], int):
            self.txn_details["aggregate_reducer"] = [
                self.data_request["aggregate"]["reducer"]
            ]
        else:
            self.txn_details["aggregate_reducer"] = self.data_request["aggregate"][
                "reducer"
            ]

        if isinstance(self.data_request["tally"]["reducer"], int):
            self.txn_details["tally_reducer"] = [self.data_request["tally"]["reducer"]]
        else:
            self.txn_details["tally_reducer"] = self.data_request["tally"]["reducer"]

        RAD_bytes_hash, DRO_bytes_hash = self.get_bytecode_hashes()
        self.txn_details["RAD_bytes_hash"] = RAD_bytes_hash
        self.txn_details["DRO_bytes_hash"] = DRO_bytes_hash

        self.txn_details["kinds"] = []
        self.txn_details["urls"] = []
        self.txn_details["bodies"] = []
        self.txn_details["scripts"] = []

        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos
            self.txn_details["input_values"] = input_values

            assert (
                len(output_addresses) <= 1
            ), "Unexpectedly found multiple output addresses"
            if len(output_addresses) == 1:
                self.txn_details["output_address"] = output_addresses[0]
            else:
                self.txn_details["output_address"] = None

            if len(output_values) == 1:
                self.txn_details["output_value"] = output_values[0]
            else:
                self.txn_details["output_value"] = None

            # Process RAD sources and scripts
            for retrieve in self.data_request["retrieve"]:
                self.txn_details["kinds"].append(retrieve["kind"])

                if retrieve["kind"] in ("HTTP-GET", "HTTP-POST"):
                    self.txn_details["urls"].append(retrieve["url"])
                else:
                    self.txn_details["urls"].append(None)

                if retrieve["kind"] == "HTTP-POST":
                    self.txn_details["bodies"].append(bytearray(retrieve["body"]))
                else:
                    self.txn_details["bodies"].append(bytearray())

                self.txn_details["scripts"].append(bytearray(retrieve["script"]))

            # Collect aggregation stage details
            if len(self.data_request["aggregate"]["filters"]) > 0:
                self.txn_details["aggregate_filters"] = [
                    (aggregate_filter["op"], bytearray(aggregate_filter["args"]))
                    for aggregate_filter in self.data_request["aggregate"]["filters"]
                ]
            else:
                self.txn_details["aggregate_filters"] = []

            # Collect tally stage details
            if len(self.data_request["tally"]["filters"]) > 0:
                self.txn_details["tally_filters"] = [
                    (tally_filter["op"], bytearray(tally_filter["args"]))
                    for tally_filter in self.data_request["tally"]["filters"]
                ]
            else:
                self.txn_details["tally_filters"] = []

            return DataRequestTransactionForExplorer().load(self.txn_details)

        if call_from == "api":
            # Process RAD sources and scripts
            for retrieve in self.data_request["retrieve"]:
                self.txn_details["kinds"].append(retrieve["kind"])

                if retrieve["kind"] in ("HTTP-GET", "HTTP-POST"):
                    self.txn_details["urls"].append(retrieve["url"])
                else:
                    self.txn_details["urls"].append(None)

                if retrieve["kind"] == "HTTP-POST":
                    self.txn_details["bodies"].append(
                        "".join([chr(c) for c in retrieve["body"]])
                    )
                else:
                    self.txn_details["bodies"].append("")

                self.txn_details["scripts"].append(
                    self.translate_script(retrieve["script"])
                )

            # Collect aggregation stage details
            if len(self.data_request["aggregate"]["filters"]) > 0:
                self.txn_details["aggregate_filters"] = self.translate_filters(
                    [
                        (aggregate_filter["op"], aggregate_filter["args"])
                        for aggregate_filter in self.data_request["aggregate"][
                            "filters"
                        ]
                    ]
                )
            else:
                self.txn_details["aggregate_filters"] = ""

            # Collect tally stage details
            if len(self.data_request["tally"]["filters"]) > 0:
                self.txn_details["tally_filters"] = self.translate_filters(
                    [
                        (tally_filter["op"], tally_filter["args"])
                        for tally_filter in self.data_request["tally"]["filters"]
                    ]
                )
            else:
                self.txn_details["tally_filters"] = ""

            # Translate reducers
            self.txn_details["aggregate_reducer"] = self.translate_reducer(
                self.txn_details["aggregate_reducer"]
            )
            self.txn_details["tally_reducer"] = self.translate_reducer(
                self.txn_details["tally_reducer"]
            )

            return DataRequestTransactionForBlock().load(self.txn_details)

    def get_bytecode_hashes(self):
        RAD_bytes_hash, _ = self.protobuf_encoder.get_RAD_bytecode(
            self.txn_details["epoch"]
        )
        DRO_bytes_hash, _ = self.protobuf_encoder.get_DRO_bytecode(
            self.txn_details["epoch"]
        )
        return RAD_bytes_hash, DRO_bytes_hash

    def get_transaction_from_database(self, data_request_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                data_request_txns.DRO_bytes_hash,
                data_request_txns.RAD_bytes_hash,
                data_request_txns.input_addresses,
                data_request_txns.input_values,
                data_request_txns.input_utxos,
                data_request_txns.output_value,
                data_request_txns.witnesses,
                data_request_txns.witness_reward,
                data_request_txns.collateral,
                data_request_txns.consensus_percentage,
                data_request_txns.commit_and_reveal_fee,
                data_request_txns.weight,
                data_request_txns.kinds,
                data_request_txns.urls,
                data_request_txns.bodies,
                data_request_txns.scripts,
                data_request_txns.aggregate_filters,
                data_request_txns.aggregate_reducer,
                data_request_txns.tally_filters,
                data_request_txns.tally_reducer
            FROM
                data_request_txns
            LEFT JOIN
                blocks
            ON
                data_request_txns.epoch=blocks.epoch
            WHERE
                data_request_txns.txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(data_request_hash)],
            custom_types=["utxo", "filter"],
        )

        if result:
            (
                block_hash,
                block_epoch,
                block_confirmed,
                block_reverted,
                DRO_bytes_hash,
                RAD_bytes_hash,
                input_addresses,
                input_values,
                input_utxos,
                output_value,
                witnesses,
                witness_reward,
                collateral,
                consensus_percentage,
                commit_and_reveal_fee,
                weight,
                kinds,
                urls,
                bodies,
                scripts,
                aggregate_filters,
                aggregate_reducer,
                tally_filters,
                tally_reducer,
            ) = result

            input_utxo_values = []
            for address, value, (utxo_hash, utxo_idx) in zip(
                input_addresses, input_values, input_utxos
            ):
                input_utxo_values.append(
                    {
                        "address": address,
                        "value": value,
                        "input_utxo": f"{utxo_hash.hex()}:{utxo_idx}",
                    }
                )

            dro_fee, miner_fee = self.calculate_fees(
                witnesses,
                witness_reward,
                commit_and_reveal_fee,
                input_values,
                output_value,
            )
            # Get an integer value for the weighted fee
            txn_priority = calculate_priority(miner_fee, weight)

            # Add kinds, urls, bodies and translate scripts
            # psycopg2 does not handle arrays of enums very well
            # handle as a string starting and ending with {} + split on commas
            txn_retrieve = []
            kinds = re.match(r"^{(.*)}$", kinds).group(1).split(",")
            for kind, url, body, script in zip(kinds, urls, bodies, scripts):
                txn_retrieve.append(
                    {
                        "kind": kind,
                        "url": url,
                        "body": "".join([chr(c) for c in bytearray(body)]),
                        "script": self.translate_script(script),
                    }
                )

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

            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            return DataRequestTransactionForApi().load(
                {
                    "hash": data_request_hash,
                    "RAD_bytes_hash": RAD_bytes_hash.hex(),
                    "DRO_bytes_hash": DRO_bytes_hash.hex(),
                    "block": block_hash.hex(),
                    "input_addresses": list(set(input_addresses)),
                    "input_utxos": input_utxo_values,
                    "miner_fee": miner_fee,
                    "dro_fee": dro_fee,
                    "weight": weight,
                    "priority": txn_priority,
                    "witnesses": witnesses,
                    "witness_reward": witness_reward,
                    "collateral": collateral,
                    "consensus_percentage": consensus_percentage,
                    "commit_and_reveal_fee": commit_and_reveal_fee,
                    "retrieve": txn_retrieve,
                    "aggregate": txn_aggregate,
                    "tally": txn_tally,
                    "epoch": block_epoch,
                    "timestamp": txn_time,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}

    def calculate_fees(
        self,
        witnesses,
        witness_reward,
        commit_and_reveal_fee,
        input_values,
        output_value,
    ):
        # DRO fee = number of witnesses multiplied by their reward + total number of commits and reveals multiplied by its fee + tally fee (1)
        # The commit fees, reveal fees and tally fee go to the miners including the transactions
        # The witness reward goes to the witnesses solving a data request
        dro_fee = witnesses * (witness_reward + 2 * commit_and_reveal_fee) + 1
        # Miner fee = the sum of input values minus the sum of output values minus the DRO fee
        # This fee goes to the miner who picks the data request from the memory pool and includes it in a block
        # This fee is divided by the transaction weight and determines the priority for being executed (included in a block)
        miner_fee = sum(input_values) - (output_value or 0) - dro_fee
        return dro_fee, miner_fee

    def translate_script(self, script):
        translation = ""
        for op in list(cbor.loads(bytearray(script))):
            if type(op) is int:
                translation += self.translator.hex2str(op, "opcode") + "()."
            else:
                translation += (
                    self.translator.hex2str(op[0], "opcode") + "(" + str(op[1]) + ")."
                )
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
