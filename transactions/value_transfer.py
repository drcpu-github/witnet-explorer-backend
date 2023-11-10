import psycopg2
import time

from transactions.transaction import Transaction

class ValueTransfer(Transaction):
    def process_transaction(self, call_from):
        if self.json_txn == {}:
            self.logger.warning(f"Empty transaction {self.txn_hash}")
            return self.json_txn

        # If we create a ValueTransfer from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["ValueTransfer"]

        # Calculate transaction addresses
        self.txn_details["input_addresses"] = self.calculate_addresses(self.json_txn["signatures"])

        # Collect input details
        input_utxos, input_values = self.get_inputs(self.txn_details["input_addresses"], self.json_txn["body"]["inputs"])
        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos
        else:
            self.txn_details["input_utxos"] = [(input_txn.hex(), input_idx) for input_txn, input_idx in input_utxos]
        self.txn_details["input_values"] = input_values

        # Collect output details
        output_addresses, output_values, timelocks = self.get_outputs(self.json_txn["body"]["outputs"])
        self.txn_details["output_addresses"] = output_addresses
        self.txn_details["output_values"] = output_values
        self.txn_details["timelocks"] = timelocks

        # Attempt to be smart and split output addresses into true output addresses and change output addresses
        self.txn_details["true_output_addresses"] = list(set(self.txn_details["output_addresses"]) - set(self.txn_details["input_addresses"]))
        self.txn_details["change_output_addresses"] = list(set(self.txn_details["input_addresses"]) & set(self.txn_details["output_addresses"]))

        self.txn_details["fee"] = sum(input_values) - sum(output_values) if sum(input_values) > 0 else 0

        total_value, true_value, change_value = 0, 0, 0
        for output_address, output_value in zip(output_addresses, output_values):
            if output_address not in self.txn_details["input_addresses"]:
                true_value += output_value
            else:
                change_value += output_value
            total_value += output_value
        self.txn_details["value"] = total_value
        self.txn_details["true_value"] = true_value
        self.txn_details["change_value"] = change_value

        self.txn_details["priority"] = max(1, self.txn_details["fee"] // self.txn_details["weight"])

        return self.txn_details

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                value_transfer_txns.input_addresses,
                value_transfer_txns.input_values,
                value_transfer_txns.input_utxos,
                value_transfer_txns.output_addresses,
                value_transfer_txns.output_values,
                value_transfer_txns.timelocks,
                value_transfer_txns.weight
            FROM value_transfer_txns
            LEFT JOIN blocks ON
                value_transfer_txns.epoch=blocks.epoch
            WHERE
                value_transfer_txns.txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytearray.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            block_hash, block_epoch, block_confirmed, block_reverted, input_addresses, input_values, input_utxos, output_addresses, output_values, timelocks, weight = result
            block_hash = block_hash.hex()

            txn_fee = sum(input_values) - sum(output_values) if len(input_values) > 0 else 0
            txn_weight = weight
            # Get an integer value for the weighted fee
            txn_priority = max(1, txn_fee // txn_weight)

            assert len(input_addresses) ==  len(input_values)

            # Return all inputs separate
            input_utxo_values = {}
            for input_address, input_value, (input_txn, input_idx) in zip(input_addresses, input_values, input_utxos):
                if not input_address in input_utxo_values:
                    input_utxo_values[input_address] = []
                input_utxo_values[input_address].append([input_value, bytearray(input_txn).hex(), input_idx])

            # Merge inputs from the same address
            inputs_combined = {}
            for input_address, input_value in zip(input_addresses, input_values):
                if not input_address in inputs_combined:
                    inputs_combined[input_address] = 0
                inputs_combined[input_address] += input_value
            input_addresses = [[input_address, input_value] for input_address, input_value in inputs_combined.items()]

            assert len(output_addresses) == len(output_values) == len(timelocks)

            # Return all outputs as separate utxos
            output_utxos = {}
            for output_address, output_value, timelock in zip(output_addresses, output_values, timelocks):
                if not output_address in output_utxos:
                    output_utxos[output_address] = []
                output_utxos[output_address].append([output_value, timelock, timelock > time.time()])

            # Merge outputs to the same address (if they don't have different timelocks)
            change_output = {}
            outputs_combined = {}
            timelocked_outputs = []
            for output_address, output_value, timelock in zip(output_addresses, output_values, timelocks):
                # Keep change outputs separate
                if output_address in input_addresses:
                    if not output_address in change_output:
                        change_output[output_address] = 0
                    change_output[output_address] += output_value
                # If timelock is zero combine utxos to the same addresses
                elif timelock == 0:
                    if not output_address in outputs_combined:
                        outputs_combined[output_address] = 0
                    outputs_combined[output_address] += output_value
                # Otherwise keep them separated
                else:
                    timelocked_outputs.append([output_address, output_value, timelock, timelock > time.time()])
            # Normal outputs
            output_addresses = [[output_address, output_value, 0, False] for output_address, output_value in outputs_combined.items()]
            # Timelocked outputs
            output_addresses.extend(sorted(timelocked_outputs, key=lambda l: l[2]))
            # Change outputs
            output_addresses.extend([[output_address, output_value, 0, False] for output_address, output_value in change_output.items()])

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
            inputs = []
            outputs = []
            txn_fee = 0
            txn_weight = 0
            txn_priority = 0
            txn_epoch = 0
            txn_time = ""
            block_confirmed = False
            txn_status = "transaction not found"

        return {
            "type": "value_transfer_txn",
            "block_hash": block_hash,
            "txn_hash": txn_hash,
            "input_utxos": input_utxo_values,
            "input_addresses": input_addresses,
            "output_utxos": output_utxos,
            "output_addresses": output_addresses,
            "fee": txn_fee,
            "weight": txn_weight,
            "priority": txn_priority,
            "txn_epoch": txn_epoch,
            "txn_time": txn_time,
            "confirmed": block_confirmed,
            "status": txn_status
        }
