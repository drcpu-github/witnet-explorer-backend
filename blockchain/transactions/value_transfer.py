import time

from blockchain.transactions.transaction import Transaction
from schemas.component.value_transfer_schema import (
    ValueTransferTransactionForApi,
    ValueTransferTransactionForBlock,
    ValueTransferTransactionForExplorer,
)


class ValueTransfer(Transaction):
    def process_transaction(self, call_from):
        # If we create a ValueTransfer from the transaction RPC, we still have to get the sub-dictionary
        if "transaction" in self.json_txn:
            self.json_txn = self.json_txn["transaction"]["ValueTransfer"]

        # Calculate transaction addresses
        self.txn_details["input_addresses"] = self.calculate_addresses(
            self.json_txn["signatures"]
        )

        # Collect input details
        input_utxos, input_values = self.get_inputs(self.json_txn["body"]["inputs"])
        self.txn_details["input_values"] = input_values

        # Collect output details
        output_addresses, output_values, timelocks = self.get_outputs(
            self.json_txn["body"]["outputs"]
        )
        self.txn_details["output_addresses"] = output_addresses
        self.txn_details["output_values"] = output_values
        self.txn_details["timelocks"] = timelocks

        if sum(input_values) > 0:
            self.txn_details["fee"] = sum(input_values) - sum(output_values)
        else:
            self.txn_details["fee"] = 0

        if call_from == "explorer":
            self.txn_details["input_utxos"] = input_utxos

            return ValueTransferTransactionForExplorer().load(self.txn_details)

        if call_from == "api":
            self.txn_details["unique_input_addresses"] = list(
                set(self.txn_details["input_addresses"])
            )

            # Attempt to be smart and filter out change output addresses to get the true output addresses
            self.txn_details["true_output_addresses"] = list(
                set(self.txn_details["output_addresses"])
                - set(self.txn_details["input_addresses"])
            )

            _, self.txn_details["true_value"], _ = self.calculate_transaction_values(
                self.txn_details["input_addresses"],
                output_addresses,
                output_values,
            )

            self.txn_details["priority"] = max(
                1, int(self.txn_details["fee"] / self.txn_details["weight"])
            )
            self.txn_details["timestamp"] = (
                self.start_time + (self.txn_details["epoch"] + 1) * self.epoch_period
            )

            # Delete fields not used in the frontend to display a block
            del self.txn_details["input_addresses"]
            del self.txn_details["input_values"]
            del self.txn_details["output_addresses"]
            del self.txn_details["output_values"]
            del self.txn_details["timelocks"]

            return ValueTransferTransactionForBlock().load(self.txn_details)

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
                input_utxos,
                output_addresses,
                output_values,
                timelocks,
                weight,
            ) = result

            txn_fee = (
                sum(input_values) - sum(output_values) if len(input_values) > 0 else 0
            )
            # Get an integer value for the weighted fee
            txn_priority = max(1, int(txn_fee / weight))

            # Return all inputs separate
            address_value_utxos = []
            for address, value, (utxo_hash, txn_idx) in zip(
                input_addresses, input_values, input_utxos
            ):
                address_value_utxos.append(
                    {
                        "address": address,
                        "value": value,
                        "input_utxo": f"{utxo_hash.hex()}:{txn_idx}",
                    }
                )

            # Merge inputs from the same address
            inputs_merged = self.create_merged_inputs(input_addresses, input_values)

            true_output_addresses = list(set(output_addresses) - set(input_addresses))
            change_output_addresses = list(set(input_addresses) & set(output_addresses))

            total_value, true_value, change_value = self.calculate_transaction_values(
                input_addresses,
                output_addresses,
                output_values,
            )

            now = int(time.time())
            # Return all outputs as separate utxos
            utxos = self.create_utxos(output_addresses, output_values, timelocks, now)
            # Merge outputs to the same address (if they don't have different timelocks)
            utxos_merged = self.create_merged_utxos(
                input_addresses,
                output_addresses,
                output_values,
                timelocks,
                now,
            )

            txn_epoch = block_epoch
            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            return ValueTransferTransactionForApi().load(
                {
                    "block": block_hash.hex(),
                    "hash": txn_hash,
                    "input_addresses": input_addresses,
                    "input_utxos": address_value_utxos,
                    "inputs_merged": inputs_merged,
                    "output_addresses": output_addresses,
                    "output_values": output_values,
                    "timelocks": timelocks,
                    "utxos": utxos,
                    "utxos_merged": utxos_merged,
                    "fee": txn_fee,
                    "weight": weight,
                    "priority": txn_priority,
                    "epoch": txn_epoch,
                    "timestamp": txn_time,
                    "value": total_value,
                    "true_value": true_value,
                    "change_value": change_value,
                    "true_output_addresses": true_output_addresses,
                    "change_output_addresses": change_output_addresses,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}

    def calculate_transaction_values(
        self,
        input_addresses,
        output_addresses,
        output_values,
    ):
        total_value, true_value, change_value = 0, 0, 0
        for output_address, output_value in zip(output_addresses, output_values):
            if output_address not in input_addresses:
                true_value += output_value
            else:
                change_value += output_value
            total_value += output_value
        return total_value, true_value, change_value

    def create_utxos(self, output_addresses, output_values, timelocks, now):
        utxos = []
        for output_address, output_value, timelock in zip(
            output_addresses, output_values, timelocks
        ):
            utxos.append(
                {
                    "address": output_address,
                    "value": output_value,
                    "timelock": timelock,
                    "locked": timelock > now,
                }
            )
        return utxos

    def create_merged_utxos(
        self,
        input_addresses,
        output_addresses,
        output_values,
        timelocks,
        now,
    ):
        utxos_merged = {}
        change_output, values_merged, timelocked_outputs = {}, {}, []
        for address, value, timelock in zip(output_addresses, output_values, timelocks):
            # Keep change outputs separate
            if address in input_addresses:
                if address not in change_output:
                    change_output[address] = 0
                change_output[address] += value
            # If timelock is zero combine utxos to the same addresses
            elif timelock == 0:
                if address not in values_merged:
                    values_merged[address] = 0
                values_merged[address] += value
            # Otherwise keep them separated
            else:
                timelocked_outputs.append(
                    {
                        "address": address,
                        "value": value,
                        "timelock": timelock,
                        "locked": timelock > now,
                    }
                )
        # Normal outputs
        utxos_merged = [
            {
                "address": address,
                "value": value,
                "timelock": 0,
                "locked": False,
            }
            for address, value in values_merged.items()
        ]
        # Timelocked outputs
        utxos_merged.extend(
            sorted(timelocked_outputs, key=lambda output: output["timelock"])
        )
        # Change outputs
        utxos_merged.extend(
            [
                {
                    "address": address,
                    "value": value,
                    "timelock": 0,
                    "locked": False,
                }
                for address, value in change_output.items()
            ]
        )

        return utxos_merged

    def create_merged_inputs(
        self,
        input_addresses,
        input_values,
    ):
        inputs = {}
        for input_address, input_value in zip(input_addresses, input_values):
            if input_address not in inputs:
                inputs[input_address] = 0
            inputs[input_address] += input_value
        return [
            {"address": address, "value": value} for address, value in inputs.items()
        ]
