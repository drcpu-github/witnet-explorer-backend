import json

import cbor2

from blockchain.transactions.transaction import Transaction
from schemas.component.tally_schema import (
    TallyTransactionForApi,
    TallyTransactionForBlock,
    TallyTransactionForExplorer,
)
from util.radon_translator import RadonTranslator


class Tally(Transaction):
    def process_transaction(self, call_from):
        # Collect output details
        output_addresses, output_values, _ = self.get_outputs(self.json_txn["outputs"])
        self.txn_details["output_addresses"] = output_addresses
        self.txn_details["output_values"] = output_values

        self.txn_details["data_request"] = self.json_txn["dr_pointer"]

        # Translate tally value
        success, tally_translation = translate_tally(
            self.txn_hash, self.json_txn["tally"]
        )
        self.txn_details["success"] = success

        # Get error_addresses and liar_addresses
        if call_from == "explorer":
            self.txn_details["error_addresses"] = self.json_txn["error_committers"]
            self.txn_details["liar_addresses"] = list(
                set(self.json_txn["out_of_consensus"])
                - set(self.json_txn["error_committers"])
            )
            self.txn_details["tally"] = bytearray(self.json_txn["tally"])

            return TallyTransactionForExplorer().load(self.txn_details)

        if call_from == "api":
            self.txn_details["num_error_addresses"] = len(
                self.json_txn["error_committers"]
            )
            self.txn_details["num_liar_addresses"] = len(
                list(
                    set(self.json_txn["out_of_consensus"])
                    - set(self.json_txn["error_committers"])
                )
            )
            self.txn_details["tally"] = tally_translation

            return TallyTransactionForBlock().load(self.txn_details)

    def get_data_request_hash(self, txn_hash):
        sql = """
            SELECT
                data_request
            FROM
                tally_txns
            WHERE
                tally_txns.txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(txn_hash)],
        )

        if result:
            return result[0].hex()
        else:
            return {"error": "could not find tally transaction"}

    def get_tally_for_data_request(self, data_request_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                tally_txns.txn_hash,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.result,
                tally_txns.epoch
            FROM
                tally_txns
            LEFT JOIN
                blocks
            ON
                tally_txns.epoch=blocks.epoch
            WHERE
                tally_txns.data_request=%s
            ORDER BY
                tally_txns.epoch
            DESC
        """
        results = self.database.sql_return_all(
            sql,
            parameters=[bytearray.fromhex(data_request_hash)],
        )

        tally = None
        found_confirmed, found_mined = False, False
        if results:
            for result in results:
                (
                    block_hash,
                    block_confirmed,
                    block_reverted,
                    txn_hash,
                    error_addresses,
                    liar_addresses,
                    tally_result,
                    epoch,
                ) = result

                if block_confirmed:
                    found_confirmed = True
                elif not block_reverted:
                    found_mined = True

                # Do not set a reverted tally when there is a newer mined or confirmed tally
                if (found_confirmed or found_mined) and block_reverted:
                    continue

                # No block found for this tally, most likely it was reverted and deleted
                if block_hash is None:
                    continue

                success, tally_result = translate_tally(txn_hash.hex(), tally_result)

                timestamp = self.start_time + (epoch + 1) * self.epoch_period

                tally = {
                    "hash": txn_hash.hex(),
                    "block": block_hash.hex(),
                    "error_addresses": error_addresses,
                    "liar_addresses": liar_addresses,
                    "num_error_addresses": len(error_addresses),
                    "num_liar_addresses": len(liar_addresses),
                    "tally": tally_result,
                    "success": success,
                    "epoch": epoch,
                    "timestamp": timestamp,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }

        return tally

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                tally_txns.data_request,
                tally_txns.output_addresses,
                tally_txns.output_values,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.result,
                tally_txns.epoch
            FROM
                tally_txns
            LEFT JOIN
                blocks
            ON
                tally_txns.epoch=blocks.epoch
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
                data_request,
                output_addresses,
                output_values,
                error_addresses,
                liar_addresses,
                result,
                epoch,
            ) = result

            success, tally_result = translate_tally(txn_hash, result)

            txn_epoch = epoch
            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            return TallyTransactionForApi().load(
                {
                    "hash": txn_hash,
                    "block": block_hash.hex(),
                    "data_request": data_request.hex(),
                    "output_addresses": output_addresses,
                    "output_values": output_values,
                    "error_addresses": error_addresses,
                    "liar_addresses": liar_addresses,
                    "num_error_addresses": len(error_addresses),
                    "num_liar_addresses": len(liar_addresses),
                    "success": success,
                    "tally": tally_result,
                    "epoch": txn_epoch,
                    "timestamp": txn_time,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}


def translate_tally(txn_hash, tally):
    success = True

    translation = cbor2.loads(bytearray(tally))
    if isinstance(translation, bytes):
        translation = translation.hex()
    else:
        translation = str(translation)

    # If the translation starts with 'CBORTag(39, ' there was a RADON error
    if translation.startswith("CBORTag(39, "):
        success = False
        try:
            # Extract the array containing the error code and potentially some extra metadata
            translation_error_data = translation[12:-1]

            # Replace the quotes in the potentially included metadata
            translation_error_data = translation_error_data.replace('"', "")
            translation_error_data = translation_error_data.replace("'", '"')
            error = json.loads(translation_error_data)
            error_code = int(error[0])
            error_text = error[1] if len(error) == 2 else ""

            # Translate error code to human-readable format
            translator = RadonTranslator()
            translation = translator.hex2str(error_code, "error")

            # Handle some special cases, adding extra explanations
            if error_code == 0x51 and len(error) == 3:  # InsufficientConsensus
                translation += (
                    ": "
                    + ("%.0f" % (error[1] * 100))
                    + "% <= "
                    + ("%.0f" % (error[2] * 100))
                    + "%"
                )
            elif error_code == 0x52 and len(error) == 3:  # InsufficientCommits
                translation += ": " + str(error[1]) + " < " + str(error[2])
            elif error_text != "":
                translation += ": " + str(error_text)
        except Exception:
            translation = translation[translation.find("[") : translation.find("]") + 1]
            print(f"Tally exception ({txn_hash}): {translation}")

    return success, translation
