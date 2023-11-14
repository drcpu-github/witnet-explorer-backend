import json

import cbor

from blockchain.transactions.transaction import Transaction
from schemas.component.reveal_schema import (
    RevealTransactionForApi,
    RevealTransactionForBlock,
    RevealTransactionForExplorer,
)
from util.radon_translator import RadonTranslator


class Reveal(Transaction):
    def process_transaction(self, call_from):
        # Calculate transaction addresses
        addresses = self.calculate_addresses(self.json_txn["signatures"])
        assert len(list(set(addresses))) == 1
        self.txn_details["address"] = addresses[0]

        # Data request transaction hash
        self.txn_details["data_request"] = self.json_txn["body"]["dr_pointer"]

        # Translate revealed value
        success, reveal_translation = translate_reveal(
            self.txn_hash, self.json_txn["body"]["reveal"]
        )
        self.txn_details["success"] = success

        # Add reveal value
        if call_from == "explorer":
            self.txn_details["reveal"] = bytearray(self.json_txn["body"]["reveal"])

            return RevealTransactionForExplorer().load(self.txn_details)

        if call_from == "api":
            self.txn_details["reveal"] = reveal_translation

            return RevealTransactionForBlock().load(self.txn_details)

    def get_data_request_hash(self, txn_hash):
        sql = """
            SELECT
                data_request
            FROM
                reveal_txns
            WHERE
                reveal_txns.txn_hash=%s
            LIMIT 1
        """
        result = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(txn_hash)],
        )

        if result:
            return result[0].hex()
        else:
            return {"error": "transaction not found"}

    def get_reveals_for_data_request(self, data_request_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                reveal_txns.txn_hash,
                reveal_txns.txn_address,
                reveal_txns.result,
                reveal_txns.epoch
            FROM
                reveal_txns
            LEFT JOIN
                blocks
            ON
                reveal_txns.epoch=blocks.epoch
            WHERE
                reveal_txns.data_request=%s
            ORDER BY
                reveal_txns.epoch
            DESC
        """
        results = self.database.sql_return_all(
            sql,
            parameters=[bytearray.fromhex(data_request_hash)],
        )

        if results is None:
            return []

        reveals = []
        found_confirmed, found_mined = False, False
        for reveal in results:
            (
                block_hash,
                block_confirmed,
                block_reverted,
                txn_hash,
                txn_address,
                reveal_result,
                epoch,
            ) = reveal

            if block_confirmed:
                found_confirmed = True
            elif not block_reverted:
                found_mined = True

            # Do not append reverted reveals when there are newer mined or confirmed reveals
            if (found_confirmed or found_mined) and block_reverted:
                continue

            # No block found for this tally, most likely it was reverted and deleted
            if block_hash is None:
                continue

            success, reveal_result = translate_reveal(txn_hash.hex(), reveal_result)

            timestamp = self.start_time + (epoch + 1) * self.epoch_period

            reveals.append(
                {
                    "block": block_hash.hex(),
                    "hash": txn_hash.hex(),
                    "address": txn_address,
                    "reveal": reveal_result,
                    "success": success,
                    "error": not success,
                    "liar": False,
                    "epoch": epoch,
                    "timestamp": timestamp,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )

        return reveals

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.confirmed,
                blocks.reverted,
                reveal_txns.txn_address,
                reveal_txns.result,
                reveal_txns.epoch
            FROM
                reveal_txns
            LEFT JOIN blocks ON
                reveal_txns.epoch=blocks.epoch
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
                txn_address,
                reveal_result,
                epoch,
            ) = result

            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            success, reveal_result = translate_reveal(txn_hash, reveal_result)

            return RevealTransactionForApi().load(
                {
                    "hash": txn_hash,
                    "block": block_hash.hex(),
                    "epoch": epoch,
                    "timestamp": txn_time,
                    "address": txn_address,
                    "success": success,
                    "reveal": reveal_result,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )
        else:
            return {"error": "transaction not found"}


def translate_reveal(txn_hash, reveal):
    success = True
    translation = cbor.loads(bytearray(reveal))

    if isinstance(translation, bytes):
        translation = translation.hex()
    else:
        translation = str(translation)

    # If the translation starts with 'Tag(39, ' there was a RADON error
    if translation.startswith("Tag(39, "):
        success = False
        try:
            # Extract the array containing the error code and potentially some extra metadata
            translation_error_data = translation[8:-1]
            # Replace the quotes in the potentially included metadata
            translation_error_data = translation_error_data.replace('"', "")
            translation_error_data = translation_error_data.replace("'", '"')
            error = json.loads(translation_error_data)
            error_code = int(error[0])
            error_text = error[1] if len(error) == 2 else ""

            # Translate error code to human-readable format
            translator = RadonTranslator()
            translation = translator.hex2str(error_code, "error")

            # Adding extra explanation
            if error_text != "":
                translation += f": {error_text}"
        except Exception:
            translation = translation[translation.find("[") + 1 : translation.find("]")]
            print(f"Reveal exception ({txn_hash}): {translation}")

    return success, translation
