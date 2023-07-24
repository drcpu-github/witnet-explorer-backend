import cbor
import json
import psycopg2

from transactions.transaction import Transaction

from util.radon_translator import RadonTranslator

class Reveal(Transaction):
    def process_transaction(self, call_from):
        if self.json_txn == {}:
            return self.json_txn

        # Calculate transaction addresses
        addresses = self.calculate_addresses(self.json_txn["signatures"])
        assert len(list(set(addresses))) == 1
        self.txn_details["txn_address"] = addresses[0]

        # Data request transaction hash
        self.txn_details["data_request_txn_hash"] = self.json_txn["body"]["dr_pointer"]

        # Add reveal value
        if call_from == "explorer":
            self.txn_details["reveal_value"] = bytearray(self.json_txn["body"]["reveal"])

        # Translate revealed value
        success, reveal_translation = translate_reveal(self.txn_hash, self.json_txn["body"]["reveal"])
        self.txn_details["success"] = success
        self.txn_details["reveal_translation"] = reveal_translation

        return self.txn_details

    def get_data_request_hash(self, txn_hash):
        sql = """
            SELECT
                data_request_txn_hash
            FROM reveal_txns
            WHERE
                reveal_txns.txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytes.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            return result[0].hex()
        else:
            return {
                "error": "could not find reveal transaction"
            }

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
            FROM reveal_txns
            LEFT JOIN blocks ON 
                reveal_txns.epoch=blocks.epoch
            WHERE
                reveal_txns.data_request_txn_hash=%s
            ORDER BY epoch DESC
        """ % psycopg2.Binary(bytes.fromhex(data_request_hash))
        results = self.witnet_database.sql_return_all(sql)

        if results == None:
            return []

        reveals = []
        found_confirmed, found_mined = False, False
        for reveal in results:
            block_hash, block_confirmed, block_reverted, txn_hash, txn_address, reveal_result, epoch = reveal

            success, reveal_result = translate_reveal(txn_hash.hex(), reveal_result)

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

            # No block found for this tally, most likely it was reverted and deleted
            if block_hash is None:
                continue

            reveals.append({
                "block_hash": block_hash.hex(),
                "txn_hash": txn_hash.hex(),
                "txn_address": txn_address,
                "reveal": reveal_result,
                "success": success,
                "error": False,
                "liar": False,
                "epoch": epoch,
                "time": timestamp,
                "status": status,
            })

        return reveals

def translate_reveal(txn_hash, reveal):
    success = True
    translation = cbor.loads(bytearray(reveal))

    if type(translation) == bytes:
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
            translation_error_data = translation_error_data.replace("\"", "").replace("\'", "\"")
            error = json.loads(translation_error_data)
            error_code = int(error[0])
            error_text = error[1] if len(error) == 2 else ""

            # Translate error code to human-readable format
            translator = RadonTranslator()
            translation = translator.hex2str(error_code, "error")

            # Adding extra explanation
            if error_text != "":
                translation += ": " + str(error_text)
        except Exception:
            translation = translation[translation.find("[")+1:translation.find("]")]
            print(f"Reveal exception ({txn_hash}): {translation}")

    return success, translation
