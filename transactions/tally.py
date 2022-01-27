import cbor
import json
import psycopg2

from transactions.transaction import Transaction

from util.radon_translator import RadonTranslator

class Tally(Transaction):
    def process_transaction(self, call_from):
        if self.json_txn == {}:
            return self.json_txn

        # Collect output details
        _, output_values, _ = self.get_outputs(self.json_txn["outputs"])
        self.txn_details["output_values"] = output_values

        # Get error_addresses and liar_addresses
        if call_from == "explorer":
            self.txn_details["error_addresses"] = self.json_txn["error_committers"]
            self.txn_details["liar_addresses"] = list(set(self.json_txn["out_of_consensus"]) - set(self.json_txn["error_committers"]))
        else:
            self.txn_details["num_error_addresses"] = len(self.json_txn["error_committers"])
            self.txn_details["num_liar_addresses"] = len(list(set(self.json_txn["out_of_consensus"]) - set(self.json_txn["error_committers"])))

        if call_from == "explorer":
            self.txn_details["tally_value"] = bytearray(self.json_txn["tally"])

        # Translate tally value
        success, tally_translation = translate_tally(self.txn_hash, self.json_txn["tally"])
        self.txn_details["success"] = success
        self.txn_details["tally_translation"] = tally_translation

        self.txn_details["data_request_txn_hash"] = self.json_txn["dr_pointer"]

        return self.txn_details

    def get_data_request_hash(self, txn_hash):
        sql = """
            SELECT
                data_request_txn_hash
            FROM tally_txns
            WHERE
                tally_txns.txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytes.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            return result[0].hex()
        else:
            return {
                "error": "could not find tally transaction"
            }

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
            FROM tally_txns
            LEFT JOIN blocks ON 
                tally_txns.epoch=blocks.epoch
            WHERE
                tally_txns.data_request_txn_hash=%s
            ORDER BY tally_txns.epoch DESC
        """ % psycopg2.Binary(bytes.fromhex(data_request_hash))
        results = self.witnet_database.sql_return_all(sql)

        tally = None
        found_confirmed, found_mined = False, False
        for result in results:
            block_hash, block_confirmed, block_reverted, txn_hash, error_addresses, liar_addresses, tally_result, epoch = result

            success, tally_result = translate_tally(txn_hash.hex(), tally_result)

            timestamp = self.start_time + (epoch + 1) * self.epoch_period

            if block_confirmed:
                found_confirmed = True
                status = "confirmed"
            elif block_reverted:
                status = "reverted"
            else:
                found_mined = True
                status = "mined"

            # Do not set a reverted tally when there is a newer mined or confirmed tally
            if (found_confirmed or found_mined) and block_reverted:
                continue

            tally = {
                "block_hash": block_hash.hex(),
                "txn_hash": txn_hash.hex(),
                "error_addresses": error_addresses,
                "liar_addresses": liar_addresses,
                "num_error_addresses": len(error_addresses),
                "num_liar_addresses": len(liar_addresses),
                "tally": tally_result,
                "success": success,
                "epoch": epoch,
                "time": timestamp,
                "status": status,
            }

        return tally

    def get_transaction_from_database(self, txn_hash):
        sql = """
            SELECT
                blocks.confirmed,
                blocks.reverted,
                tally_txns.output_values,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.result,
                tally_txns.epoch
            FROM tally_txns
            LEFT JOIN blocks ON
                tally_txns.epoch=blocks.epoch
            WHERE
                txn_hash=%s
            LIMIT 1
        """ % psycopg2.Binary(bytearray.fromhex(txn_hash))
        result = self.witnet_database.sql_return_one(sql)

        if result:
            block_confirmed, block_reverted, output_values, error_addresses, liar_addresses, result, epoch = result

            tally_outputs = output_values

            success, tally_result = translate_tally(bytearray.fromhex(txn_hash), result)

            txn_epoch = epoch
            txn_time = self.start_time + (epoch + 1) * self.epoch_period

            if block_confirmed:
                status = "confirmed"
            elif block_reverted:
                status = "reverted"
            else:
                status = "mined"
        else:
            tally_outputs = []
            error_addresses = []
            liar_addresses = []
            success = False
            tally_result = ""
            txn_epoch = ""
            txn_time = ""
            status = "transaction not found"

        return {
            "type": "tally_txn",
            "txn_hash": txn_hash,
            "outputs": tally_outputs,
            "error_addresses": error_addresses,
            "liar_addresses": liar_addresses,
            "success": success,
            "tally": tally_result,
            "txn_epoch": txn_epoch,
            "txn_time": txn_time,
            "status": status,
        }

def translate_tally(txn_hash, tally):
    success = True

    translation = cbor.loads(bytearray(tally))
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

            # Handle some special cases, adding extra explanations
            if error_code == 0x51 and len(error) == 3: # InsufficientConsensus
                translation += ": " + ("%.0f" % (error[1] * 100)) + "% <= " + ("%.0f" % (error[2] * 100)) + "%"
            elif error_code == 0x52 and len(error) == 3: # InsufficientCommits
                translation += ": " + str(error[1]) + " < " + str(error[2])
            elif error_text != "":
                translation += ": " + str(error_text)
        except Exception:
            translation = translation[translation.find("["):translation.find("]")+1]
            print(f"Tally exception ({txn_hash.hex()}): {translation}")

    return success, translation
