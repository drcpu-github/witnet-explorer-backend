import logging
import logging.handlers
import psycopg2

from blockchain.witnet_database import WitnetDatabase

from transactions.data_request import DataRequest
from transactions.tally import translate_tally

class DataRequestHistory(object):
    def __init__(self, consensus_constants, logging_queue, database_config):
        # Copy relevant consensus constants
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        # Set up logger
        self.logging_queue = logging_queue
        self.configure_logging_process(logging_queue, "node-manager")
        self.logger = logging.getLogger("node-manager")

        # Set up database connection
        db_user = database_config["user"]
        db_name = database_config["name"]
        db_pass = database_config["password"]
        self.witnet_database = WitnetDatabase(db_user, db_name, db_pass, self.logging_queue, "db-history")

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def get_history(self, hash_type, bytes_hash, start, stop, amount):
        self.logger.info(f"{hash_type}, get_history({bytes_hash})")

        sql = """
            SELECT
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                data_request_txns.txn_hash,
                data_request_txns.witnesses,
                data_request_txns.witness_reward,
                data_request_txns.collateral,
                data_request_txns.consensus_percentage,
                data_request_txns.RAD_bytes_hash,
                tally_txns.txn_hash,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.result,
                tally_txns.success,
                tally_txns.epoch
            FROM data_request_txns
            LEFT JOIN blocks ON
                data_request_txns.epoch=blocks.epoch
            LEFT JOIN tally_txns ON
                data_request_txns.txn_hash=tally_txns.data_request_txn_hash
            WHERE
                data_request_txns.%s=%s
                %s
                %s
            ORDER BY
                blocks.epoch DESC
        """ % (
            hash_type,
            psycopg2.Binary(bytes.fromhex(bytes_hash)),
            f" AND data_request_txns.epoch>={start}" if start > 0 else "",
            f" AND data_request_txns.epoch<={stop}" if stop > 0 else "",
        )
        results = self.witnet_database.sql_return_all(sql)

        num_data_requests = len(results) if results else 0
        first_epoch = min(r[0] for r in results) if results else 0
        last_epoch = max(r[0] for r in results) if results else 0

        data_request_history = []
        for result in results:
            block_epoch, block_confirmed, block_reverted, data_request_txn_hash, witnesses, witness_reward, collateral, consensus_percentage, RAD_bytes_hash, tally_txn_hash, error_addresses, liar_addresses, result, success, tally_epoch = result

            # Ignore tallies which happened before the data request / block epoch
            # These originate from errored request and get replaced with newer ones
            if tally_epoch and tally_epoch <= block_epoch:
                continue

            txn_epoch = block_epoch
            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            data_request_txn_hash = data_request_txn_hash.hex()

            RAD_bytes_hash = RAD_bytes_hash.hex()

            num_errors = len(error_addresses) if error_addresses else ""
            num_liars = len(liar_addresses) if liar_addresses else ""

            tally_result = ""
            if result:
                _, tally_result = translate_tally(tally_txn_hash, result)

            tally_success = success if success else ""

            if block_confirmed:
                txn_status = "confirmed"
            elif block_reverted:
                txn_status = "reverted"
            else:
                txn_status = "mined"

            data_request_history.append([
                    txn_status,
                    tally_success,
                    txn_time,
                    txn_epoch,
                    data_request_txn_hash,
                    witnesses,
                    witness_reward,
                    collateral,
                    consensus_percentage,
                    num_errors,
                    num_liars,
                    tally_result,
            ])

            # We fetch more than 'amount' requests, but break here to be able to filter out replaced tally transactions
            if len(data_request_history) == amount:
                break

        # post processing
        # if all witnesses, witness_reward, collateral and consensus_percentage variables are the same, filter them out
        add_parameters = False
        witnesses_set = set(drh[5] for drh in data_request_history)
        witness_reward_set = set(drh[6] for drh in data_request_history)
        collateral_set = set(drh[7] for drh in data_request_history)
        consensus_percentage_set = set(drh[8] for drh in data_request_history)
        if len(witnesses_set) + len(witness_reward_set) + len(collateral_set) + len(consensus_percentage_set) == 4:
            add_parameters = True
            data_request_history = [[drh[0], drh[1], drh[2], drh[3], drh[4], drh[9], drh[10], drh[11]] for drh in data_request_history]

        return_value = {
            "type": "data_request_history",
            "bytes_hash": bytes_hash,
            "RAD_bytes_hash": RAD_bytes_hash,
            "hash_type": hash_type,
            "history": data_request_history,
            "num_data_requests": num_data_requests,
            "first_epoch": first_epoch,
            "last_epoch": last_epoch,
            "status": "found",
        }

        if add_parameters:
            return_value["data_request_parameters"] = {
                "witnesses": witnesses,
                "witness_reward": witness_reward,
                "collateral": collateral,
                "consensus_percentage": consensus_percentage,
            }

        return return_value
