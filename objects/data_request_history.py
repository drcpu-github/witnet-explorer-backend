import logging
import logging.handlers
import psycopg2

from blockchain.witnet_database import WitnetDatabase

from transactions.data_request import DataRequest
from transactions.tally import translate_tally

class DataRequestHistory(object):
    def __init__(self, hash_type, bytes_hash, consensus_constants, logging_queue, database_config):
        # Copy hash parameters
        self.hash_type = hash_type
        self.bytes_hash = bytes_hash

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

    def get_history(self):
        self.logger.info(f"{self.hash_type}, get_history({self.bytes_hash})")

        # Get the matching data requests
        self.get_matching_data_requests()

        return {
            "type": "data_request_history",
            "hash_type": self.hash_type,
            "bytes_hash": self.bytes_hash,
            "data_request_history": self.data_request_history,
            "num_data_requests": self.num_data_requests,
            "status": "found",
        }

    def get_matching_data_requests(self):
        self.logger.info(f"get_matching_data_requests({self.bytes_hash})")

        sql = """
            SELECT
                COUNT(*)
            FROM data_request_txns
            WHERE
                data_request_txns.%s=%s
        """ % (self.hash_type, psycopg2.Binary(bytes.fromhex(self.bytes_hash)))
        self.num_data_requests = self.witnet_database.sql_return_one(sql)[0]

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
                data_request_txns.data_request_bytes_hash,
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
            ORDER BY
                blocks.epoch DESC
            LIMIT 200
        """ % (self.hash_type, psycopg2.Binary(bytes.fromhex(self.bytes_hash)))
        results = self.witnet_database.sql_return_all(sql)

        self.data_request_history = []
        data_requests_inserted = {}
        for result in results:
            block_epoch, block_confirmed, block_reverted, data_request_txn_hash, witnesses, witness_reward, collateral, consensus_percentage, RAD_bytes_hash, data_request_bytes_hash, tally_txn_hash, error_addresses, liar_addresses, result, success, tally_epoch = result

            # Ignore tallies which happened before the data request / block epoch
            # These originate from errored request and get replaced with newer ones
            if tally_epoch and tally_epoch <= block_epoch:
                continue

            txn_epoch = block_epoch
            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            data_request_txn_hash = data_request_txn_hash.hex()

            RAD_bytes_hash = RAD_bytes_hash.hex()
            data_request_bytes_hash = data_request_bytes_hash.hex()

            num_errors = ""
            if error_addresses:
                num_errors = len(error_addresses)

            num_liars = ""
            if liar_addresses:
                num_liars = len(liar_addresses)

            tally_result = ""
            if result:
                _, tally_result = translate_tally(tally_txn_hash, result)

            tally_success = ""
            if success:
                tally_success = success

            if block_confirmed:
                txn_status = "confirmed"
            elif block_reverted:
                txn_status = "reverted"
            else:
                txn_status = "mined"

            self.data_request_history.append({
                "type": "data_request_txn",
                "txn_hash": data_request_txn_hash,
                "RAD_bytes_hash": RAD_bytes_hash,
                "data_request_bytes_hash": data_request_bytes_hash,
                "txn_epoch": txn_epoch,
                "txn_time": txn_time,
                "status": txn_status,
                "witnesses": witnesses,
                "witness_reward": witness_reward,
                "collateral": collateral,
                "consensus_percentage": consensus_percentage,
                "num_errors": num_errors,
                "num_liars": num_liars,
                "tally_result": tally_result,
                "tally_success": tally_success,
            })

            # We fetch more than 100 requests, but break here to be able to filter out replaced tally transactions
            if len(self.data_request_history) == 100:
                break
