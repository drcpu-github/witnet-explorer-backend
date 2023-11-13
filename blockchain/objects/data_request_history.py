import logging
import logging.handlers

from psycopg.sql import SQL, Identifier

from schemas.search.data_request_history_schema import DataRequestHistory as DataRequestHistorySchema
from blockchain.transactions.data_request import DataRequest
from blockchain.transactions.tally import Tally
from util.database_manager import DatabaseManager
from util.data_transformer import re_sql

class DataRequestHistory(object):
    def __init__(self, consensus_constants, logger, database):
        # Copy relevant consensus constants
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        self.logger = logger
        self.database = database

        self.data_request = DataRequest(consensus_constants, logger=logger, database=database)
        self.tally = Tally(consensus_constants, logger=logger, database=database)

    def get_history(self, hash_type, bytes_hash):
        self.logger.info(f"{hash_type}, get_history({bytes_hash})")

        sql = """
            SELECT
                blocks.epoch,
                blocks.confirmed,
                blocks.reverted,
                data_request_txns.txn_hash,
                tally_txns.txn_hash,
                tally_txns.epoch
            FROM
                data_request_txns
            LEFT JOIN
                blocks
            ON
                data_request_txns.epoch=blocks.epoch
            LEFT JOIN
                tally_txns
            ON
                data_request_txns.txn_hash=tally_txns.data_request
            WHERE
                data_request_txns.{column_name}=%s
            ORDER BY
                blocks.epoch
            DESC
        """
        sql = SQL(re_sql(sql)).format(column_name=Identifier(hash_type))
        results = self.database.sql_return_all(sql, parameters=[bytearray.fromhex(bytes_hash)])

        num_data_requests = len(results) if results else 0
        first_epoch = min(r[0] for r in results) if results else 0
        last_epoch = max(r[0] for r in results) if results else 0

        data_request = None
        data_request_history = []
        for result in results:
            block_epoch, block_confirmed, block_reverted, data_request_hash, tally_txn_hash, tally_epoch = result

            # Ignore tallies which happened before the data request / block epoch
            # These originate from errored requests and get replaced with newer ones
            if tally_epoch and tally_epoch <= block_epoch:
                continue

            txn_epoch = block_epoch
            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            data_request_hash = data_request_hash.hex()
            data_request = self.data_request.get_transaction_from_database(data_request_hash)

            tally_result = ""
            num_errors, num_liars = 0, 0
            tally_success = False
            if tally_txn_hash:
                tally_txn_hash = tally_txn_hash.hex()
                tally = self.tally.get_transaction_from_database(tally_txn_hash)

                tally_result = tally["tally"]
                num_errors = tally["num_error_addresses"]
                num_liars = tally["num_liar_addresses"]
                tally_success = tally["success"]

            data_request_history.append(
                {
                    "success": tally_success,
                    "epoch": txn_epoch,
                    "timestamp": txn_time,
                    "data_request": data_request_hash,
                    "witnesses": data_request["witnesses"],
                    "witness_reward": data_request["witness_reward"],
                    "collateral": data_request["collateral"],
                    "consensus_percentage": data_request["consensus_percentage"],
                    "num_errors": num_errors,
                    "num_liars": num_liars,
                    "result": tally_result,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )

        # post processing
        # if all witnesses, witness_reward, collateral and consensus_percentage variables are the same, filter them out
        add_parameters = False
        witnesses_set = set(drh["witnesses"] for drh in data_request_history)
        witness_reward_set = set(drh["witness_reward"] for drh in data_request_history)
        collateral_set = set(drh["collateral"] for drh in data_request_history)
        consensus_percentage_set = set(drh["consensus_percentage"] for drh in data_request_history)
        if len(witnesses_set) + len(witness_reward_set) + len(collateral_set) + len(consensus_percentage_set) == 4:
            add_parameters = True
            data_request_history = [
                {
                    "success": drh["success"],
                    "epoch": drh["epoch"],
                    "timestamp": drh["timestamp"],
                    "data_request": drh["data_request"],
                    "num_errors": drh["num_errors"],
                    "num_liars": drh["num_liars"],
                    "result": drh["result"],
                    "confirmed": drh["confirmed"],
                    "reverted": drh["reverted"],
                } for drh in data_request_history
            ]

        return_value = {
            "hash": bytes_hash,
            "hash_type": hash_type,
            "history": data_request_history,
            "num_data_requests": num_data_requests,
            "first_epoch": first_epoch,
            "last_epoch": last_epoch,
        }

        if add_parameters and data_request:
            return_value["data_request_parameters"] = {
                "witnesses": data_request["witnesses"],
                "witness_reward": data_request["witness_reward"],
                "collateral": data_request["collateral"],
                "consensus_percentage": data_request["consensus_percentage"],
            }

        if data_request:
            if hash_type != "RAD_bytes_hash":
                return_value["RAD_bytes_hash"] = data_request["RAD_bytes_hash"]
            return_value["RAD_data"] = {
                "retrieve": data_request["retrieve"],
                "aggregate": data_request["aggregate"],
                "tally": data_request["tally"],
            }

        return DataRequestHistorySchema().load(return_value)
