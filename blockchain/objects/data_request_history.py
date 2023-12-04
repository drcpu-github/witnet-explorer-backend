from psycopg.sql import SQL, Identifier

from blockchain.transactions.data_request import (
    build_retrieval,
    translate_filters,
    translate_reducer,
)
from blockchain.transactions.tally import translate_tally
from schemas.search.data_request_history_schema import (
    DataRequestHistory as DataRequestHistorySchema,
)
from util.data_transformer import re_sql


class DataRequestHistory(object):
    def __init__(self, consensus_constants, logger, database):
        # Copy relevant consensus constants
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        self.logger = logger
        self.database = database

    def get_history(self, hash_type, bytes_hash, rows=50, row_offset=0):
        self.logger.info(f"{hash_type}, get_history({bytes_hash})")

        sql = """
            SELECT
                COUNT(*)
            FROM
                data_request_txns
            WHERE
                data_request_txns.{column_name}=%s
        """
        sql = SQL(re_sql(sql)).format(column_name=Identifier(hash_type.lower()))
        (count,) = self.database.sql_return_one(
            sql,
            parameters=[bytearray.fromhex(bytes_hash)],
        )

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
                data_request_txns.kinds,
                data_request_txns.urls,
                data_request_txns.headers,
                data_request_txns.bodies,
                data_request_txns.scripts,
                data_request_txns.aggregate_filters,
                data_request_txns.aggregate_reducer,
                data_request_txns.tally_filters,
                data_request_txns.tally_reducer,
                tally_txns.txn_hash,
                tally_txns.epoch,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.result
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
            LIMIT
                %s
            OFFSET
                %s
        """
        sql = SQL(re_sql(sql)).format(column_name=Identifier(hash_type.lower()))
        results = self.database.sql_return_all(
            sql,
            parameters=[bytearray.fromhex(bytes_hash), rows, row_offset],
            custom_types=["filter"],
        )

        data_request = None
        data_request_history = []
        for result in results:
            (
                block_epoch,
                block_confirmed,
                block_reverted,
                data_request_hash,
                data_request_witnesses,
                data_request_witness_reward,
                data_request_collateral,
                data_request_consensus_percentage,
                data_request_RAD_bytes_hash,
                data_request_kinds,
                data_request_urls,
                data_request_headers,
                data_request_bodies,
                data_request_scripts,
                data_request_aggregate_filters,
                data_request_aggregate_reducer,
                data_request_tally_filters,
                data_request_tally_reducer,
                tally_txn_hash,
                tally_epoch,
                tally_error_addresses,
                tally_liar_addresses,
                tally_result,
            ) = result

            # Ignore tallies which happened before the data request / block epoch
            # These originate from errored requests and get replaced with newer ones
            if tally_epoch and tally_epoch <= block_epoch:
                continue

            txn_epoch = block_epoch
            txn_time = self.start_time + (block_epoch + 1) * self.epoch_period

            if tally_txn_hash:
                tally_success, tally_result = translate_tally(
                    tally_txn_hash.hex(), tally_result
                )
            else:
                tally_success = False
                tally_result = ""
            num_errors = (
                0 if tally_error_addresses is None else len(tally_error_addresses)
            )
            num_liars = 0 if tally_liar_addresses is None else len(tally_liar_addresses)

            data_request_history.append(
                {
                    "success": tally_success,
                    "epoch": txn_epoch,
                    "timestamp": txn_time,
                    "data_request": data_request_hash.hex(),
                    "witnesses": data_request_witnesses,
                    "witness_reward": data_request_witness_reward,
                    "collateral": data_request_collateral,
                    "consensus_percentage": data_request_consensus_percentage,
                    "num_errors": num_errors,
                    "num_liars": num_liars,
                    "result": tally_result,
                    "confirmed": block_confirmed,
                    "reverted": block_reverted,
                }
            )

            # Intialize these variables once to use them after the loop
            if data_request is None:
                data_request_retrieval = build_retrieval(
                    data_request_kinds,
                    data_request_urls,
                    data_request_headers,
                    data_request_bodies,
                    data_request_scripts,
                )

                # Translate aggregation stage
                data_request_aggregate = translate_filters(
                    data_request_aggregate_filters
                )
                if len(data_request_aggregate) > 0:
                    data_request_aggregate += "."
                data_request_aggregate += translate_reducer(
                    data_request_aggregate_reducer
                )

                # Translate tally stage
                data_request_tally = translate_filters(data_request_tally_filters)
                if len(data_request_tally) > 0:
                    data_request_tally += "."
                data_request_tally += translate_reducer(data_request_tally_reducer)

                data_request = {
                    "witnesses": data_request_witnesses,
                    "witness_reward": data_request_witness_reward,
                    "collateral": data_request_collateral,
                    "consensus_percentage": data_request_consensus_percentage,
                    "RAD_bytes_hash": data_request_RAD_bytes_hash.hex(),
                    "retrieve": data_request_retrieval,
                    "aggregate": data_request_aggregate,
                    "tally": data_request_tally,
                }

        # If all witnesses, witness_reward, collateral and consensus_percentage variables are the same, filter them out
        add_parameters = False
        witnesses_set = set(drh["witnesses"] for drh in data_request_history)
        witness_reward_set = set(drh["witness_reward"] for drh in data_request_history)
        collateral_set = set(drh["collateral"] for drh in data_request_history)
        consensus_percentage_set = set(
            drh["consensus_percentage"] for drh in data_request_history
        )
        if (
            len(witnesses_set)
            + len(witness_reward_set)
            + len(collateral_set)
            + len(consensus_percentage_set)
            == 4
        ):
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
                }
                for drh in data_request_history
            ]

        return_value = {
            "hash": bytes_hash,
            "hash_type": hash_type,
            "history": data_request_history,
        }

        # If all data request parameters are the same, add them as a separate structure
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

        return count, DataRequestHistorySchema().load(return_value)
