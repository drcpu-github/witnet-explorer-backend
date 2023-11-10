import time

import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError
from psycopg.sql import SQL, Identifier

from schemas.misc.abort_schema import AbortSchema
from schemas.network.mempool_schema import NetworkMempoolArgs, NetworkMempoolResponse
from util.common_functions import calculate_timestamp_from_epoch, get_network_times
from util.data_transformer import re_sql

network_mempool_blueprint = Blueprint(
    "network mempool",
    "network mempool",
    description="Fetch the historical Witnet network mempool.",
)


@network_mempool_blueprint.route("/mempool")
class NetworkMempool(MethodView):
    @network_mempool_blueprint.arguments(NetworkMempoolArgs, location="query")
    @network_mempool_blueprint.response(
        200,
        NetworkMempoolResponse(many=True),
        description="Returns a historical snapshot of the amount of pending transactions.",
    )
    @network_mempool_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect format for mempool statistics.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]

        # Use the last 24h
        if "start_epoch" not in args or "stop_epoch" not in args:
            timestamp_stop = int(time.time())
            timestamp_start = timestamp_stop - 24 * 60 * 60
        # Calculate timestamps from epochs
        else:
            start_time, epoch_period = get_network_times(database)
            timestamp_start = calculate_timestamp_from_epoch(
                start_time, epoch_period, args["start_epoch"]
            )
            timestamp_stop = calculate_timestamp_from_epoch(
                start_time, epoch_period, args["stop_epoch"]
            )

        transaction_type = args["transaction_type"]
        logger.info(
            f"network_mempool({transaction_type}, {timestamp_start}, {timestamp_stop})"
        )

        key = f"network_mempool_{transaction_type}_{timestamp_start}_{timestamp_stop}"
        mempool = cache.get(key)
        if not mempool:
            logger.info(f"Could not find {key} in memcached cache")
            mempool = get_historical_mempool(
                database,
                transaction_type,
                timestamp_start,
                timestamp_stop,
            )

            try:
                NetworkMempoolResponse(many=True).load(mempool)
            except ValidationError as err_info:
                logger.error(f"Incorrect format for mempool statistics: {err_info}")
                abort(404, message="Incorrect format for mempool statistics.")

            try:
                # no timeout required since this data never becomes stale
                cache.set(key, mempool)
            except pylibmc.TooBig:
                logger.warning(
                    f"Could not save {key} in cache because the item size exceeded 1MB"
                )
        else:
            logger.info(f"Found {key} in memcached cache")

        return mempool


def get_historical_mempool(database, transaction_type, timestamp_start, timestamp_stop):
    mempool_transactions = {}
    table_mapping = {
        "data_requests": "pending_data_request_txns",
        "value_transfers": "pending_value_transfer_txns",
    }

    # Get lists between the required timestamps
    sql = """
        SELECT
            timestamp,
            fee_per_unit,
            num_txns
        FROM {}
        WHERE
            timestamp BETWEEN %s AND %s
        ORDER BY timestamp ASC
    """
    sql = SQL(re_sql(sql)).format(Identifier(table_mapping[transaction_type]))
    data = database.sql_return_all(sql, [timestamp_start, timestamp_stop])

    # Loop over the values and check if they're spaced per minute
    # If they are not, there were no pending transactions, add empty lists
    mempool_transactions = interpolate_and_transform(
        timestamp_start, timestamp_stop, data
    )

    return mempool_transactions


def interpolate_and_transform(start_timestamp, stop_timestamp, data):
    interpolated_data = []
    # Edge case where there are no transactions
    if len(data) == 0:
        timestamp = start_timestamp
        while stop_timestamp - timestamp >= 0:
            interpolated_data.append({"timestamp": timestamp, "fee": [], "amount": []})
            timestamp += 60
    # Normal case
    else:
        # First check if we need to insert empty lists at the start
        timestamp = data[0][0]
        while timestamp - start_timestamp >= 60:
            interpolated_data.append(
                {"timestamp": start_timestamp, "fee": [], "amount": []}
            )
            start_timestamp += 60
        interpolated_data.append(
            {"timestamp": data[0][0], "fee": data[0][1], "amount": data[0][2]}
        )

        # Then loop over the available data and check if we need to interpolate
        timestamp = interpolated_data[-1]["timestamp"]
        for entry in data[1:]:
            while entry[0] - timestamp > 60:
                interpolated_data.append(
                    {"timestamp": timestamp + 60, "fee": [], "amount": []}
                )
                timestamp += 60
            interpolated_data.append(
                {"timestamp": entry[0], "fee": entry[1], "amount": entry[2]}
            )
            timestamp = entry[0]

        # Last check if we need to append empty lists at the end
        timestamp = interpolated_data[-1]["timestamp"]
        while stop_timestamp - timestamp >= 60:
            interpolated_data.append(
                {"timestamp": timestamp + 60, "fee": [], "amount": []}
            )
            timestamp += 60

    return interpolated_data
