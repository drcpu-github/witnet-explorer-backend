import time

import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError
from psycopg.sql import SQL, Identifier

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.mempool_schema import NetworkMempoolArgs, NetworkMempoolResponse
from util.common_functions import (
    calculate_priority,
    calculate_timestamp_from_epoch,
    get_network_times,
)
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
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
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

        config = current_app.config["explorer"]

        # Use the last 24h
        if "start_epoch" not in args or "stop_epoch" not in args:
            timestamp_stop = int(time.time() / 60) * 60
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

        granularity = args["granularity"]
        sample_rate = int(granularity / config["explorer"]["mempool_interval"])

        transaction_type = args["transaction_type"]
        logger.info(
            f"network_mempool({transaction_type}, {timestamp_start}, {timestamp_stop}, {granularity})"
        )

        key = f"network_mempool_{transaction_type}_{timestamp_start}_{timestamp_stop}_{granularity}"
        mempool = cache.get(key)
        if not mempool:
            logger.info(f"Could not find {key} in memcached cache")
            mempool = get_historical_mempool(
                database,
                transaction_type,
                timestamp_start,
                timestamp_stop,
                sample_rate,
                granularity,
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

        return mempool, 200, {"X-Version": "v1.0.0"}


def get_historical_mempool(
    database,
    transaction_type,
    timestamp_start,
    timestamp_stop,
    sample_rate,
    granularity,
):
    mempool_transactions = {}
    table_mapping = {
        "data_requests": "data_request_mempool",
        "value_transfers": "value_transfer_mempool",
    }

    # Get lists between the required timestamps
    sql = """
        SELECT
            timestamp,
            fee,
            weight
        FROM
            {}
        WHERE
            timestamp BETWEEN %s AND %s
        ORDER BY
            timestamp
        ASC
    """
    sql = SQL(re_sql(sql)).format(Identifier(table_mapping[transaction_type]))
    data = database.sql_return_all(sql, [timestamp_start, timestamp_stop])

    # Loop over the values and check if they're spaced per minute
    # If they are not, there were no pending transactions, add empty lists
    mempool_transactions = interpolate_and_transform(
        timestamp_start,
        timestamp_stop,
        data,
        sample_rate,
        granularity,
    )

    return mempool_transactions


def interpolate_and_transform(
    start_timestamp, stop_timestamp, raw_data, sample_rate, granularity
):
    histogram_data = [
        {"timestamp": timestamp, "fee": [], "amount": []}
        for timestamp in range(start_timestamp, stop_timestamp, granularity)
    ]

    # Loop over the available data and check if we need to interpolate
    counter = 0
    for hd in range(0, len(histogram_data)):
        aggregated_histogram = {}
        for rd in range(counter, len(raw_data)):
            if raw_data[rd][0] <= histogram_data[hd]["timestamp"]:
                histogram = build_priority_histogram(raw_data[rd][1], raw_data[rd][2])
                for priority, amount in histogram.items():
                    if priority not in aggregated_histogram:
                        aggregated_histogram[priority] = 0
                    aggregated_histogram[priority] += amount
            else:
                break
            counter += 1
        if len(aggregated_histogram) > 0:
            histogram_data[hd]["fee"] = [
                priority for priority, _ in sorted(aggregated_histogram.items())
            ]
            histogram_data[hd]["amount"] = [
                calculate_priority(amount, sample_rate, round_priority=True)
                for _, amount in sorted(aggregated_histogram.items())
            ]

    return histogram_data


def build_priority_histogram(absolute_fee, weights):
    priorities = {}

    for abs_fee, weight in zip(absolute_fee, weights):
        priority = calculate_priority(abs_fee, weight, round_priority=True)
        if priority not in priorities:
            priorities[priority] = 0
        priorities[priority] += 1

    return priorities
