import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from caching.network_stats_functions import aggregate_nodes, read_from_database
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.statistics_schema import (
    NetworkStatisticsArgs,
    NetworkStatisticsResponse,
)
from util.common_sql import sql_last_confirmed_block

network_statistics_blueprint = Blueprint(
    "network statistics",
    "network statistics",
    description="Fetch historical network statistics.",
)

key_data_mapping = {
    "list-rollbacks": "rollbacks",
    "num-unique-miners": "miners",
    "num-unique-data-request-solvers": "data_request_solvers",
    "top-100-miners": "miners",
    "top-100-data-request-solvers": "data_request_solvers",
    "percentile-staking-balances": "staking",
    "histogram-data-requests": "data_requests",
    "histogram-data-request-composition": "data_requests",
    "histogram-data-request-witness": "data_requests",
    "histogram-data-request-reward": "data_requests",
    "histogram-data-request-collateral": "data_requests",
    "histogram-data-request-lie-rate": "lie_rate",
    "histogram-burn-rate": "burn_rate",
    "histogram-value-transfers": "value_transfers",
}


@network_statistics_blueprint.route("/statistics")
class NetworkStatistics(MethodView):
    @network_statistics_blueprint.arguments(NetworkStatisticsArgs, location="query")
    @network_statistics_blueprint.response(
        200,
        NetworkStatisticsResponse,
        description="Returns the requested network statistics data.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @network_statistics_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "No network statistics data found.",
                "Incorrect format for network statistics.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]

        logger.info(
            f"network_statistics({args['key']}, {args.get('start_epoch', 0)}, {args.get('stop_epoch', 0)})"
        )

        caching_config = config["api"]["caching"]
        aggregation_epochs = caching_config["scripts"]["network_stats"][
            "aggregation_epochs"
        ]
        start_epoch = args.get("start_epoch", None)
        stop_epoch = args.get("stop_epoch", None)

        # Period to fetch data for depends on the requested data
        # Staking balances is always a snapshot of the current state, no period is required
        if args["key"] == "percentile-staking-balances":
            period = [None, None]
        else:
            # No epochs requested: return a statistic since network inception or for the last 60 periods
            if start_epoch is None and stop_epoch is None:
                # These keys have a global statistic since network inception to return
                if args["key"] in (
                    "list-rollbacks",
                    "num-unique-miners",
                    "num-unique-data-request-solvers",
                    "top-100-miners",
                    "top-100-data-request-solvers",
                ):
                    period = [None, None]
                # These keys do not, return the last 60 periods
                else:
                    period = calculate_network_start_stop_epoch(
                        database,
                        aggregation_epochs,
                        start_epoch,
                        stop_epoch,
                    )
                    if not period:
                        abort(404, message="No network statistics data found.")
            # Start and stop epochs were requested, round them
            else:
                period = calculate_network_start_stop_epoch(
                    database,
                    aggregation_epochs,
                    start_epoch,
                    stop_epoch,
                )
                if not period:
                    abort(404, message="No network statistics data found.")

        cache_key = f"{args['key']}_{period[0]}_{period[1]}"
        response = cache.get(cache_key)
        if response:
            logger.info(f"Found response for {cache_key} in cache")
            return response, 200, {"X-Version": "v1.0.0"}

        # Rollbacks are saved as a list in the database, so even if epochs are specified, retrieve the complete list
        if args["key"] == "list-rollbacks":
            rollback_period = period
            period = [None, None]

        last_epoch, stats_data = read_from_database(
            key_data_mapping[args["key"]],
            aggregation_epochs,
            database,
            period=period,
        )
        last_epoch = int(last_epoch)

        # Reset rollback period to the requested epochs
        if args["key"] == "list-rollbacks":
            period = rollback_period

        # ARS and TRS balances do not require a start and stop epoch
        if args["key"] == "percentile-staking-balances":
            response = {
                "staking": stats_data[0][2],
            }
        # If below keys are requested and no epochs are defined, return the statistics for the whole network lifetime
        elif (
            args["key"]
            in (
                "num-unique-miners",
                "num-unique-data-request-solvers",
                "top-100-miners",
                "top-100-data-request-solvers",
            )
            and start_epoch is None
            and stop_epoch is None
        ):
            if args["key"].startswith("top-100"):
                stats_data = stats_data[0][2]["top-100"]
                stats_data = translate_address_ids(database, stats_data, logger)
            else:
                stats_data = stats_data[0][2]["amount"]
            response = {
                "start_epoch": 0,
                "stop_epoch": last_epoch,
                args["key"].replace("-", "_"): stats_data,
            }
        # If rollbacks are requested without epochs, return all
        elif (
            args["key"] == "list-rollbacks"
            and start_epoch is None
            and stop_epoch is None
        ):
            stats_data = stats_data[0][2]
            response = {
                "start_epoch": 0,
                "stop_epoch": last_epoch,
                args["key"].replace("-", "_"): [
                    {
                        "timestamp": sd[0],
                        "epoch_from": sd[1],
                        "epoch_to": sd[2],
                        "length": sd[3],
                    }
                    for sd in stats_data
                ],
            }
        else:
            # Set the returned stop epoch
            period = (period[0], min(last_epoch, period[1] or last_epoch))

            # The list-rollbacks key requires special handling since it is not saved as periodic data
            if args["key"] == "list-rollbacks":
                stats_data = stats_data[0][2]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [
                        {
                            "timestamp": sd[0],
                            "epoch_from": sd[1],
                            "epoch_to": sd[2],
                            "length": sd[3],
                        }
                        for sd in stats_data
                        if sd[2] >= period[0] and sd[1] <= period[1]
                    ],
                }
            # Aggregate depending on the requested key
            elif args["key"] in (
                "num-unique-miners",
                "top-100-miners",
                "num-unique-data-request-solvers",
                "top-100-data-request-solvers",
            ):
                num_unique, top_100 = aggregate_nodes(
                    [stats_data[i][2] for i in range(len(stats_data))]
                )
                if args["key"] in (
                    "num-unique-miners",
                    "num-unique-data-request-solvers",
                ):
                    response = {
                        "start_epoch": period[0],
                        "stop_epoch": period[1],
                        args["key"].replace("-", "_"): num_unique,
                    }
                else:
                    top_100_mapped = translate_address_ids(database, top_100, logger)
                    response = {
                        "start_epoch": period[0],
                        "stop_epoch": period[1],
                        args["key"].replace("-", "_"): top_100_mapped,
                    }
            elif args["key"] == "histogram-data-requests":
                stats_data = [stats_data[i][2] for i in range(len(stats_data))]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [
                        {"total": sd[0], "failure": sd[0] - sd[1]} for sd in stats_data
                    ],
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-data-request-composition":
                stats_data = [stats_data[i][2] for i in range(len(stats_data))]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [
                        {
                            "total": sd[0],
                            "http_get": sd[2],
                            "http_post": sd[3],
                            "rng": sd[4],
                        }
                        for sd in stats_data
                    ],
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-data-request-witness":
                stats_data = [stats_data[i][2] for i in range(len(stats_data))]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [sd[5] for sd in stats_data],
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-data-request-reward":
                stats_data = [stats_data[i][2] for i in range(len(stats_data))]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [sd[6] for sd in stats_data],
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-data-request-collateral":
                stats_data = [stats_data[i][2] for i in range(len(stats_data))]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [sd[7] for sd in stats_data],
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-burn-rate":
                stats_data = [stats_data[i][2] for i in range(len(stats_data))]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): [
                        {"reverted": sd[0], "lies": sd[1]} for sd in stats_data
                    ],
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-value-transfers":
                data = [{"value_transfers": sd[2][0]} for sd in stats_data]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): data,
                    "histogram_period": aggregation_epochs,
                }
            elif args["key"] == "histogram-data-request-lie-rate":
                data = [
                    {
                        "witnessing_acts": sd[2][0],
                        "errors": sd[2][1],
                        "no_reveal_lies": sd[2][2],
                        "out_of_consensus_lies": sd[2][3],
                    }
                    for sd in stats_data
                ]
                response = {
                    "start_epoch": period[0],
                    "stop_epoch": period[1],
                    args["key"].replace("-", "_"): data,
                    "histogram_period": aggregation_epochs,
                }

        # Validate the data before saving it in the cache
        try:
            NetworkStatisticsResponse().load(response)
        except ValidationError as err_info:
            logger.error(f"Incorrect format for network statistics: {err_info}")
            abort(404, message="Incorrect format for network statistics.")

        # Try to save the response in the cache
        try:
            cache.set(
                cache_key,
                response,
                timeout=caching_config["views"]["network_stats"]["timeout"],
            )
        except pylibmc.TooBig:
            logger.warning(
                f"Could not save {cache_key} in the memcached instance because its size exceeded 1MB"
            )

        return response, 200, {"X-Version": "v1.0.0"}


def calculate_network_start_stop_epoch(
    database,
    aggregation_epochs,
    start_epoch,
    stop_epoch,
):
    # If we need to calculate a start or stop epoch, fetch the latest confirmed one from the database
    data = database.sql_return_one(sql_last_confirmed_block)
    if data:
        last_confirmed_epoch = data[1]
    else:
        return [None, None]

    # Start epoch given: use the aggregation period to floor it
    if start_epoch is not None:
        start_epoch = int(start_epoch / aggregation_epochs) * aggregation_epochs
    # No start epoch given: use the aggregation period surrounding the last confirmed epoch
    # Return the last 60 aggregation periods worth of data
    # This is roughly one month at the default aggregation period of 1000 epochs
    else:
        start_epoch = (
            int(last_confirmed_epoch / aggregation_epochs - 59) * aggregation_epochs
        )

    # No stop epoch given: use the aggregation period surrounding the last confirmed epoch
    if stop_epoch is None:
        stop_epoch = last_confirmed_epoch
    # Stop epoch given: use the aggregation period to ceil it
    elif int(stop_epoch) > last_confirmed_epoch:
        stop_epoch = last_confirmed_epoch
    # Subtract one from the stop epoch so it is not inclusive with the next aggregation period
    if stop_epoch % 1000 == 0:
        stop_epoch -= 1
    stop_epoch = int(stop_epoch / aggregation_epochs + 1) * aggregation_epochs

    # Need at least a difference of one day worth of epochs between the start and stop epoch
    if start_epoch == stop_epoch:
        start_epoch -= aggregation_epochs

    return start_epoch, stop_epoch


def translate_address_ids(database, id_values, logger):
    sql = """
        SELECT
            address,
            id
        FROM
            addresses
    """
    addresses = database.sql_return_all(sql)

    # Transform list of data to dictionary
    ids_to_addresses = {}
    if addresses:
        for address, address_id in addresses:
            ids_to_addresses[address_id] = address

    # Translate address ids to addresses
    id_to_address = []
    for address_id, value in id_values:
        address_id = int(address_id)
        if address_id not in ids_to_addresses:
            logger.warning(f"Could not find address value for id {address_id}")
            id_to_address.append({"address": str(address_id), "amount": value})
        else:
            id_to_address.append(
                {"address": str(ids_to_addresses[address_id]), "amount": value}
            )

    return sorted(
        id_to_address,
        key=lambda am_ad: (am_ad["amount"], am_ad["address"]),
        reverse=True,
    )
