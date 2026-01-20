import time

from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.config import BlockchainConfig
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.stakes_schema import NetworkStakesArgs, NetworkStakesResponse

network_stakes_blueprint = Blueprint(
    "network stakes",
    "network stakes",
    description="Fetch the current network stakes.",
)


@network_stakes_blueprint.route("/stakes")
class NetworkStakes(MethodView):
    @network_stakes_blueprint.arguments(NetworkStakesArgs, location="query")
    @network_stakes_blueprint.response(
        200,
        NetworkStakesResponse,
        description="Returns the current network stakes.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @network_stakes_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for network stakes data.",
                "Could not fetch network stakes.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        config = BlockchainConfig.config

        if "validator" in args and "withdrawer" in args:
            logger.info(
                f"network_stakes(validator={args['validator']}, withdrawer={args['withdrawer']})"
            )
        elif "validator" in args:
            logger.info(f"network_stakes(validator={args['validator']})")
        elif "withdrawer" in args:
            logger.info(f"network_stakes(withdrawer={args['withdrawer']})")
        else:
            logger.info("network_stakes(all)")

        stakes = cache.get("network_stakes_all")
        if stakes:
            logger.info("Found the network stakes in our memcached instance")
            return (
                {
                    "stakes": self.filter_stakes(args, stakes["stakes"]),
                    "last_updated": stakes["last_updated"],
                },
                200,
                {"X-Version": "1.0.0"},
            )

        # Could not fetch required data from the cache
        # Building the full response with all metadata is too expensive, so set metadata to 0
        last_updated = int(time.time())
        stakes = witnet_node.get_stakes(None, None)
        if "result" in stakes:
            logger.info("Fetched network stakes from node")
            stakes = stakes["result"]

            all_stakes = [
                {
                    "validator": stake["key"]["validator"],
                    "withdrawer": stake["key"]["withdrawer"],
                    "current_stake": stake["value"]["coins"],
                    "time_weighted_stake": 0,
                    "nonce": stake["value"]["nonce"],
                    "blocks": 0,
                    "rewards": 0,
                    "data_requests": 0,
                    "lies": 0,
                    "genesis": 0,
                    "last_active": 0,
                }
                for stake in stakes
            ]

            try:
                cache.set(
                    "network_stakes_all",
                    NetworkStakesResponse().load(
                        {
                            "stakes": all_stakes,
                            "last_updated": last_updated,
                        }
                    ),
                    timeout=config["api"]["caching"]["scripts"]["stakes"]["timeout"],
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for network stakes data: {err_info}"
                )
                abort(
                    404,
                    message="Incorrect message format for network stakes data.",
                    headers={"X-Version": "1.0.0"},
                )

            return (
                {
                    "stakes": self.filter_stakes(args, all_stakes),
                    "last_updated": last_updated,
                },
                200,
                {"X-Version": "1.0.0"},
            )
        else:
            logger.error(f"Could not fetch network stakes: {stakes['error']}")
            abort(
                404,
                message="Could not fetch network stakes.",
                headers={"X-Version": "1.0.0"},
            )

    def filter_stakes(self, args, stakes):
        if "validator" in args and "withdrawer" in args:
            for stake in stakes:
                if (
                    stake["validator"] == args["validator"]
                    and stake["withdrawer"] == args["withdrawer"]
                ):
                    return [stake]
        elif "validator" in args:
            selected_stakes = []
            for stake in stakes:
                if stake["validator"] == args["validator"]:
                    selected_stakes.append(stake)
            return selected_stakes
        elif "withdrawer" in args:
            selected_stakes = []
            for stake in stakes:
                if stake["withdrawer"] == args["withdrawer"]:
                    selected_stakes.append(stake)
            return selected_stakes
        else:
            return stakes
