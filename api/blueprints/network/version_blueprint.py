from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.config import BlockchainConfig
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.version_schema import NetworkVersionArgs, NetworkVersionResponse
from util.blockchain_functions import calculate_current_epoch

network_version_blueprint = Blueprint(
    "network versions info",
    "network versions info",
    description="Fetch the current and future network versions info.",
)


@network_version_blueprint.route("/version")
class NetworkVersion(MethodView):
    @network_version_blueprint.arguments(NetworkVersionArgs, location="query")
    @network_version_blueprint.response(
        200,
        NetworkVersionResponse,
        description="Returns the current and future network versions info.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @network_version_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for network versions data.",
                "Could not fetch network versions.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        config = BlockchainConfig.config

        logger.info(f"network_version({args['key']})")

        if args["key"] == "all":
            versions = cache.get("network_version_all")
        else:
            versions = cache.get("network_version_current")
        if versions:
            logger.info("Found the network versions in our memcached instance")
            return (
                versions,
                200,
                {"X-Version": "1.0.0"},
            )

        versions = witnet_node.get_protocol_info()
        if "result" in versions:
            logger.info("Fetched network versions from node")
            versions = versions["result"]

            if args["key"] == "all":
                all_versions = []
                for version, period in versions["all_checkpoints_periods"].items():
                    all_versions.append(
                        {
                            "version": version,
                            "epoch": versions["all_versions"]["efv"][version],
                            "period": period,
                        }
                    )
                versions = {
                    "current_version": versions["current_version"],
                    "current_epoch": calculate_current_epoch(),
                    "versions": all_versions,
                }
            else:
                versions = {
                    "current_version": versions["current_version"],
                    "current_epoch": calculate_current_epoch(),
                }

            try:
                cache.set(
                    f"network_version_{args['key']}",
                    NetworkVersionResponse().load(versions),
                    timeout=config["api"]["caching"]["views"]["version"]["timeout"],
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for network versions data: {err_info}"
                )
                abort(
                    404,
                    message="Incorrect message format for network versions data.",
                    headers={"X-Version": "1.0.0"},
                )

            return (
                versions,
                200,
                {"X-Version": "1.0.0"},
            )
        else:
            logger.error(f"Could not fetch network versions: {versions['error']}")
            abort(
                404,
                message="Could not fetch network versions.",
                headers={"X-Version": "1.0.0"},
            )
