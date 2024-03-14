from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.supply_schema import NetworkSupplyArgs

network_supply_blueprint = Blueprint(
    "supply info",
    "supply info",
    description="Fetch supply info statistics as single integer numbers.",
)


@network_supply_blueprint.route("/supply")
class SupplyInfo(MethodView):
    @network_supply_blueprint.arguments(NetworkSupplyArgs, location="query")
    @network_supply_blueprint.response(
        200,
        description="Returns a single integer value for the requested supply info key.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @network_supply_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not find supply info data in the cache.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]

        key = args["key"]
        logger.info(f"supply({key})")

        home = cache.get("home")
        if not home:
            logger.error("Could not find supply_info in memcached cache")
            abort(404, message="Could not find supply info data in the cache.")
        else:
            logger.info("Found supply_info in memcached cache")

        if key in (
            "blocks_minted",
            "blocks_missing",
            "current_time",
            "epoch",
            "in_flight_requests",
        ):
            return str(int(home["supply_info"][key])), 200, {"X-Version": "1.0.0"}
        else:
            return (
                str(int(home["supply_info"][key] / 1e9)),
                200,
                {"X-Version": "1.0.0"},
            )
