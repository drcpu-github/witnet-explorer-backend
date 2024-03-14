import time

import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.reputation_schema import NetworkReputationResponse

network_reputation_blueprint = Blueprint(
    "Reputation",
    "reputation",
    description="Fetch reputation for all addresses in the ARS.",
)


@network_reputation_blueprint.route("/reputation")
class Reputation(MethodView):
    @network_reputation_blueprint.response(
        200,
        NetworkReputationResponse,
        description="Returns a list of addresses, their reputation and eligibity.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @network_reputation_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not retrieve reputation data.",
                "Incorrect message format for reputation.",
            ]
        },
    )
    def get(self):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        logger.info("reputation()")

        reputation = cache.get("reputation")
        if not reputation:
            logger.info("Could not find reputation in our memcached instance.")

            result = witnet_node.get_reputation_all()
            if "result" not in result:
                logger.error(f"Could not retrieve reputation data: {result['error']}")
                abort(404, message="Could not retrieve reputation data.")

            # Parse reputation statistics
            stats = result["result"]["stats"]
            total_reputation = result["result"]["total_reputation"]
            # Only keep identities with a non-zero reputation
            reputation = [
                {
                    "address": key,
                    "reputation": stats[key]["reputation"],
                    "eligibility": stats[key]["eligibility"] / total_reputation * 100,
                }
                for key in stats.keys()
                if stats[key]["reputation"] > 0
            ]
            reputation = sorted(
                reputation, key=lambda rep: rep["reputation"], reverse=True
            )

            try:
                reputation = NetworkReputationResponse().load(
                    {
                        "reputation": reputation,
                        "total_reputation": result["result"]["total_reputation"],
                        "last_updated": int(time.time()),
                    }
                )
            except ValidationError as err_info:
                logger.error(f"Incorrect message format for reputation: {err_info}")
                abort(404, message="Incorrect message format for reputation.")

            # Try to save the data in our memcached instance
            try:
                cache.set("reputation", reputation)
            except pylibmc.TooBig:
                logger.warning(
                    "Could not save reputation data in our memcached instance because the item size exceeded 1MB"
                )
        else:
            logger.info("Found reputation in our memcached instance.")

        return reputation, 200, {"X-Version": "1.0.0"}
