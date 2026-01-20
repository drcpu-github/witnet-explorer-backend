from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.config import BlockchainConfig
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.transaction.priority_schema import (
    TransactionPriorityArgs,
    TransactionPriorityResponse,
)

transaction_priority_blueprint = Blueprint(
    "transaction priority",
    "transaction priority",
    description="Fetch recommended transaction priority fees.",
)


@transaction_priority_blueprint.route("/priority")
class TransactionPriority(MethodView):
    @transaction_priority_blueprint.arguments(TransactionPriorityArgs, location="query")
    @transaction_priority_blueprint.response(
        200,
        TransactionPriorityResponse,
        description="Returns a set of required transaction fees for a defined expected time to block.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @transaction_priority_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for priority.",
                "Could not fetch transaction priority fees.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        config = BlockchainConfig.config
        cache_timeout = config["api"]["caching"]["views"]["priority"]["timeout"]

        priority_key = args["key"]

        logger.info(f"get_priority({priority_key})")

        priority = cache.get("priority")
        if not priority:
            logger.info("Could not find 'priority' in memcached cache")
            priority = witnet_node.get_priority()
            if "result" in priority:
                result = priority["result"]
                priority = {}
                for key in result:
                    transaction, speed = key.split("_")
                    if transaction not in priority:
                        priority[transaction] = {}
                    priority[transaction][speed] = result[key]
                try:
                    cache.set(
                        "priority",
                        TransactionPriorityResponse().load(priority),
                        timeout=cache_timeout,
                    )
                except ValidationError as err_info:
                    logger.error(f"Incorrect message format for priority: {err_info}")
                    abort(
                        404,
                        message="Incorrect message format for priority.",
                        headers={"X-Version": "2.0.0"},
                    )
            else:
                logger.error(
                    f"Could not fetch transaction priority fees: {priority['error']}"
                )
                abort(
                    404,
                    message="Could not fetch transaction priority fees.",
                    headers={"X-Version": "2.0.0"},
                )
        else:
            logger.info("Found 'priority' in memcached cache")

        if priority_key == "all":
            return priority, 200, {"X-Version": "2.0.0"}
        else:
            filtered_priority = {priority_key: priority[priority_key]}
            return filtered_priority, 200, {"X-Version": "2.0.0"}
