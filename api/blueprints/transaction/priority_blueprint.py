from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

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
        config = current_app.config["explorer"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        priority_key = args["key"]

        logger.info(f"get_priority({priority_key})")

        priority = cache.get("priority")
        if not priority:
            logger.info("Could not find 'priority' in memcached cache")
            priority = witnet_node.get_priority()
            if "result" in priority:
                priority = priority["result"]
                try:
                    cache.set(
                        "priority",
                        TransactionPriorityResponse().load(priority),
                        timeout=config["api"]["caching"]["views"]["priority"][
                            "timeout"
                        ],
                    )
                except ValidationError as err_info:
                    logger.error(f"Incorrect message format for priority: {err_info}")
                    abort(
                        404,
                        message="Incorrect message format for priority.",
                        headers={"X-Version": "1.0.0"},
                    )
            else:
                logger.error(
                    f"Could not fetch transaction priority fees: {priority['error']}"
                )
                abort(
                    404,
                    message="Could not fetch transaction priority fees.",
                    headers={"X-Version": "1.0.0"},
                )
        else:
            logger.info("Found 'priority' in memcached cache")

        if priority_key == "all":
            return priority, 200, {"X-Version": "1.0.0"}
        else:
            filtered_priority = {
                key: priority[key]
                for key in priority.keys()
                if key.startswith(priority_key)
            }
            return filtered_priority, 200, {"X-Version": "1.0.0"}
