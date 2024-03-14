import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.transaction.mempool_schema import (
    TransactionMempoolArgs,
    TransactionMempoolResponse,
)

transaction_mempool_blueprint = Blueprint(
    "transaction mempool",
    "transaction mempool",
    description="Fetch a snapshot of the current mempool transactions.",
)


@transaction_mempool_blueprint.route("/mempool")
class TransactionMempool(MethodView):
    @transaction_mempool_blueprint.arguments(TransactionMempoolArgs, location="query")
    @transaction_mempool_blueprint.response(
        200,
        TransactionMempoolResponse,
        description="Returns a snapshot of the current mempool transactions split by data requests and value transfers.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @transaction_mempool_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for mempool data.",
                "Could not fetch the live mempool.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        logger.info(f"network_mempool({args['type']})")

        mempool = cache.get("transaction_mempool")
        if mempool:
            logger.info("Found the live mempool in our memcached instance")
            return (
                build_return_value(args["type"], mempool),
                200,
                {"X-Version": "1.0.0"},
            )

        mempool = witnet_node.get_mempool()
        if "result" in mempool:
            logger.info("Fetched transaction mempool from node")
            mempool = mempool["result"]

            try:
                cache.set(
                    "transaction_mempool",
                    TransactionMempoolResponse().load(mempool),
                    timeout=config["api"]["caching"]["views"]["mempool"]["timeout"],
                )
            except ValidationError as err_info:
                logger.error(f"Incorrect message format for mempool data: {err_info}")
                abort(
                    404,
                    message="Incorrect message format for mempool data.",
                )
            except pylibmc.TooBig:
                logger.warning(
                    "Could not save the mempool in our memcached instance because the item size exceeded 1MB"
                )

            return (
                build_return_value(args["type"], mempool),
                200,
                {"X-Version": "1.0.0"},
            )
        else:
            logger.error(f"Could not fetch the live mempool: {mempool['error']}")
            abort(
                404,
                message="Could not fetch the live mempool.",
            )


def build_return_value(txn_type, mempool):
    if txn_type == "all":
        return {
            "data_request": mempool["data_request"],
            "value_transfer": mempool["value_transfer"],
        }
    if txn_type == "data_requests":
        return {"data_request": mempool["data_request"]}
    else:
        return {"value_transfer": mempool["value_transfer"]}
