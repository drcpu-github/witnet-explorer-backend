from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.transaction.nonce_schema import (
    TransactionNonceArgs,
    TransactionNonceResponse,
)

transaction_nonce_blueprint = Blueprint(
    "transaction nonce",
    "transaction nonce",
    description="Fetch nonce for unstake transaction from a validator-withdrawer pair.",
)


@transaction_nonce_blueprint.route("/nonce")
class Reputation(MethodView):
    @transaction_nonce_blueprint.arguments(TransactionNonceArgs, location="query")
    @transaction_nonce_blueprint.response(
        200,
        TransactionNonceResponse,
        description="Returns the nonce for a validator-withdrawer pair.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @transaction_nonce_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not fetch nonce data.",
                "Incorrect message format for nonce data.",
            ]
        },
    )
    def get(self, args):
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        validator, withdrawer = args["validator"], args["withdrawer"]
        logger.info(f"get_nonce({validator}, {withdrawer})")

        # Nullify withdrawer until wit/2.1 is live
        stakes = witnet_node.get_stakes(validator, None)
        if "result" in stakes:
            nonce_response = {
                "validator": validator,
                "withdrawer": withdrawer,
                "nonce": stakes["result"][0]["value"]["nonce"],
            }
        else:
            logger.error(
                f"Could not fetch nonce for ({validator}, {withdrawer}): {stakes}"
            )
            abort(
                404,
                message=f"Could not fetch nonce for ({validator}, {withdrawer}).",
                headers={"X-Version": "1.0.0"},
            )

        try:
            TransactionNonceResponse().load(nonce_response)
        except ValidationError as err_info:
            logger.error(
                f"Incorrect message format for nonce data for ({validator}, {withdrawer}): {err_info}"
            )
            abort(
                404,
                message="Incorrect message format for nonce data.",
                headers={"X-Version": "1.0.0"},
            )

        return nonce_response, 200, {"X-Version": "1.0.0"}
