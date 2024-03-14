from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import INCLUDE, ValidationError

from schemas.include.post_transaction_schema import PostTransaction
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.transaction.send_schema import ValueTransferArgs, ValueTransferResponse

transaction_send_blueprint = Blueprint(
    "transaction send",
    "transaction send",
    description="Send a value transfer transaction.",
)


@transaction_send_blueprint.route("/send")
class TransactionSend(MethodView):
    # ValueTransferArgs does not include the actual transaction as an argument
    # The API does require the argument to be present (see test below)
    # But not testing it as part of the argument allows for custom error messages
    @transaction_send_blueprint.arguments(
        ValueTransferArgs(unknown=INCLUDE),
        examples={
            "Value transfer transaction": {
                "description": "Example of an expected request body format to send a value transfer transaction.",
                "value": {
                    "test": False,
                    "transaction": {
                        "ValueTransfer": {
                            "body": {
                                "inputs": [{"output_pointer": "string"}],
                                "outputs": [
                                    {"pkh": "string", "time_lock": 0, "value": 1}
                                ],
                            },
                            "signatures": [
                                {
                                    "public_key": {"bytes": "string", "compressed": 0},
                                    "signature": None,
                                }
                            ],
                        },
                    },
                },
            },
            "Stake transaction": {
                "description": "Example of an expected request body format to send a stake transaction.",
                "value": {
                    "test": False,
                    "transaction": {
                        "Stake": {},
                    },
                },
            },
            "Unstake transaction": {
                "description": "Example of an expected request body format to send an unstake transaction.",
                "value": {
                    "test": False,
                    "transaction": {
                        "Unstake": {},
                    },
                },
            },
        },
    )
    @transaction_send_blueprint.response(
        201,
        ValueTransferResponse,
        description="Returns whether the value transfer was valid.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @transaction_send_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Missing transaction argument.",
                "Failed to validate value transfer.",
                "Could not send value transfer: {reason}.",
                "Unexpectedly could not send value transfer.",
            ]
        },
    )
    def post(self, args):
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        logger.info(f"transaction_send({args['test']})")

        if "transaction" not in args:
            logger.error(f"Missing transaction argument: {args}")
            abort(404, message="Missing transaction argument.")
        prefix = "Testing" if args["test"] else "Sending"
        logger.info(f"{prefix} transaction: {args['transaction']}")

        try:
            transaction = PostTransaction().load(args["transaction"])
        except ValidationError as err_info:
            logger.error(f"Failed to validate value transfer: {err_info}")
            abort(404, message="Failed to validate value transfer.")

        if args["test"]:
            return (
                ValueTransferResponse().load({"result": "Value transfer is valid."}),
                201,
                {"X-Version": "1.0.0"},
            )
        else:
            response = witnet_node.send_vtt({"transaction": transaction})
            if "reason" in response:
                logger.error(
                    f"Could not send value transfer: {response['reason']['message']}"
                )
                abort(
                    404,
                    message=f"Could not send value transfer: {response['reason']['message']}.",
                )
            else:
                if "result" in response and response["result"]:
                    return (
                        ValueTransferResponse().load(
                            {"result": "Succesfully sent value transfer."}
                        ),
                        201,
                        {"X-Version": "1.0.0"},
                    )
                else:
                    logger.error(
                        f"Unexpectedly could not send value transfer: {response}"
                    )
                    abort(404, message="Unexpectedly could not send value transfer.")
