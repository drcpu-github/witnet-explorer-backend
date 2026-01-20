from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import INCLUDE, ValidationError

from schemas.include.post_transaction_schema import PostTransaction
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.transaction.send_schema import SendArgs, SendResponse

transaction_send_blueprint = Blueprint(
    "transaction send",
    "transaction send",
    description="Send a transaction.",
)


@transaction_send_blueprint.route("/send")
class TransactionSend(MethodView):
    # SendArgs does not include the actual transaction as an argument
    # The API does require the argument to be present (see test below)
    # But not testing it as part of the argument allows for custom error messages
    @transaction_send_blueprint.arguments(
        SendArgs(unknown=INCLUDE),
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
                                    "signature": {
                                        "Secp256k1": {
                                            "der": "string",
                                        },
                                    },
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
                        "Stake": {
                            "body": {
                                "inputs": [{"output_pointer": "string"}],
                                "output": {
                                    "authorization": {
                                        "public_key": {
                                            "bytes": "string",
                                            "compressed": 0,
                                        },
                                        "signature": {
                                            "Secp256k1": {
                                                "der": "string",
                                            },
                                        },
                                    },
                                    "key": {
                                        "validator": "address",
                                        "withdrawer": "address",
                                    },
                                    "value": 0,
                                },
                                "change": {"pkh": "string", "time_lock": 0, "value": 1},
                            },
                            "signatures": [
                                {
                                    "public_key": {"bytes": "string", "compressed": 0},
                                    "signature": {
                                        "Secp256k1": {
                                            "der": "string",
                                        },
                                    },
                                }
                            ],
                        },
                    },
                },
            },
            "Unstake transaction": {
                "description": "Example of an expected request body format to send an unstake transaction.",
                "value": {
                    "test": False,
                    "transaction": {
                        "Unstake": {
                            "body": {
                                "operator": "address",
                                "withdrawal": {
                                    "pkh": "string",
                                    "time_lock": 0,
                                    "value": 1,
                                },
                                "nonce": 1,
                                "fee": 1,
                            },
                            "signature": {
                                "public_key": {"bytes": "string", "compressed": 0},
                                "signature": {
                                    "Secp256k1": {
                                        "der": "string",
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    )
    @transaction_send_blueprint.response(
        201,
        SendResponse,
        description="Returns whether the transaction was valid.",
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
                "Failed to validate transaction.",
                "Could not send transaction: {reason}.",
                "Unexpectedly could not send transaction.",
            ]
        },
    )
    def post(self, args):
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        logger.info(f"transaction_send({args['test']})")

        if "transaction" not in args:
            logger.error(f"Missing transaction argument: {args}")
            abort(
                404,
                message="Missing transaction argument.",
                headers={"X-Version": "2.0.0"},
            )
        prefix = "Testing" if args["test"] else "Sending"
        logger.info(f"{prefix} transaction: {args['transaction']}")

        try:
            transaction = PostTransaction().load(args["transaction"])
        except ValidationError as err_info:
            logger.error(f"Failed to validate transaction: {err_info}")
            abort(
                404,
                message="Failed to validate transaction.",
                headers={"X-Version": "2.0.0"},
            )

        if args["test"]:
            return (
                SendResponse().load({"result": "Transaction is valid."}),
                201,
                {"X-Version": "2.0.0"},
            )
        else:
            response = witnet_node.send_transaction({"transaction": transaction})
            if "reason" in response:
                logger.error(f"Could not send transaction: {response['reason']}")
                abort(
                    404,
                    message=f"Could not send transaction: {response['reason']}.",
                    headers={"X-Version": "2.0.0"},
                )
            else:
                if "result" in response and response["result"]:
                    return (
                        SendResponse().load(
                            {"result": "Succesfully sent transaction."}
                        ),
                        201,
                        {"X-Version": "2.0.0"},
                    )
                else:
                    logger.error(f"Unexpectedly could not send transaction: {response}")
                    abort(404, message="Unexpectedly could not send transaction.")
