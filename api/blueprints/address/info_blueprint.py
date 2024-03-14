from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.address.info_schema import AddressInfoArgs, AddressInfoResponse
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from util.data_transformer import re_sql

address_info_blueprint = Blueprint(
    "address info",
    "address info",
    description="Fetch address label, block and transactions info for a set of addresses.",
)


@address_info_blueprint.route("/info")
class AddressInfo(MethodView):
    @address_info_blueprint.arguments(AddressInfoArgs, location="query")
    @address_info_blueprint.response(
        200,
        AddressInfoResponse(many=True),
        description="Returns a list of addresses with label, block and transaction metadata.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_info_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for address info for {addresses}.",
            ]
        },
    )
    def get(self, args):
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]

        addresses = args["addresses"]
        logger.info(f"address_info({addresses})")

        sql = """
            SELECT
                address,
                label,
                active,
                block,
                mint,
                value_transfer,
                data_request,
                commit,
                reveal,
                tally
            FROM
                addresses
            WHERE
                address = ANY(%s)
        """
        addresses = database.sql_return_all(re_sql(sql), [addresses])
        if addresses:
            logger.info(f"Found {len(addresses)} out of {len(addresses)}")

            try:
                return (
                    AddressInfoResponse(many=True).load(
                        [
                            {
                                "address": address[0],
                                "label": address[1] if address[1] else "",
                                "active": address[2],
                                "block": address[3],
                                "mint": address[4],
                                "value_transfer": address[5],
                                "data_request": address[6],
                                "commit": address[7],
                                "reveal": address[8],
                                "tally": address[9],
                            }
                            for address in addresses
                        ]
                    ),
                    200,
                    {"X-Version": "1.0.0"},
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for address info for {addresses}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for address info for {addresses}.",
                )
        else:
            return [], 200, {"X-Version": "1.0.0"}
