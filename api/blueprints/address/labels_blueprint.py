from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.address.labels_schema import AddressLabelResponse
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from util.data_transformer import re_sql

address_labels_blueprint = Blueprint(
    "address labels",
    "address labels",
    description="Fetch labels for all labeled addresses.",
)


@address_labels_blueprint.route("/labels")
class AddressLabels(MethodView):
    @address_labels_blueprint.response(
        200,
        AddressLabelResponse(many=True),
        description="Returns a list of addresses with labels.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_labels_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for address labels for {addresses}.",
            ]
        },
    )
    def get(self):
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]

        logger.info("address_labels()")

        sql = """
            SELECT
                address,
                label
            FROM
                addresses
            WHERE
                label IS NOT NULL
        """
        addresses = database.sql_return_all(re_sql(sql))
        if addresses:
            logger.info(f"Returning {len(addresses)} addresses with labels")
            try:
                return (
                    AddressLabelResponse(many=True).load(
                        [
                            {
                                "address": address[0],
                                "label": address[1],
                            }
                            for address in addresses
                        ]
                    ),
                    200,
                    {"X-Version": "1.0.0"},
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for address labels for {addresses}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for address labels for {addresses}.",
                    headers={"X-Version": "1.0.0"},
                )
        else:
            return [], 200, {"X-Version": "1.0.0"}
