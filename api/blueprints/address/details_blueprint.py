from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.objects.address import Address
from schemas.address.details_view_schema import DetailsView
from schemas.include.address_schema import AddressSchema
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from util.common_functions import send_address_caching_request

address_details_blueprint = Blueprint(
    "address details",
    "address details",
    description="List the balance and reputation details of address.",
)


@address_details_blueprint.route("/details")
class AddressDetails(MethodView):
    @address_details_blueprint.arguments(AddressSchema, location="query")
    @address_details_blueprint.response(
        200,
        DetailsView,
        description="Returns the balance, reputation and label of an address.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_details_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for details data for {address}.",
            ]
        },
    )
    def get(self, args):
        address_caching_server = current_app.extensions["address_caching_server"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        arg_address = args["address"]
        logger.info(f"address_details({arg_address})")

        request = {"method": "track", "addresses": [arg_address], "id": 1}
        send_address_caching_request(logger, address_caching_server, request)

        address = Address(
            arg_address,
            config,
            database=database,
            witnet_node=witnet_node,
            logger=logger,
        )
        details = address.get_details()
        try:
            DetailsView().load(details)
        except ValidationError as err_info:
            logger.error(
                f"Incorrect message format for details data for {arg_address}: {err_info}"
            )
            abort(
                404,
                message=f"Incorrect message format for details data for {arg_address}.",
                headers={"X-Version": "1.0.0"},
            )
        return details, 200, {"X-Version": "1.0.0"}
