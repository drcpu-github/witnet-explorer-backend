from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.objects.address import Address
from schemas.address.value_transfer_view_schema import ValueTransferView
from schemas.include.address_schema import AddressSchema
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from util.common_functions import send_address_caching_request

address_value_transfers_blueprint = Blueprint(
    "address value transfers",
    "address value transfers",
    description="List the value transfers transactions of an address.",
)


@address_value_transfers_blueprint.route("/value-transfers")
class AddressValueTransfers(MethodView):
    @address_value_transfers_blueprint.arguments(AddressSchema, location="query")
    @address_value_transfers_blueprint.response(
        200,
        ValueTransferView(many=True),
        description="Returns a list of value transfers for an address.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_value_transfers_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for value transfer data for {address}.",
            ]
        },
    )
    @address_value_transfers_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, args, pagination_parameters):
        address_caching_server = current_app.extensions["address_caching_server"]
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        arg_address = args["address"]
        logger.info(f"address_value_transfers({arg_address})")

        request = {"method": "track", "addresses": [arg_address], "id": 1}
        send_address_caching_request(logger, address_caching_server, request)

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size

        # Try to fetch the result from the cache
        cached_value_transfers = cache.get(f"{arg_address}_value-transfers")
        # Return cached version if found (fast)
        if cached_value_transfers:
            logger.info(
                f"Found {len(cached_value_transfers)} value transfers for {arg_address} in cache"
            )
            pagination_parameters.item_count = len(cached_value_transfers)
            return cached_value_transfers[start:stop], 200, {"X-Version": "v1.0.0"}
        # Query the database and build the requested view (slow)
        else:
            logger.info(
                f"Did not find value transfers for {arg_address} in cache, querying database"
            )
            address = Address(
                arg_address,
                config,
                database=database,
                witnet_node=witnet_node,
                logger=logger,
            )
            value_transfers = address.get_value_transfers()
            try:
                ValueTransferView(many=True).load(value_transfers[start:stop])
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for value transfer data for {arg_address}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for value transfer data for {arg_address}.",
                )
            pagination_parameters.item_count = len(value_transfers)
            return value_transfers[start:stop], 200, {"X-Version": "v1.0.0"}
