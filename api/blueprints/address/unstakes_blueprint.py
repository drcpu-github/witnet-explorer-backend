from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from api.connect import send_address_caching_request
from blockchain.objects.address import Address
from schemas.address.unstake_view_schema import UnstakeView
from schemas.include.address_schema import AddressSchema
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema

address_unstakes_blueprint = Blueprint(
    "address unstakes",
    "address unstakes",
    description="List the unstake transactions of an address.",
)


@address_unstakes_blueprint.route("/unstakes")
class AddressUnstakes(MethodView):
    @address_unstakes_blueprint.arguments(AddressSchema, location="query")
    @address_unstakes_blueprint.response(
        200,
        UnstakeView(many=True),
        description="Returns a list of unstakes for an address.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_unstakes_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for unstake data for {address}.",
            ]
        },
    )
    @address_unstakes_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, args, pagination_parameters):
        address_caching_server = current_app.extensions["address_caching_server"]
        cache = current_app.extensions["cache"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        arg_address = args["address"]
        logger.info(f"address_unstakes({arg_address})")

        request = {"method": "track", "addresses": [arg_address], "id": 1}
        send_address_caching_request(logger, address_caching_server, request)

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size

        # Try to fetch the result from the cache
        cached_unstakes = cache.get(f"{arg_address}_unstakes")
        # Return cached version if found (fast)
        if cached_unstakes is not None:
            logger.info(
                f"Found {len(cached_unstakes)} unstakes for {arg_address} in cache"
            )
            pagination_parameters.item_count = len(cached_unstakes)
            return cached_unstakes[start:stop], 200, {"X-Version": "1.0.0"}
        # Query the database and build the requested view (slow)
        else:
            logger.info(
                f"Did not find unstakes for {arg_address} in cache, querying database"
            )
            address = Address(
                arg_address,
                database=database,
                witnet_node=witnet_node,
                logger=logger,
            )
            unstakes = address.get_unstakes()
            try:
                UnstakeView(many=True).load(unstakes[start:stop])
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for unstake data for {arg_address}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for unstake data for {arg_address}.",
                    headers={"X-Version": "1.0.0"},
                )
            pagination_parameters.item_count = len(unstakes)
            return unstakes[start:stop], 200, {"X-Version": "1.0.0"}
