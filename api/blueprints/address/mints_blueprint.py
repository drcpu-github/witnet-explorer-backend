from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.objects.address import Address
from schemas.address.mint_view_schema import MintView
from schemas.include.address_schema import AddressSchema
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from util.common_functions import send_address_caching_request

address_mints_blueprint = Blueprint(
    "address mints",
    "address mints",
    description="List the mint transactions received by an address.",
)


@address_mints_blueprint.route("/mints")
class AddressMints(MethodView):
    @address_mints_blueprint.arguments(AddressSchema, location="query")
    @address_mints_blueprint.response(
        200,
        MintView(many=True),
        description="Returns a list of mint transactions received by an address.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_mints_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for mint data for {address}.",
            ]
        },
    )
    @address_mints_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, args, pagination_parameters):
        address_caching_server = current_app.extensions["address_caching_server"]
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        arg_address = args["address"]
        logger.info(f"address_mints({arg_address})")

        request = {"method": "track", "addresses": [arg_address], "id": 1}
        send_address_caching_request(logger, address_caching_server, request)

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size

        # Try to fetch the result from the cache
        cached_mints = cache.get(f"{arg_address}_mints")
        # Return cached version if found (fast)
        if cached_mints:
            logger.info(
                f"Found {len(cached_mints)} mint transactions for {arg_address} in cache"
            )
            pagination_parameters.item_count = len(cached_mints)
            return cached_mints[start:stop], 200, {"X-Version": "v1.0.0"}
        # Query the database and build the requested view (slow)
        else:
            logger.info(
                f"Did not find mints for {arg_address} in cache, querying database"
            )
            address = Address(
                arg_address,
                config,
                database=database,
                witnet_node=witnet_node,
                logger=logger,
            )
            mints = address.get_mints()
            try:
                MintView(many=True).load(mints)
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for mint data for {arg_address}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for mint data for {arg_address}.",
                )
            pagination_parameters.item_count = len(mints)
            return mints[start:stop], 200, {"X-Version": "v1.0.0"}
