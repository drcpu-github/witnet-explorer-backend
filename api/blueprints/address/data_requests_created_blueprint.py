from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.objects.address import Address
from schemas.address.data_request_view_schema import DataRequestCreatedView
from schemas.include.address_schema import AddressSchema
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from util.common_functions import send_address_caching_request

address_data_requests_created_blueprint = Blueprint(
    "address data requests created",
    "address data requests created",
    description="List the data requests created by an address.",
)


@address_data_requests_created_blueprint.route("/data-requests-created")
class AddressDataRequestsCreated(MethodView):
    @address_data_requests_created_blueprint.arguments(AddressSchema, location="query")
    @address_data_requests_created_blueprint.response(
        200,
        DataRequestCreatedView(many=True),
        description="Returns a list of data requests created for an address.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @address_data_requests_created_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for data requests created data for {address}.",
            ]
        },
    )
    @address_data_requests_created_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, args, pagination_parameters):
        address_caching_server = current_app.extensions["address_caching_server"]
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        arg_address = args["address"]
        logger.info(f"address_data_requests_created({arg_address})")

        request = {"method": "track", "addresses": [arg_address], "id": 1}
        send_address_caching_request(logger, address_caching_server, request)

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size

        # Try to fetch the result from the cache
        cached_data_requests_created = cache.get(f"{arg_address}_data-requests-created")
        # Return cached version if found (fast)
        if cached_data_requests_created:
            logger.info(
                f"Found {len(cached_data_requests_created)} data requests created for {arg_address} in cache"
            )
            pagination_parameters.item_count = len(cached_data_requests_created)
            return (
                cached_data_requests_created[start:stop],
                200,
                {"X-Version": "v1.0.0"},
            )
        # Query the database and build the requested view (slow)
        else:
            logger.info(
                f"Did not find data requests created for {arg_address} in cache, querying database"
            )
            address = Address(
                arg_address,
                config,
                database=database,
                witnet_node=witnet_node,
                logger=logger,
            )
            data_requests_created = address.get_data_requests_created()
            try:
                DataRequestCreatedView(many=True).load(data_requests_created)
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for data requests created for {arg_address}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for data requests created data for {arg_address}.",
                )
            pagination_parameters.item_count = len(data_requests_created)
            return data_requests_created[start:stop], 200, {"X-Version": "v1.0.0"}
