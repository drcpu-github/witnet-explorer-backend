from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from objects.address import Address
from schemas.address.data_request_view_schema import DataRequestSolvedView
from schemas.include.address_schema import AddressSchema
from schemas.misc.abort_schema import AbortSchema
from util.common_functions import send_address_caching_request

address_data_requests_solved_blueprint = Blueprint(
    "address data requests solved",
    "address data requests solved",
    description="List the data requests solved by an address.",
)


@address_data_requests_solved_blueprint.route("/data-requests-solved")
class AddressDataRequestsSolved(MethodView):
    @address_data_requests_solved_blueprint.arguments(AddressSchema, location="query")
    @address_data_requests_solved_blueprint.response(
        200,
        DataRequestSolvedView(many=True),
        description="Returns a list of data requests solved for an address.",
    )
    @address_data_requests_solved_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for data requests solved data for {address}.",
            ]
        },
    )
    @address_data_requests_solved_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, args, pagination_parameters):
        address_caching_server = current_app.extensions["address_caching_server"]
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        arg_address = args["address"]
        logger.info(f"address_data_requests_solved({arg_address})")

        request = {"method": "track", "addresses": [arg_address], "id": 1}
        send_address_caching_request(logger, address_caching_server, request)

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size

        # Try to fetch the result from the cache
        cached_data_requests_solved = cache.get(f"{arg_address}_data-requests-solved")
        # Return cached version if found (fast)
        if cached_data_requests_solved:
            logger.info(
                f"Found {len(cached_data_requests_solved)} data requests solved for {arg_address} in cache"
            )
            pagination_parameters.item_count = len(cached_data_requests_solved)
            return cached_data_requests_solved[start:stop]
        # Query the database and build the requested view (slow)
        else:
            logger.info(
                f"Did not find data requests solved for {arg_address} in cache, querying database"
            )
            address = Address(
                arg_address,
                config,
                database=database,
                witnet_node=witnet_node,
                logger=logger,
            )
            data_requests_solved = address.get_data_requests_solved()
            try:
                DataRequestSolvedView(many=True).load(data_requests_solved)
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for data requests solved for {arg_address}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for data requests solved data for {arg_address}.",
                )
            pagination_parameters.item_count = len(data_requests_solved)
            return data_requests_solved[start:stop]
