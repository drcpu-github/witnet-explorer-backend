from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.network.balances_schema import NetworkBalancesResponse

network_balances_blueprint = Blueprint(
    "balances",
    "balances",
    description="Fetch a list of addresses and their balance.",
)


@network_balances_blueprint.route("/balances")
class BalanceList(MethodView):
    @network_balances_blueprint.response(
        200,
        NetworkBalancesResponse,
        description="Returns a paginated list of addresses, their balance and a label.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @network_balances_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not find required list of balances in memcached cache.",
            ]
        },
    )
    @network_balances_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, pagination_parameters):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]

        logger.info(
            f"network_balances({pagination_parameters.page}, {pagination_parameters.page_size})"
        )

        # This is the hardcoded size of the number of addresses saved per entry in our memcached instance
        items_per_cache_entry = 1000

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size - 1
        list_start = int(start / items_per_cache_entry) * items_per_cache_entry
        list_stop = int(start / items_per_cache_entry + 1) * items_per_cache_entry
        key = f"balance-list_{list_start}-{list_stop}"

        balance_list_part = cache.get(key)

        if not balance_list_part:
            pagination_parameters.item_count = 0
            logger.error("Could not find required list of balances in memcached cache")
            abort(
                404,
                message="Could not find required list of balances in memcached cache.",
            )
        else:
            logger.info(
                f"Found {key} to build page {pagination_parameters.page} ({start} - {stop}) in memcached cache"
            )

        pagination_parameters.item_count = balance_list_part["total_items"]

        paginated_balances = [
            balance
            for idx, balance in enumerate(balance_list_part["balances"])
            if idx >= start % items_per_cache_entry
            and idx <= stop % items_per_cache_entry
        ]

        return (
            NetworkBalancesResponse().load(
                {
                    "balances": paginated_balances,
                    "total_items": balance_list_part["total_items"],
                    "total_balance_sum": balance_list_part["total_balance_sum"],
                    "last_updated": balance_list_part["last_updated"],
                }
            ),
            200,
            {"X-Version": "v1.0.0"},
        )
