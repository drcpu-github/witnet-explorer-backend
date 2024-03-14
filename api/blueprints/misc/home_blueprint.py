from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.home_schema import HomeArgs, HomeResponse
from schemas.misc.version_schema import VersionSchema

home_blueprint = Blueprint(
    "Home",
    "home",
    description="Fetch the data required to build the Witnet explorer homepage.",
)


@home_blueprint.route("/home")
class Home(MethodView):
    @home_blueprint.arguments(HomeArgs, location="query")
    @home_blueprint.response(
        200,
        HomeResponse,
        description="Returns recent network statistics used to build the homepage.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @home_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not find home in memcached cache.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        logger = current_app.extensions["logger"]

        key = args["key"]
        logger.info(f"home({key})")

        home = cache.get("home")
        if not home:
            logger.error("Could not find home in memcached cache")
            abort(
                404,
                message="Could not find homepage data in the cache.",
                headers={"X-Version": "1.0.0"},
            )
        else:
            logger.info("Found home in memcached cache")

        if key == "full":
            return home, 200, {"X-Version": "1.0.0"}
        elif key in ("network_stats", "supply_info"):
            return {key: home[key]}, 200, {"X-Version": "1.0.0"}
        elif key in ("blocks", "data_requests", "value_transfers"):
            return (
                {f"latest_{key}": home[f"latest_{key}"]},
                200,
                {"X-Version": "1.0.0"},
            )
