import base64
import io
import os

from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError
from PIL import Image

from schemas.misc.abort_schema import AbortSchema
from schemas.network.tapi_schema import NetworkTapiArgs, NetworkTapiResponse

network_tapi_blueprint = Blueprint(
    "network tapi",
    "network tapi",
    description="Fetch network TAPI's.",
)


@network_tapi_blueprint.route("/tapi")
class NetworkTapi(MethodView):
    @network_tapi_blueprint.arguments(NetworkTapiArgs, location="query")
    @network_tapi_blueprint.response(
        200,
        NetworkTapiResponse(many=True),
        description="Returns a list of TAPIs.",
    )
    @network_tapi_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not fetch tapi data.",
                "Incorrect message format for TAPI data.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]

        logger.info(f"network_tapi({args['return_all']})")

        # Fetching known TAPI ids
        sql = """
            SELECT
                id
            FROM
                wips
            WHERE
                tapi_bit IS NOT NULL
            ORDER BY
                id
            ASC
        """
        tapis_cached = database.sql_return_all(sql)

        all_tapis = []
        if tapis_cached:
            for counter in tapis_cached:
                # Fetch TAPI details from cache
                counter = counter[0]
                tapi = cache.get(f"tapi-{counter}")
                if tapi is None:
                    logger.info(
                        f"Could not find tapi-{counter} in our memcached instance, fetching it from the database"
                    )
                    sql = """
                        SELECT
                            tapi_json
                        FROM
                            wips
                        WHERE
                            id=%s
                    """
                    (tapi,) = database.sql_return_one(sql, parameters=[counter])
                    if tapi is None:
                        logger.error(f"Could not fetch tapi-{counter}")
                        abort(404, message="Could not fetch tapi data.")
                        continue

                # Skip unactivated TAPIs unless requested otherwise
                if not args["return_all"] and not tapi["activated"]:
                    logger.debug(f"Not sending tapi-{counter} as it was not activated")
                    continue

                # Serialize TAPI plot to bytes (if found)
                tapi_plot = os.path.join(
                    config["api"]["caching"]["plot_directory"], f"tapi-{counter}.png"
                )
                if os.path.exists(tapi_plot):
                    plot = Image.open(tapi_plot, mode="r")
                    byte_arr = io.BytesIO()
                    plot.save(byte_arr, format="PNG")
                    encoded_plot = base64.encodebytes(byte_arr.getvalue())
                    tapi["plot"] = encoded_plot.decode("ascii")
                elif tapi["current_epoch"] < tapi["start_epoch"]:
                    tapi["plot"] = "The TAPI did not start yet"
                else:
                    tapi["plot"] = "Could not find TAPI plot"

                all_tapis.append(tapi)

        if all_tapis == []:
            logger.info("No TAPI's found in memcached cache")

        try:
            NetworkTapiResponse(many=True).load(all_tapis)
        except ValidationError as err_info:
            logger.error(f"Incorrect message format for TAPI data: {err_info}")
            abort(404, message="Incorrect message format for TAPI data.")

        logger.info(
            f"Returning TAPI's {', '.join(str(tapi['tapi_id']) for tapi in all_tapis)}"
        )

        return all_tapis
