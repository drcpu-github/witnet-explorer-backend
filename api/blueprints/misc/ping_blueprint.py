from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.ping_schema import PingResponse
from schemas.misc.version_schema import VersionSchema

ping_blueprint = Blueprint(
    "ping",
    "ping",
    description="Fetch the status of the API.",
)


@ping_blueprint.route("/ping")
class Ping(MethodView):
    @ping_blueprint.response(
        200,
        PingResponse,
        description="Returns a pong response to indicate the API is online.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @ping_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for ping response.",
            ]
        },
    )
    def get(self):
        logger = current_app.extensions["logger"]
        logger.info("ping()")
        try:
            return (
                PingResponse().load({"response": "pong"}),
                200,
                {"X-Version": "v1.0.0"},
            )
        except ValidationError as err_info:
            logger.error(f"Incorrect message format for ping response: {err_info}")
            abort(404, message="Incorrect message format for ping response.")
