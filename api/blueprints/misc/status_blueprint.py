from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from schemas.misc.abort_schema import AbortSchema
from schemas.misc.status_schema import StatusResponse
from schemas.misc.version_schema import VersionSchema
from util.common_functions import calculate_current_epoch, get_network_times
from util.common_sql import sql_last_block, sql_last_confirmed_block

status_blueprint = Blueprint(
    "status",
    "status",
    description="Fetch status of different backend components of the explorer.",
)


@status_blueprint.route("/status")
class Status(MethodView):
    @status_blueprint.response(
        200,
        StatusResponse,
        description="Returns the status of different backend components.",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @status_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for status.",
            ]
        },
    )
    def get(self):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        logger.info("status()")

        status = cache.get("status")
        if not status:
            logger.info("Could not find status in memcached cache")

            all_healthy = True

            # Calculate what the expected epoch should be
            start_time, epoch_period = get_network_times(database)
            expected_epoch = calculate_current_epoch(start_time, epoch_period)

            # Fetch the node pool status
            node_pool_status = witnet_node.get_sync_status()
            if "error" in node_pool_status:
                node_pool_message = {"message": node_pool_status["error"]}
                all_healthy = False
            else:
                if node_pool_status["result"]["node_state"] != "Synced":
                    all_healthy = False
                node_pool_message = {
                    "epoch": node_pool_status["result"]["current_epoch"],
                    "status": node_pool_status["result"]["node_state"],
                    "message": "fetched node pool status correctly",
                }

            # Get the last confirmed and unconfirmed block from the database
            database_message = "database processes seem healthy"
            data = database.sql_return_one(sql_last_confirmed_block)
            confirmed_block_hash = data[0].hex()
            confirmed_epoch = int(data[1])
            data = database.sql_return_one(sql_last_block)
            unconfirmed_block_hash = data[0].hex()
            unconfirmed_epoch = int(data[1])

            # Check if the last confirmed epoch was the block before the previous superepoch
            sql = """
                SELECT
                    int_val
                FROM
                    consensus_constants
                WHERE
                    KEY = 'superblock_period'
            """
            superblock_period = database.sql_return_one(sql)
            if superblock_period:
                expected_confirmed_epoch = (
                    int(unconfirmed_epoch / superblock_period[0]) * superblock_period[0]
                    - superblock_period[0]
                    - 1
                )
                if confirmed_epoch < expected_confirmed_epoch:
                    database_message = (
                        "The network has probably rolled back a superepoch"
                    )

            # More than 100 unconfirmed blocks have elapsed, maybe the database process crashed
            if expected_epoch > confirmed_epoch + 100:
                database_message = "database processes have probably crashed"
                all_healthy = False

            # We did not (yet) insert a block for the previous epoch, did the explorer crash?
            if expected_epoch - 2 > unconfirmed_epoch:
                database_message = "database processes have probably crashed"
                all_healthy = False

            if all_healthy:
                health_message = "all backend services are up and running"
            else:
                health_message = "some backend services are down"

            status = {
                "message": health_message,
                "node_pool_message": node_pool_message,
                "database_confirmed": {
                    "hash": confirmed_block_hash,
                    "epoch": confirmed_epoch,
                },
                "database_unconfirmed": {
                    "hash": unconfirmed_block_hash,
                    "epoch": unconfirmed_epoch,
                },
                "database_message": database_message,
                "expected_epoch": expected_epoch,
            }

            try:
                cache.set(
                    "status",
                    StatusResponse().load(status),
                    timeout=config["api"]["caching"]["views"]["status"]["timeout"],
                )
            except ValidationError as err_info:
                logger.error(f"Incorrect message format for status: {err_info}")
                abort(404, message="Incorrect message format for status.")
        else:
            logger.info("Found status in memcached cache")

        return status, 200, {"X-Version": "1.0.0"}
