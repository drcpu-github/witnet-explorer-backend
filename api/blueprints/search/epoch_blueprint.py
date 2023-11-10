import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from node.consensus_constants import ConsensusConstants
from objects.block import Block
from schemas.misc.abort_schema import AbortSchema
from schemas.search.epoch_schema import SearchEpochArgs, SearchEpochResponse

search_epoch_blueprint = Blueprint(
    "search epoch",
    "search epoch",
    description="Lookup the block for the specified epoch.",
)


@search_epoch_blueprint.route("/epoch")
class SearchEpoch(MethodView):
    @search_epoch_blueprint.arguments(SearchEpochArgs, location="query")
    @search_epoch_blueprint.response(
        200,
        SearchEpochResponse,
        description="Returns the block associated with the requested epoch.",
    )
    @search_epoch_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Block for epoch {epoch} not found.",
                "Incorrect message format for block {epoch}.",
            ]
        },
    )
    def get(self, args):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        epoch = args["value"]
        logger.info(f"search_epoch({epoch})")

        # Epoch cached items return block hash
        cached_block_hash = cache.get(f"{epoch}")
        if cached_block_hash:
            # Fetch the actual block based on the block hash
            cached_block = cache.get(cached_block_hash)
            if cached_block:
                logger.info(
                    f"Found block {epoch} with hash {cached_block_hash} in memcached cache"
                )
                return cached_block

        # Create consensus constants
        consensus_constants = ConsensusConstants(
            database=database,
            witnet_node=witnet_node,
        )

        # Fetch block from a node
        block = Block(
            consensus_constants,
            block_epoch=epoch,
            logger=logger,
            database=database,
            witnet_node=witnet_node,
        )
        # Process and validate block for API
        try:
            block_json = block.process_block("api")
        except ValidationError as err_info:
            logger.error(f"Incorrect message format for block {epoch}: {err_info}")
            abort(
                404,
                message=f"Incorrect message format for block {epoch}.",
            )

        if "error" in block_json:
            logger.error(f"Block for epoch {epoch} not found: {block_json['error']}")
            abort(
                404,
                message=f"Block for epoch {epoch} not found.",
            )

        # Attempt to cache the block
        block_hash = block_json["details"]["hash"]
        if block_json["details"]["confirmed"]:
            try:
                # First, cache the actual block with the hash as key
                try:
                    cache.set(
                        block_hash,
                        SearchEpochResponse().load(
                            {
                                "response_type": "block",
                                "block": block_json,
                            }
                        ),
                        timeout=config["api"]["caching"]["scripts"]["blocks"][
                            "timeout"
                        ],
                    )
                except ValidationError as err_info:
                    logger.error(
                        f"Incorrect message format for block {epoch}: {err_info}"
                    )
                    abort(
                        404,
                        message=f"Incorrect message format for block {epoch}.",
                    )
                # Second, cache the block hash with the block epoch as key
                cache.set(
                    str(epoch),
                    block_hash,
                    timeout=config["api"]["caching"]["scripts"]["blocks"]["timeout"],
                )
                logger.info(
                    f"Added block {epoch} with hash {block_hash} to the memcached cache"
                )
            except pylibmc.TooBig:
                logger.warning(
                    f"Could not save block {epoch} with hash {block_hash} in the memcached instance because its size exceeded 1MB"
                )
        else:
            logger.info(
                f"Did not add unconfirmed block {epoch} with hash {block_hash} to the memcached cache"
            )

        try:
            return SearchEpochResponse().load(
                {
                    "response_type": "block",
                    "block": block_json,
                }
            )
        except ValidationError as err_info:
            logger.error(f"Incorrect message format for block {epoch}: {err_info}")
            abort(
                404,
                message=f"Incorrect message format for block {epoch}.",
            )
