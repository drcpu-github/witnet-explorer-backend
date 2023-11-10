from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from node.consensus_constants import ConsensusConstants
from schemas.misc.abort_schema import AbortSchema
from schemas.network.blockchain_schema import NetworkBlockchainResponse
from util.common_functions import (
    calculate_block_reward,
    calculate_current_epoch,
    calculate_timestamp_from_epoch,
)
from util.common_sql import sql_last_block

network_blockchain_blueprint = Blueprint(
    "network blockchain",
    "network blockchain",
    description="Fetch the Witnet blockchain.",
)


@network_blockchain_blueprint.route("/blockchain")
class NetworkBlockchain(MethodView):
    @network_blockchain_blueprint.response(
        200,
        NetworkBlockchainResponse,
        description="Returns a paginated overview of the Witnet blockchain.",
    )
    @network_blockchain_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Incorrect message format for blockchain response.",
            ]
        },
    )
    @network_blockchain_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, pagination_parameters):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        logger.info(
            f"network_blockchain({pagination_parameters.page}, {pagination_parameters.page_size})"
        )

        cache_key = f"blockchain_page-{pagination_parameters.page}_page-size-{pagination_parameters.page_size}"
        blockchain = cache.get(cache_key)
        if blockchain:
            logger.info(f"Found {cache_key} in memcached cache")
            pagination_parameters.item_count = blockchain["total_epochs"]
            return blockchain

        logger.info(f"Could not find {cache_key} in memcached cache")

        # Get the expected epoch
        consensus_constants = ConsensusConstants(
            database=database,
            witnet_node=witnet_node,
        )
        expected_epoch = calculate_current_epoch(
            consensus_constants.checkpoint_zero_timestamp,
            consensus_constants.checkpoints_period,
        )

        # Get the last processed epoch
        data = database.sql_return_one(sql_last_block)
        if data:
            last_epoch = data[1]
        else:
            last_epoch = -1
        pagination_parameters.item_count = last_epoch
        if expected_epoch - 2 > last_epoch:
            logger.info(
                f"Expected epoch is {expected_epoch}, but last seen epoch is {last_epoch}"
            )
        else:
            logger.info(f"Last seen epoch is {last_epoch}")

        start = (pagination_parameters.page - 1) * pagination_parameters.page_size
        stop = pagination_parameters.page * pagination_parameters.page_size - 1
        blockchain = get_blockchain_details(
            database,
            last_epoch,
            start,
            stop,
            consensus_constants,
        )

        # Validate data before we save it in the cache
        try:
            blockchain = NetworkBlockchainResponse().load(blockchain)
            cache.set(
                cache_key,
                blockchain,
                timeout=config["api"]["caching"]["views"]["blockchain"]["timeout"],
            )
        except ValidationError as err_info:
            logger.error(
                f"Incorrect message format for blockchain response: {err_info}"
            )
            abort(404, message="Incorrect message format for blockchain response.")

        return blockchain


def get_blockchain_details(database, last_epoch, start, stop, consensus_constants):
    sql = """
        SELECT
            blocks.block_hash,
            blocks.epoch,
            blocks.value_transfer,
            blocks.data_request,
            blocks.commit,
            blocks.reveal,
            blocks.tally,
            blocks.confirmed,
            blocks.reverted,
            mint_txns.miner,
            mint_txns.output_values
        FROM
            blocks
        LEFT JOIN
            mint_txns
        ON
            blocks.epoch=mint_txns.epoch
        WHERE
            blocks.epoch
        BETWEEN
            %s AND %s
        ORDER BY
            blocks.epoch
        DESC
    """
    blocks = database.sql_return_all(sql, [last_epoch - stop, last_epoch - start])

    blockchain = []
    for block in blocks:
        # Reverted block
        if block[8]:
            continue

        timestamp = calculate_timestamp_from_epoch(
            consensus_constants.checkpoint_zero_timestamp,
            consensus_constants.checkpoints_period,
            block[1],
        )
        block_reward = calculate_block_reward(
            block[1],
            consensus_constants.halving_period,
            consensus_constants.initial_block_reward,
        )
        blockchain.append(
            {
                "hash": block[0].hex(),
                "epoch": block[1],
                "timestamp": timestamp,
                "value_transfers": block[2],
                "data_requests": block[3],
                "commits": block[4],
                "reveals": block[5],
                "tallies": block[6],
                "confirmed": block[7],
                "miner": block[9],
                "fees": sum(block[10]) - block_reward,
            }
        )

    reverted_blocks = []
    sorted_epochs = sorted([block["epoch"] for block in blockchain])
    for epoch in range(last_epoch - stop, last_epoch - start + 1):
        if epoch not in sorted_epochs:
            reverted_blocks.append(epoch)

    return {
        "blockchain": blockchain,
        "reverted": reverted_blocks,
        "total_epochs": last_epoch,
    }
