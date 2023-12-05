import pylibmc
from flask import current_app
from flask.views import MethodView
from flask_smorest import Blueprint, abort
from marshmallow import ValidationError

from blockchain.objects.block import Block
from blockchain.objects.data_request_history import DataRequestHistory
from blockchain.objects.data_request_report import DataRequestReport
from blockchain.transactions.commit import Commit
from blockchain.transactions.data_request import DataRequest
from blockchain.transactions.mint import Mint
from blockchain.transactions.reveal import Reveal
from blockchain.transactions.tally import Tally
from blockchain.transactions.value_transfer import ValueTransfer
from node.consensus_constants import ConsensusConstants
from schemas.misc.abort_schema import AbortSchema
from schemas.misc.version_schema import VersionSchema
from schemas.search.hash_schema import SearchHashArgs, SearchHashResponse

search_hash_blueprint = Blueprint(
    "search hash",
    "search hash",
    description="Lookup a block, transaction, RAD or DRO hash",
)


@search_hash_blueprint.route("/hash")
class SearchHash(MethodView):
    @search_hash_blueprint.arguments(SearchHashArgs, location="query")
    @search_hash_blueprint.response(
        200,
        SearchHashResponse,
        description="Returns the block, transaction, data request report, RAD or DRO overview associated with the requested hash",
        headers={
            "X-Version": {
                "description": "Version of this API endpoint.",
                "schema": VersionSchema,
            }
        },
    )
    @search_hash_blueprint.alt_response(
        404,
        schema=AbortSchema,
        description="List of possible abort errors.",
        example={
            "message": [
                "Could not fetch the pending transactions.",
                "Could not find transaction hash.",
                "Incorrect message format for block {hash_value}.",
                "Incorrect message format for mint transaction {hash_value}.",
                "Incorrect message format for value transfer transaction {hash_value}.",
                "Incorrect message format for data request {hash_value}.",
                "Incorrect message format for commit transaction {hash_value}.",
                "Incorrect message format for reveal transaction {hash_value}.",
                "Incorrect message format for tally transaction {hash_value}.",
                "Incorrect message format for data request report {data_request_hash}.",
                "Incorrect message format for data request history: {hash_value}.",
            ]
        },
    )
    @search_hash_blueprint.paginate(page_size=50, max_page_size=1000)
    def get(self, args, pagination_parameters):
        cache = current_app.extensions["cache"]
        config = current_app.config["explorer"]
        database = current_app.extensions["database"]
        logger = current_app.extensions["logger"]
        witnet_node = current_app.extensions["witnet_node"]

        hash_value = args["value"]
        simple = args["simple"]
        logger.info(f"search_hash({hash_value}, {simple})")

        # Set default item count for pagination header (only used for data request histories)
        pagination_parameters.item_count = 1

        found = False
        hashed_item = cache.get(hash_value)
        if hashed_item:
            logger.info(
                f"Found {hashed_item['response_type'].replace('_', ' ')} hash {hash_value} in memcached cache"
            )
            # Data request reports are cached on the data request hash (and thus indistinguishable from data requests)
            # If simple is used in conjuction with a data request hash, only return the data request data
            if simple and hashed_item["response_type"] == "data_request_report":
                data_request = hashed_item["data_request_report"]["data_request"]
                data_request["transaction_type"] = "data_request"
                hashed_item = {
                    "response_type": "data_request",
                    "data_request": data_request,
                }
            # The commit, reveal or tally hash was found in the memcached cache, but the request was for the full data request report
            if not simple and hashed_item["response_type"] in (
                "commit",
                "reveal",
                "tally",
            ):
                found = True
            else:
                return hashed_item, 200, {"X-Version": "v1.0.0"}

        sql = """
            SELECT
                type
            FROM
                hashes
            WHERE
                hash=%s
        """
        result = database.sql_return_one(
            sql, parameters=[bytearray.fromhex(hash_value)]
        )
        if result:
            hash_type = result[0]
        # Check if the transaction is in the mempool and pending block inclusion
        else:
            transactions_pool = witnet_node.get_mempool()

            if "error" in transactions_pool:
                logger.error(
                    f"Could not fetch the pending transactions: {transactions_pool['error']}"
                )
                abort(404, message="Could not fetch the pending transactions.")

            transactions_pool = transactions_pool["result"]
            if hash_value in transactions_pool["data_request"]:
                return (
                    SearchHashResponse().load(
                        {
                            "response_type": "pending",
                            "pending": "Data request is pending.",
                        }
                    ),
                    200,
                    {"X-Version": "v1.0.0"},
                )
            elif hash_value in transactions_pool["value_transfer"]:
                return (
                    SearchHashResponse().load(
                        {
                            "response_type": "pending",
                            "pending": "Value transfer is pending.",
                        }
                    ),
                    200,
                    {"X-Version": "v1.0.0"},
                )
            else:
                logger.warning(f"Could not find transaction hash {hash_value}")
                abort(404, message=f"Could not find transaction hash {hash_value}.")

        if found:
            logger.info(
                f"Found {hash_type.replace('_', ' ')} {hash_value} in memcached cache, but the full data request report was requested"
            )
        else:
            logger.info(
                f"Could not find {hash_type.replace('_', ' ')} {hash_value} in memcached cache"
            )

        cache_config = config["api"]["caching"]

        consensus_constants = ConsensusConstants(
            database=database,
            witnet_node=witnet_node,
        )

        if hash_type == "block":
            # Fetch block from a node
            block = Block(
                consensus_constants,
                block_hash=hash_value,
                logger=logger,
                database=database,
                witnet_node=witnet_node,
            )
            # Fetch JSON representation of the block which has already been validated
            try:
                block_json = block.process_block("api")
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for block {hash_value}: {err_info}"
                )
                abort(404, message=f"Incorrect message format for block {hash_value}.")
            if block_json["details"]["confirmed"]:
                try:
                    # First cache the block with its hash as a key
                    cache.set(
                        hash_value,
                        SearchHashResponse().load(
                            {
                                "response_type": "block",
                                "block": block_json,
                            }
                        ),
                        timeout=cache_config["scripts"]["blocks"]["timeout"],
                    )
                    # Second cache the block hash with the block epoch as key
                    block_epoch = block_json["details"]["epoch"]
                    cache.set(
                        str(block_epoch),
                        hash_value,
                        timeout=cache_config["scripts"]["blocks"]["timeout"],
                    )
                    logger.info(f"Added block {hash_value} to our memcached instance")
                except ValidationError as err_info:
                    logger.error(
                        f"Incorrect message format for block {hash_value}: {err_info}"
                    )
                    abort(
                        404,
                        message=f"Incorrect message format for block {hash_value}.",
                    )
                except pylibmc.TooBig:
                    logger.warning(
                        f"Could not save block {hash_value} in our memcached instance because its size exceeded 1MB"
                    )
            else:
                logger.info(
                    f"Did not add unconfirmed block {hash_value} to our memcached instance"
                )
            try:
                return (
                    SearchHashResponse().load(
                        {
                            "response_type": "block",
                            "block": block_json,
                        }
                    ),
                    200,
                    {"X-Version": "v1.0.0"},
                )
            except ValidationError as err_info:
                logger.error(f"Incorrect message format for block: {err_info}")
                abort(
                    404,
                    message=f"Incorrect message format for block {hash_value}.",
                )

        # Create mint transaction and get the details from the database
        if hash_type == "mint_txn":
            mint = Mint(consensus_constants, logger=logger, database=database)
            try:
                mint_txn = mint.get_transaction_from_database(hash_value)
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for mint transaction {hash_value}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for mint transaction {hash_value}.",
                )
            if mint_txn["confirmed"]:
                logger.info(
                    f"Added mint transaction {hash_value} to our memcached instance"
                )
                try:
                    cache.set(
                        hash_value,
                        SearchHashResponse().load(
                            {
                                "response_type": "mint",
                                "mint": mint_txn,
                            }
                        ),
                        timeout=cache_config["views"]["hash"]["timeout"],
                    )
                except ValidationError as err_info:
                    logger.error(
                        f"Incorrect message format for mint transaction {hash_value}: {err_info}"
                    )
                    abort(
                        404,
                        message=f"Incorrect message format for mint transaction {hash_value}.",
                    )
            else:
                logger.info(
                    f"Did not add unconfirmed mint transaction {hash_value} to our memcached instance"
                )
            try:
                return (
                    SearchHashResponse().load(
                        {"response_type": "mint", "mint": mint_txn}
                    ),
                    200,
                    {"X-Version": "v1.0.0"},
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for mint transaction {hash_value}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for mint transaction {hash_value}.",
                )

        # Create value transfer transaction and get the details from the database
        if hash_type == "value_transfer_txn":
            value_transfer = ValueTransfer(
                consensus_constants,
                logger=logger,
                database=database,
            )
            try:
                value_transfer_txn = value_transfer.get_transaction_from_database(
                    hash_value
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for value transfer transaction {hash_value}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for value transfer transaction {hash_value}.",
                )
            if value_transfer_txn["confirmed"]:
                logger.info(
                    f"Added value transfer transaction {hash_value} to our memcached instance"
                )
                try:
                    cache.set(
                        hash_value,
                        SearchHashResponse().load(
                            {
                                "response_type": "value_transfer",
                                "value_transfer": value_transfer_txn,
                            }
                        ),
                        timeout=cache_config["views"]["hash"]["timeout"],
                    )
                except ValidationError as err_info:
                    logger.error(
                        f"Incorrect message format for value transfer transaction {hash_value}: {err_info}"
                    )
                    abort(
                        404,
                        message=f"Incorrect message format for value transfer transaction {hash_value}.",
                    )
            else:
                logger.info(
                    f"Did not add unconfirmed value transfer transaction {hash_value} to our memcached instance"
                )
            try:
                return (
                    SearchHashResponse().load(
                        {
                            "response_type": "value_transfer",
                            "value_transfer": value_transfer_txn,
                        }
                    ),
                    200,
                    {"X-Version": "v1.0.0"},
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for value transfer transaction {hash_value}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for value transfer transaction {hash_value}.",
                )

        if hash_type in ("data_request_txn", "commit_txn", "reveal_txn", "tally_txn"):
            # Only return a single transaction, don't build a DataRequestReport
            if simple:
                if hash_type == "data_request_txn":
                    data_request = DataRequest(
                        consensus_constants,
                        logger=logger,
                        database=database,
                        witnet_node=witnet_node,
                    )
                    try:
                        transaction = data_request.get_transaction_from_database(
                            hash_value
                        )
                    except ValidationError as err_info:
                        logger.error(
                            f"Incorrect message format for data request {hash_value}: {err_info}"
                        )
                        abort(
                            404,
                            message=f"Incorrect message format for data request {hash_value}.",
                        )
                    try:
                        # Do not cache a the result of a query for a single data request transaction
                        # This would conflict with the data request report which uses the same hash_value as key
                        return (
                            SearchHashResponse().load(
                                {
                                    "response_type": "data_request",
                                    "data_request": transaction,
                                }
                            ),
                            200,
                            {"X-Version": "v1.0.0"},
                        )
                    except ValidationError:
                        abort(
                            404,
                            message=f"Incorrect message format for data request {hash_value}.",
                        )
                elif hash_type == "commit_txn":
                    commit = Commit(
                        consensus_constants,
                        logger=logger,
                        database=database,
                        witnet_node=witnet_node,
                    )
                    try:
                        transaction = commit.get_transaction_from_database(hash_value)
                    except ValidationError as err_info:
                        logger.error(
                            f"Incorrect message format for commit transaction {hash_value}: {err_info}"
                        )
                        abort(
                            404,
                            message=f"Incorrect message format for commit transaction {hash_value}.",
                        )
                    try:
                        cache.set(
                            hash_value,
                            SearchHashResponse().load(
                                {
                                    "response_type": "commit",
                                    "commit": transaction,
                                }
                            ),
                            timeout=cache_config["views"]["hash"]["timeout"],
                        )
                        return (
                            {"response_type": "commit", "commit": transaction},
                            200,
                            {"X-Version": "v1.0.0"},
                        )
                    except ValidationError:
                        abort(
                            404,
                            message=f"Incorrect message format for commit transaction {hash_value}.",
                        )
                elif hash_type == "reveal_txn":
                    reveal = Reveal(
                        consensus_constants,
                        logger=logger,
                        database=database,
                        witnet_node=witnet_node,
                    )
                    try:
                        transaction = reveal.get_transaction_from_database(hash_value)
                    except ValidationError as err_info:
                        logger.error(
                            f"Incorrect message format for reveal transaction {hash_value}: {err_info}"
                        )
                        abort(
                            404,
                            message=f"Incorrect message format for reveal transaction {hash_value}.",
                        )
                    try:
                        cache.set(
                            hash_value,
                            SearchHashResponse().load(
                                {
                                    "response_type": "reveal",
                                    "reveal": transaction,
                                }
                            ),
                            timeout=cache_config["views"]["hash"]["timeout"],
                        )
                        return (
                            {"response_type": "reveal", "reveal": transaction},
                            200,
                            {"X-Version": "v1.0.0"},
                        )
                    except ValidationError:
                        abort(
                            404,
                            message=f"Incorrect message format for reveal transaction {hash_value}.",
                        )
                elif hash_type == "tally_txn":
                    tally = Tally(
                        consensus_constants,
                        logger=logger,
                        database=database,
                        witnet_node=witnet_node,
                    )
                    try:
                        transaction = tally.get_transaction_from_database(hash_value)
                    except ValidationError as err_info:
                        logger.error(
                            f"Incorrect message format for tally transaction {hash_value}: {err_info}"
                        )
                        abort(
                            404,
                            message=f"Incorrect message format for tally transaction {hash_value}.",
                        )
                    try:
                        cache.set(
                            hash_value,
                            SearchHashResponse().load(
                                {
                                    "response_type": "tally",
                                    "tally": transaction,
                                }
                            ),
                            timeout=cache_config["views"]["hash"]["timeout"],
                        )
                        return (
                            {"response_type": "tally", "tally": transaction},
                            200,
                            {"X-Version": "v1.0.0"},
                        )
                    except ValidationError:
                        abort(
                            404,
                            message=f"Incorrect message format for tally transaction {hash_value}.",
                        )
            # Create data request report for this hash
            else:
                data_request_report = DataRequestReport(
                    hash_type[:-4],
                    hash_value,
                    consensus_constants,
                    logger=logger,
                    database=database,
                )

                # If the hash type is a commit, reveal or tally transaction, get the matching data request hash
                # Cached data request reports for a data request hash lookup will already have been returned
                if hash_type in ("commit_txn", "reveal_txn", "tally_txn"):
                    data_request_hash = data_request_report.get_data_request_hash()
                else:
                    data_request_hash = hash_value
                cached_data_request_report = cache.get(data_request_hash)

                if cached_data_request_report:
                    # Replace the transaction type with the correct one since previous searches may have cached it using another type
                    cached_data_request_report["data_request_report"][
                        "transaction_type"
                    ] = hash_type[:-4]
                    logger.info(
                        f"Found a data request report {data_request_hash} for a {hash_type.replace('_', ' ')} in our memcached instance"
                    )
                    return cached_data_request_report, 200, {"X-Version": "v1.0.0"}

                try:
                    data_request_report_json = data_request_report.get_report()
                except ValidationError as err_info:
                    logger.error(
                        f"Incorrect message format for data request report {data_request_hash}: {err_info}"
                    )
                    abort(
                        404,
                        message=f"Incorrect message format for data request report {data_request_hash}.",
                    )

                # From the API: only cache data request reports with a confirmed tally transaction
                if (
                    data_request_report_json["tally"]
                    and data_request_report_json["tally"]["confirmed"]
                ):
                    logger.info(
                        f"Added data request report {data_request_hash} to our memcached instance"
                    )
                    # Cache data request report based on the data request hash
                    try:
                        cache.set(
                            data_request_hash,
                            SearchHashResponse().load(
                                {
                                    "response_type": "data_request_report",
                                    "data_request_report": data_request_report_json,
                                }
                            ),
                            timeout=cache_config["scripts"]["data_request_reports"][
                                "timeout"
                            ],
                        )
                    except ValidationError as err_info:
                        logger.error(
                            f"Incorrect message format for data request report {data_request_hash}: {err_info}"
                        )
                        abort(
                            404,
                            message=f"Incorrect message format for data request report {data_request_hash}.",
                        )
                else:
                    if "tally" not in data_request_report_json:
                        logger.info(
                            f"Did not add unconfirmed data request report {data_request_hash} to our memcached instance"
                        )
                    else:
                        logger.info(
                            f"Did not add unconfirmed data request report {data_request_hash} to our memcached instance"
                        )

                try:
                    return (
                        SearchHashResponse().load(
                            {
                                "response_type": "data_request_report",
                                "data_request_report": data_request_report_json,
                            }
                        ),
                        200,
                        {"X-Version": "v1.0.0"},
                    )
                except ValidationError as err_info:
                    logger.error(
                        f"Incorrect message format for data request report {data_request_hash}: {err_info}"
                    )
                    abort(
                        404,
                        message=f"Incorrect message format for data request report {data_request_hash}.",
                    )

        if hash_type in ("DRO_bytes_hash", "RAD_bytes_hash"):
            # Create data request history
            data_request_history = DataRequestHistory(
                consensus_constants,
                logger,
                database,
            )
            # Data request histories change constantly, so we should not employ simple hash-based caching only
            try:
                start = (
                    pagination_parameters.page - 1
                ) * pagination_parameters.page_size
                count, history = data_request_history.get_history(
                    hash_type,
                    hash_value,
                    pagination_parameters.page_size,
                    start,
                )
                pagination_parameters.item_count = count
                return (
                    SearchHashResponse().load(
                        {
                            "response_type": "data_request_history",
                            "data_request_history": history,
                        }
                    ),
                    200,
                    {"X-Version": "v1.0.0"},
                )
            except ValidationError as err_info:
                logger.error(
                    f"Incorrect message format for data request history {hash_value}: {err_info}"
                )
                abort(
                    404,
                    message=f"Incorrect message format for data request history: {hash_value}.",
                )
