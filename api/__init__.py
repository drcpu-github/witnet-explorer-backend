import toml
from flask import Flask
from flask_smorest import Api, Blueprint

from api.blueprints.address.blocks_blueprint import address_blocks_blueprint
from api.blueprints.address.data_requests_created_blueprint import (
    address_data_requests_created_blueprint,
)
from api.blueprints.address.data_requests_solved_blueprint import (
    address_data_requests_solved_blueprint,
)
from api.blueprints.address.details_blueprint import address_details_blueprint
from api.blueprints.address.info_blueprint import address_info_blueprint
from api.blueprints.address.labels_blueprint import address_labels_blueprint
from api.blueprints.address.mints_blueprint import address_mints_blueprint
from api.blueprints.address.utxos_blueprint import address_utxos_blueprint
from api.blueprints.address.value_transfers_blueprint import (
    address_value_transfers_blueprint,
)
from api.blueprints.misc.home_blueprint import home_blueprint
from api.blueprints.misc.ping_blueprint import ping_blueprint
from api.blueprints.misc.status_blueprint import status_blueprint
from api.blueprints.network.balances_blueprint import network_balances_blueprint
from api.blueprints.network.blockchain_blueprint import network_blockchain_blueprint
from api.blueprints.network.mempool_blueprint import network_mempool_blueprint
from api.blueprints.network.reputation_blueprint import network_reputation_blueprint
from api.blueprints.network.statistics_blueprint import network_statistics_blueprint
from api.blueprints.network.supply_blueprint import network_supply_blueprint
from api.blueprints.network.tapi_blueprint import network_tapi_blueprint
from api.blueprints.search.epoch_blueprint import search_epoch_blueprint
from api.blueprints.search.hash_blueprint import search_hash_blueprint
from api.blueprints.transaction.mempool_blueprint import transaction_mempool_blueprint
from api.blueprints.transaction.priority_blueprint import transaction_priority_blueprint
from api.blueprints.transaction.send_blueprint import transaction_send_blueprint
from api.connect import (
    create_address_caching_server,
    create_cache,
    create_database,
    create_witnet_node,
)
from api.gunicorn_config import toml_config
from mockups.config import mock_config
from util.logger import configure_logger


def create_app(mock=False):
    # Create app
    app = Flask(__name__)

    # Configure app
    app.config["API_TITLE"] = "Witnet explorer REST API"
    app.config["API_VERSION"] = "v1.0.0"

    # Set configurations for OpenAPI documentation
    app.config["OPENAPI_VERSION"] = "3.0.3"
    app.config["OPENAPI_URL_PREFIX"] = "/api"
    app.config["OPENAPI_RAPIDOC_PATH"] = "/documentation"
    app.config[
        "OPENAPI_RAPIDOC_URL"
    ] = "https://cdn.jsdelivr.net/npm/rapidoc/dist/rapidoc-min.js"
    app.config["OPENAPI_RAPIDOC_CONFIG"] = {
        "allow-authentication": "false",
        "allow-spec-file-download": "true",
        "allow-server-selection": "false",
        "show-header": "false",
    }

    if not mock:
        explorer_config = toml.load(toml_config)
    else:
        explorer_config = mock_config
    app.config["explorer"] = explorer_config

    # Setup logger
    log_file = explorer_config["api"]["log"]["log_file"]
    app.extensions["logger"] = configure_logger("api", log_file, "info")

    # Create connections to external resources
    address_caching_server = create_address_caching_server(explorer_config, mock=mock)
    address_caching_server.init_app(app, "address_caching_server")

    cache = create_cache(explorer_config, mock=mock)
    cache.init_app(app)

    database = create_database(explorer_config, mock=mock)
    database.init_app(app)

    witnet_node = create_witnet_node(explorer_config, mock=mock)
    witnet_node.init_app(app)

    # Create top-level blueprints
    address_blueprint = Blueprint(
        "address",
        "address",
        description="Fetch address state.",
    )
    network_blueprint = Blueprint(
        "network",
        "network",
        description="Fetch network state.",
    )
    search_blueprint = Blueprint(
        "search",
        "search",
        description="Search for a block, transaction or epoch.",
    )
    transaction_blueprint = Blueprint(
        "transaction",
        "transaction",
        description="Execute transaction related API calls.",
    )

    # Create API
    api = Api(app)
    api.DEFAULT_ERROR_RESPONSE_NAME = None

    # Register all (nested) blueprints
    address_blueprint.register_blueprint(address_blocks_blueprint)
    address_blueprint.register_blueprint(address_data_requests_created_blueprint)
    address_blueprint.register_blueprint(address_data_requests_solved_blueprint)
    address_blueprint.register_blueprint(address_details_blueprint)
    address_blueprint.register_blueprint(address_info_blueprint)
    address_blueprint.register_blueprint(address_labels_blueprint)
    address_blueprint.register_blueprint(address_mints_blueprint)
    address_blueprint.register_blueprint(address_utxos_blueprint)
    address_blueprint.register_blueprint(address_value_transfers_blueprint)
    api.register_blueprint(address_blueprint, url_prefix="/api/address")

    api.register_blueprint(home_blueprint, url_prefix="/api")

    api.register_blueprint(ping_blueprint, url_prefix="/api")

    api.register_blueprint(status_blueprint, url_prefix="/api")

    network_blueprint.register_blueprint(network_balances_blueprint)
    network_blueprint.register_blueprint(network_blockchain_blueprint)
    network_blueprint.register_blueprint(network_mempool_blueprint)
    network_blueprint.register_blueprint(network_reputation_blueprint)
    network_blueprint.register_blueprint(network_statistics_blueprint)
    network_blueprint.register_blueprint(network_supply_blueprint)
    network_blueprint.register_blueprint(network_tapi_blueprint)
    api.register_blueprint(network_blueprint, url_prefix="/api/network")

    search_blueprint.register_blueprint(search_epoch_blueprint)
    search_blueprint.register_blueprint(search_hash_blueprint)
    api.register_blueprint(search_blueprint, url_prefix="/api/search")

    transaction_blueprint.register_blueprint(transaction_mempool_blueprint)
    transaction_blueprint.register_blueprint(transaction_priority_blueprint)
    transaction_blueprint.register_blueprint(transaction_send_blueprint)
    api.register_blueprint(transaction_blueprint, url_prefix="/api/transaction")

    return app
