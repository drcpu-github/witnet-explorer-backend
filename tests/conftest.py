import json

import pytest

from api import create_app
from blockchain.config import BlockchainConfig
from blockchain.consensus_constants import ConsensusConstants
from blockchain.objects.wip import WIP
from mockups.config import mock_config
from mockups.database import MockDatabase
from mockups.witnet_node import MockWitnetNode
from tests.schemas.component.test_block_schema import (  # noqa: F401
    block_details,
    block_for_api,
    block_transactions_for_block,
)
from tests.schemas.component.test_commit_schema import (  # noqa: F401
    commit_transaction_for_api,
    commit_transaction_for_block,
    commit_transaction_for_data_request,
    commit_transaction_for_explorer,
)
from tests.schemas.component.test_data_request_schema import (  # noqa: F401
    data_request,
    data_request_retrieval,
    data_request_transaction_for_api,
    data_request_transaction_for_block,
    data_request_transaction_for_explorer,
)
from tests.schemas.component.test_mint_schema import (  # noqa: F401
    mint_transaction,
    mint_transaction_for_api,
    mint_transaction_for_block,
    mint_transaction_for_explorer,
)
from tests.schemas.component.test_reveal_schema import (  # noqa: F401
    reveal_transaction_for_api,
    reveal_transaction_for_block,
    reveal_transaction_for_data_request,
    reveal_transaction_for_explorer,
)
from tests.schemas.component.test_stake_schema import (  # noqa: F401
    stake_transaction_for_api,
    stake_transaction_for_block,
    stake_transaction_for_explorer,
)
from tests.schemas.component.test_tally_schema import (  # noqa: F401
    tally_addresses,
    tally_output,
    tally_summary,
    tally_transaction_for_api,
    tally_transaction_for_block,
    tally_transaction_for_data_request,
    tally_transaction_for_explorer,
)
from tests.schemas.component.test_unstake_schema import (  # noqa: F401
    unstake_transaction_for_api,
    unstake_transaction_for_block,
    unstake_transaction_for_explorer,
)
from tests.schemas.component.test_value_transfer_schema import (  # noqa: F401
    value_transfer_transaction_for_api,
    value_transfer_transaction_for_block,
    value_transfer_transaction_for_explorer,
)
from tests.schemas.include.test_post_transaction_schema import (  # noqa: F401
    stake,
    stake_body,
    unstake,
    unstake_body,
    value_transfer,
)
from tests.schemas.search.test_data_request_history_schema import (  # noqa: F401
    data_request_history,
    data_request_history_entry,
    data_request_history_parameters,
    data_request_history_RAD,
)
from tests.schemas.search.test_data_request_report_schema import (  # noqa: F401
    data_request_report,
)


@pytest.fixture(autouse=True)
def config():
    BlockchainConfig.config = {"environment": {"network": "pytest"}}
    BlockchainConfig.wip = WIP(mockup=True)
    BlockchainConfig.consensus_constants = ConsensusConstants(mockup=True)


@pytest.fixture
def client():
    app = create_app(mock_config, mockup=True)
    return app.test_client()


@pytest.fixture
def mints(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("mints.json", "mint")
    return json.load(open("mockups/data/mints.json"))


@pytest.fixture
def value_transfers(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("value_transfers.json", "value_transfer")
    return json.load(open("mockups/data/value_transfers.json"))


@pytest.fixture
def data_requests(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("data_requests.json", "data_request")
    return json.load(open("mockups/data/data_requests.json"))


@pytest.fixture
def commits(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("commits.json", "commit")
    return json.load(open("mockups/data/commits.json"))


@pytest.fixture
def reveals(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("reveals.json", "reveal")
    return json.load(open("mockups/data/reveals.json"))


@pytest.fixture
def tallies(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("tallies.json", "tally")
    return json.load(open("mockups/data/tallies.json"))


@pytest.fixture
def stakes(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("stakes.json", "stake")
    return json.load(open("mockups/data/stakes.json"))


@pytest.fixture
def unstakes(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("unstakes.json", "unstake")
    return json.load(open("mockups/data/unstakes.json"))


@pytest.fixture
def balances():
    return json.load(open("mockups/data/balances.json"))


@pytest.fixture
def blockchain():
    return json.load(open("mockups/data/blockchain.json"))


@pytest.fixture
def home():
    return json.load(open("mockups/data/home.json"))


@pytest.fixture
def status():
    return json.load(open("mockups/data/status.json"))


@pytest.fixture
def mempool():
    return json.load(open("mockups/data/mempool.json"))


@pytest.fixture
def priority():
    return json.load(open("mockups/data/priority.json"))


@pytest.fixture
def utxos():
    return json.load(open("mockups/data/utxos.json"))


@pytest.fixture
def reputation():
    return json.load(open("mockups/data/reputation.json"))["cache"]


@pytest.fixture
def blocks():
    return json.load(open("mockups/data/blocks.json"))


@pytest.fixture
def address_data():
    return json.load(open("mockups/data/address_data.json"))


@pytest.fixture
def data_request_history_dro():
    return json.load(open("mockups/data/data_request_history_dro.json"))


@pytest.fixture
def data_request_history_rad():
    return json.load(open("mockups/data/data_request_history_rad.json"))


@pytest.fixture
def data_request_reports(client):
    cache = client.application.extensions["cache"]
    cache.load_json_into_cache("data_request_reports.json", "data_request_report")
    return json.load(open("mockups/data/data_request_reports.json"))


@pytest.fixture
def network_mempool():
    return json.load(open("mockups/data/network_mempool.json"))


@pytest.fixture
def network_statistics():
    return json.load(open("mockups/data/network_statistics.json"))


@pytest.fixture
def tapi():
    return json.load(open("mockups/data/tapi.json"))


@pytest.fixture
def version():
    return json.load(open("mockups/data/network_versions.json"))["api"]


@pytest.fixture
def network_stakes():
    return json.load(open("mockups/data/query_stakes.json"))["api"]


@pytest.fixture
def database():
    return MockDatabase()


@pytest.fixture
def witnet_node():
    return MockWitnetNode()


@pytest.fixture
def consensus_constants(database):
    return ConsensusConstants(database=database)
