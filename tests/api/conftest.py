import json

import pytest

from api import create_app


@pytest.fixture
def client():
    app = create_app(mock=True)
    return app.test_client()


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
def data_request_reports():
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
