import json

import pytest
import toml


@pytest.fixture
def config():
    return toml.load(open("explorer.testnet.toml"))


@pytest.fixture
def mints():
    return json.load(open("mockups/data/mints.json"))


@pytest.fixture
def value_transfers():
    return json.load(open("mockups/data/value_transfers.json"))


@pytest.fixture
def data_requests():
    return json.load(open("mockups/data/data_requests.json"))


@pytest.fixture
def commits():
    return json.load(open("mockups/data/commits.json"))


@pytest.fixture
def reveals():
    return json.load(open("mockups/data/reveals.json"))


@pytest.fixture
def tallies():
    return json.load(open("mockups/data/tallies.json"))
