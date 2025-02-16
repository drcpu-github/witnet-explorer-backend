import json

import pytest

from blockchain.config import BlockchainConfig
from blockchain.consensus_constants import ConsensusConstants
from blockchain.objects.wip import WIP


@pytest.fixture(autouse=True)
def config():
    BlockchainConfig.config = {"environment": {"network": "pytest"}}
    BlockchainConfig.wip = WIP(mockup=True)
    BlockchainConfig.consensus_constants = ConsensusConstants(mockup=True)


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


@pytest.fixture
def stakes():
    return json.load(open("mockups/data/stakes.json"))


@pytest.fixture
def unstakes():
    return json.load(open("mockups/data/unstakes.json"))
