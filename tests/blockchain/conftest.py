import pytest

from mockups.database import MockDatabase
from mockups.witnet_node import MockWitnetNode
from node.consensus_constants import ConsensusConstants


@pytest.fixture
def database():
    return MockDatabase()


@pytest.fixture
def witnet_node():
    return MockWitnetNode()


@pytest.fixture
def consensus_constants(database):
    return ConsensusConstants(database=database)
