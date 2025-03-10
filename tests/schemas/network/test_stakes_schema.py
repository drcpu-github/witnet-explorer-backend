import pytest
from marshmallow import ValidationError

from schemas.network.stakes_schema import (
    NetworkStake,
    NetworkStakesArgs,
    NetworkStakesResponse,
)
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def network_stakes_arg():
    return {
        "validator": "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33",
        "withdrawer": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
    }


def test_network_stakes_args_success():
    NetworkStakesArgs().load({})
    NetworkStakesArgs().load(
        {"validator": "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33"}
    )
    NetworkStakesArgs().load(
        {"withdrawer": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"}
    )
    NetworkStakesArgs().load(
        {
            "validator": "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33",
            "withdrawer": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        }
    )


def test_network_stakes_args_failure_address(network_stakes_arg):
    generic_address_test(
        network_stakes_arg,
        ("validator", "withdrawer"),
        NetworkStakesArgs,
    )


@pytest.fixture
def network_stake(network_stakes_arg):
    network_stake = network_stakes_arg
    network_stake.update(
        {
            "current_stake": 9636213087148406,
            "time_weighted_stake": 9636213087148406,
            "nonce": 2078,
            "blocks": 100,
            "rewards": 200,
            "data_requests": 300,
            "lies": 400,
            "genesis": 500,
            "last_active": 600,
        }
    )
    return network_stake


def test_network_version_success(network_stake):
    NetworkStake().load(network_stake)


def test_network_version_failure_address(network_stake):
    generic_address_test(
        network_stake,
        ("validator", "withdrawer"),
        NetworkStake,
    )


def test_network_stake_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStake().load(data)
    assert len(err_info.value.messages) == 11
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["current_stake"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["time_weighted_stake"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["nonce"][0] == "Missing data for required field."
    assert err_info.value.messages["blocks"][0] == "Missing data for required field."
    assert err_info.value.messages["rewards"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_requests"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["lies"][0] == "Missing data for required field."
    assert err_info.value.messages["genesis"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["last_active"][0] == "Missing data for required field."
    )


@pytest.fixture
def network_stakes(network_stake):
    return {
        "stakes": [
            network_stake,
            network_stake,
        ],
        "last_updated": 1,
    }


def test_network_stakes_response_success(network_stakes):
    NetworkStakesResponse().load(network_stakes)


def test_network_stakes_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStakesResponse().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["stakes"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["last_updated"][0] == "Missing data for required field."
    )
