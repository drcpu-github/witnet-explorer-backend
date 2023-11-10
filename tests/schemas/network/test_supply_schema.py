import pytest
from marshmallow import ValidationError

from schemas.network.supply_schema import NetworkSupply, NetworkSupplyArgs

valid_keys = [
    "blocks_minted",
    "blocks_minted_reward",
    "blocks_missing",
    "blocks_missing_reward",
    "current_locked_supply",
    "current_time",
    "current_unlocked_supply",
    "epoch",
    "in_flight_requests",
    "locked_wits_by_requests",
    "maximum_supply",
    "current_supply",
    "total_supply",
    "supply_burned_lies",
]


def test_network_supply_args_success():
    data = {"key": "blocks_minted"}
    network_supply = NetworkSupplyArgs().load(data)
    assert network_supply["key"] == "blocks_minted"


def test_network_supply_args_failure():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkSupplyArgs().load(data)
    assert err_info.value.messages["key"][0] == "Missing data for required field."


def test_network_supply_args_failure_one_of():
    data = {"key": "blocks_mined"}
    with pytest.raises(ValidationError) as err_info:
        NetworkSupplyArgs().load(data)
    assert (
        err_info.value.messages["key"][0] == f"Must be one of: {', '.join(valid_keys)}."
    )


def test_network_supply_success():
    data = {
        "blocks_minted": 0,
        "blocks_minted_reward": 0,
        "blocks_missing": 0,
        "blocks_missing_reward": 0,
        "current_locked_supply": 0,
        "current_time": 0,
        "current_unlocked_supply": 0,
        "epoch": 0,
        "in_flight_requests": 0,
        "locked_wits_by_requests": 0,
        "maximum_supply": 0,
        "current_supply": 0,
        "total_supply": 0,
        "supply_burned_lies": 0,
    }
    NetworkSupply().load(data)
