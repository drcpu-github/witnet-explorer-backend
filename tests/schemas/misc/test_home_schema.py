import pytest
from marshmallow import ValidationError

from schemas.misc.home_schema import (
    HomeArgs,
    HomeBlock,
    HomeNetworkStats,
    HomeResponse,
    HomeStaked,
    HomeTransaction,
)

valid_keys = [
    "full",
    "network_stats",
    "supply_info",
    "blocks",
    "data_requests",
    "value_transfers",
    "stakes",
    "unstakes",
    "total_staked",
]


def test_home_success():
    data = {}
    home = HomeArgs().load(data)
    assert home["key"] == "full"

    for key in valid_keys:
        data = {"key": key}
        home = HomeArgs().load(data)
        assert home["key"] == key


def test_home_failure_one_of():
    data = {"key": "blocks_mined"}
    with pytest.raises(ValidationError) as err_info:
        HomeArgs().load(data)
    assert (
        err_info.value.messages["key"][0] == f"Must be one of: {', '.join(valid_keys)}."
    )


def test_home_network_stats_success():
    data = {
        "validators": 0,
        "epochs": 0,
        "blocks": 0,
        "data_requests": 0,
        "value_transfers": 0,
        "stakes": 0,
        "unstakes": 0,
        "pending_requests": 0,
    }
    HomeNetworkStats().load(data)


def test_home_network_stats_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        HomeNetworkStats().load(data)
    assert len(err_info.value.messages) == 8
    assert (
        err_info.value.messages["validators"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["epochs"][0] == "Missing data for required field."
    assert err_info.value.messages["blocks"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_requests"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["value_transfers"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["stakes"][0] == "Missing data for required field."
    assert err_info.value.messages["unstakes"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["pending_requests"][0]
        == "Missing data for required field."
    )


def test_home_block_success():
    data = {
        "hash": "a4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
        "data_request": 0,
        "value_transfer": 0,
        "stake": 0,
        "unstake": 0,
        "timestamp": 0,
        "confirmed": True,
    }
    HomeBlock().load(data)


def test_home_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        HomeBlock().load(data)
    assert len(err_info.value.messages) == 7
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["value_transfer"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["stake"][0] == "Missing data for required field."
    assert err_info.value.messages["unstake"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."


def test_home_transaction_success():
    data = {
        "hash": "c4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
        "timestamp": 0,
        "confirmed": True,
    }
    HomeTransaction().load(data)


def test_home_transaction_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        HomeTransaction().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."


def test_home_staked_success():
    data = {
        "timestamp": 1_000,
        "staked": 1_000,
    }
    HomeStaked().load(data)


def test_home_staked_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        HomeStaked().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["staked"][0] == "Missing data for required field."


def test_home_response_success(home):
    HomeResponse().load(home)
