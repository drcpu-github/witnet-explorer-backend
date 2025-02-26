import pytest
from marshmallow import ValidationError

from schemas.misc.home_schema import (
    HomeArgs,
    HomeBlock,
    HomeNetworkStats,
    HomeResponse,
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
]


def test_home_success():
    data = {}
    home = HomeArgs().load(data)
    assert home["key"] == "full"

    data = {"key": "network_stats"}
    home = HomeArgs().load(data)
    assert home["key"] == "network_stats"


def test_home_failure_one_of():
    data = {"key": "blocks_mined"}
    with pytest.raises(ValidationError) as err_info:
        HomeArgs().load(data)
    assert (
        err_info.value.messages["key"][0] == f"Must be one of: {', '.join(valid_keys)}."
    )


def test_home_network_stats_success():
    data = {
        "epochs": 0,
        "num_blocks": 0,
        "num_data_requests": 0,
        "num_value_transfers": 0,
        "num_stakes": 0,
        "num_unstakes": 0,
        "num_pending_requests": 0,
    }
    HomeNetworkStats().load(data)


def test_home_network_stats_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        HomeNetworkStats().load(data)
    assert len(err_info.value.messages) == 7
    assert err_info.value.messages["epochs"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["num_blocks"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_data_requests"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_value_transfers"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_stakes"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_unstakes"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_pending_requests"][0]
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


def test_home_response_success():
    data = {
        "network_stats": {
            "epochs": 0,
            "num_blocks": 0,
            "num_data_requests": 0,
            "num_value_transfers": 0,
            "num_stakes": 0,
            "num_unstakes": 0,
            "num_pending_requests": 0,
        },
        "supply_info": {
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
            "current_supply": 0,
            "supply_burned_lies": 0,
            "current_staked_supply": 0,
        },
        "latest_blocks": [
            {
                "hash": "a4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "data_request": 0,
                "value_transfer": 0,
                "stake": 0,
                "unstake": 0,
                "timestamp": 0,
                "confirmed": True,
            },
            {
                "hash": "b4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "data_request": 0,
                "value_transfer": 0,
                "stake": 0,
                "unstake": 0,
                "timestamp": 0,
                "confirmed": True,
            },
        ],
        "latest_data_requests": [
            {
                "hash": "c4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "timestamp": 0,
                "confirmed": True,
            },
            {
                "hash": "d4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "timestamp": 0,
                "confirmed": True,
            },
        ],
        "latest_value_transfers": [
            {
                "hash": "e4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "timestamp": 0,
                "confirmed": True,
            },
            {
                "hash": "f4ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "timestamp": 0,
                "confirmed": True,
            },
        ],
        "latest_stakes": [
            {
                "hash": "0ea59be6f3c154836b26387dcdfbb5372f47a338706b8f108da342b699f0f1b3",
                "timestamp": 0,
                "confirmed": True,
            },
            {
                "hash": "73c94862d3ce34d6d3a6844ad29aef0a23f952ccd4cc88559bd72e12615029df",
                "timestamp": 0,
                "confirmed": True,
            },
        ],
        "latest_unstakes": [
            {
                "hash": "c66e40e48763678b4ada2442e59b5a04b69a620a2bf72799abf0cd652a5a68e7",
                "timestamp": 0,
                "confirmed": True,
            },
            {
                "hash": "8c691354a80176785fac53c7671b075daa732e69e78f426a5083cb7458372b93",
                "timestamp": 0,
                "confirmed": True,
            },
        ],
        "last_updated": 0,
    }
    HomeResponse().load(data)
