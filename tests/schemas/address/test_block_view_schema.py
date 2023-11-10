import pytest
from marshmallow import ValidationError

from schemas.address.block_view_schema import BlockView


def test_address_block_response_success():
    data = [
        {
            "hash": "deb7803196cb03fed0747820e97605bddcecfc3c62188ab0916122a73b4cf972",
            "miner": "wit100000000000000000000000000000000r0v4g2",
            "timestamp": 1,
            "epoch": 1,
            "block_reward": 1,
            "block_fees": 1,
            "value_transfers": 1,
            "data_requests": 1,
            "commits": 1,
            "reveals": 1,
            "tallies": 1,
            "confirmed": True,
        },
        {
            "hash": "deb7803196cb03fed0747820e97605bddcecfc3c62188ab0916122a73b4cf972",
            "miner": "wit100000000000000000000000000000000r0v4g2",
            "timestamp": 1,
            "epoch": 2,
            "block_reward": 1,
            "block_fees": 1,
            "value_transfers": 1,
            "data_requests": 1,
            "commits": 1,
            "reveals": 1,
            "tallies": 1,
            "confirmed": True,
        },
    ]
    BlockView(many=True).load(data)


def test_address_block_response_failure_address():
    data = {
        "hash": "deb7803196cb03fed0747820e97605bddcecfc3c62188ab0916122a73b4cf972",
        "miner": "xit100000000000000000000000000000000r0v4g",
        "timestamp": 1,
        "epoch": 1,
        "block_reward": 1,
        "block_fees": 1,
        "value_transfers": 1,
        "data_requests": 1,
        "commits": 1,
        "reveals": 1,
        "tallies": 1,
        "confirmed": True,
    }
    with pytest.raises(ValidationError) as err_info:
        BlockView().load(data)
    assert (
        err_info.value.messages["miner"][0] == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["miner"][1]
        == "Address does not start with wit1 string."
    )


def test_address_block_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockView().load(data)
    assert len(err_info.value.messages) == 12
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["block_reward"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["block_fees"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["value_transfers"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["data_requests"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["commits"][0] == "Missing data for required field."
    assert err_info.value.messages["reveals"][0] == "Missing data for required field."
    assert err_info.value.messages["tallies"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
