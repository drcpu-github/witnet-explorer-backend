import pytest
from marshmallow import ValidationError

from schemas.address.block_view_schema import BlockView
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def block():
    return {
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
        "stakes": 1,
        "unstakes": 1,
        "confirmed": True,
    }


def test_address_block_response_success(block):
    data = [block, block]
    BlockView(many=True).load(data)


def test_address_block_response_failure_address(block):
    generic_address_test(
        block,
        ("miner",),
        BlockView,
    )


def test_address_block_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockView().load(data)
    assert len(err_info.value.messages) == 14
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
    assert err_info.value.messages["stakes"][0] == "Missing data for required field."
    assert err_info.value.messages["unstakes"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
