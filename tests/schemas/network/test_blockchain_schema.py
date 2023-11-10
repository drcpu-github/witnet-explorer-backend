import pytest
from marshmallow import ValidationError

from schemas.network.blockchain_schema import BlockchainBlock, NetworkBlockchainResponse


def test_blockchain_block_success():
    data = {
        "hash": "24ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
        "miner": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
        "value_transfers": 0,
        "data_requests": 1,
        "commits": 2,
        "reveals": 3,
        "tallies": 4,
        "fees": 5,
        "epoch": 6,
        "timestamp": 7,
        "confirmed": True,
    }
    BlockchainBlock().load(data)


def test_blockchain_block_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockchainBlock().load(data)
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
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
    assert err_info.value.messages["fees"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."


def test_blockchain_response_block_success():
    data = {
        "blockchain": [
            {
                "hash": "24ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b1",
                "miner": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
                "value_transfers": 0,
                "data_requests": 1,
                "commits": 2,
                "reveals": 3,
                "tallies": 4,
                "fees": 5,
                "epoch": 6,
                "timestamp": 7,
                "confirmed": True,
            },
            {
                "hash": "24ef311401232da383ab4dc627cc8b9c1cdebd43f57a8022b383ab099b68e2b2",
                "miner": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
                "value_transfers": 0,
                "data_requests": 1,
                "commits": 2,
                "reveals": 3,
                "tallies": 4,
                "fees": 5,
                "epoch": 6,
                "timestamp": 7,
                "confirmed": False,
            },
        ],
        "reverted": [],
        "total_epochs": 0,
    }
    NetworkBlockchainResponse().load(data)


def test_blockchain_response_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkBlockchainResponse().load(data)
    assert len(err_info.value.messages) == 3
    assert (
        err_info.value.messages["blockchain"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["total_epochs"][0] == "Missing data for required field."
    )


def test_blockchain_response_block_failure_missing_block_data():
    data = {
        "blockchain": [
            {
                "value_transfers": 0,
                "data_requests": 1,
                "commits": 2,
                "reveals": 3,
                "tallies": 4,
                "fees": 5,
                "epoch": 6,
                "timestamp": 7,
                "confirmed": True,
            },
            {
                "value_transfers": 0,
                "data_requests": 1,
                "commits": 2,
                "reveals": 3,
                "tallies": 4,
                "fees": 5,
                "epoch": 6,
                "timestamp": 7,
                "confirmed": False,
            },
        ],
        "reverted": [],
    }
    with pytest.raises(ValidationError) as err_info:
        NetworkBlockchainResponse().load(data)
    assert len(err_info.value.messages["blockchain"]) == 2
    assert (
        err_info.value.messages["blockchain"][0]["hash"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["blockchain"][0]["miner"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["blockchain"][1]["hash"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["blockchain"][1]["miner"][0]
        == "Missing data for required field."
    )
