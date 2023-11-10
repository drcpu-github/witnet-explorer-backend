import pytest
from marshmallow import ValidationError

from schemas.component.commit_schema import (
    CommitTransactionForApi,
    CommitTransactionForBlock,
    CommitTransactionForDataRequest,
    CommitTransactionForExplorer,
)


@pytest.fixture
def commit_transaction_for_api():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
        "address": "wit100000000000000000000000000000000r0v4g2",
        "input_utxos": [
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
                "input_utxo": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0",
            },
        ],
        "output_value": 10,
    }


def test_commit_transaction_for_api_success(commit_transaction_for_api):
    CommitTransactionForApi().load(commit_transaction_for_api)


def test_commit_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForApi().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_value"][0] == "Missing data for required field."
    )


@pytest.fixture
def commit_transaction_for_block():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "address": "wit100000000000000000000000000000000r0v4g2",
        "collateral": 1e10,
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
    }


def test_commit_transaction_for_block_success(commit_transaction_for_block):
    CommitTransactionForBlock().load(commit_transaction_for_block)


def test_commit_transaction_for_block_failure_hash(commit_transaction_for_block):
    commit_transaction_for_block[
        "data_request"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForBlock().load(commit_transaction_for_block)
    assert (
        err_info.value.messages["data_request"][0] == "Hash is not a hexadecimal value."
    )


def test_commit_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )


@pytest.fixture
def commit_transaction_for_data_request():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "address": "wit100000000000000000000000000000000r0v4g2",
        "confirmed": True,
        "reverted": False,
    }


def test_commit_transaction_for_data_request_success(
    commit_transaction_for_data_request,
):
    CommitTransactionForDataRequest().load(commit_transaction_for_data_request)


def test_commit_transaction_for_data_request_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForDataRequest().load(data)
    assert len(err_info.value.messages) == 7
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."


@pytest.fixture
def commit_transaction_for_explorer(commit_transaction_for_block):
    commit_transaction_for_block.update(
        {
            "input_utxos": [
                (
                    bytearray.fromhex(
                        "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                    ),
                    0,
                ),
                (
                    bytearray.fromhex(
                        "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                    ),
                    1,
                ),
            ],
            "input_values": [1000, 1000],
            "output_value": 10,
        }
    )
    return commit_transaction_for_block


def test_commit_transaction_for_explorer_success(commit_transaction_for_explorer):
    CommitTransactionForExplorer().load(commit_transaction_for_explorer)


def test_commit_transaction_for_explorer_success_none(commit_transaction_for_explorer):
    commit_transaction_for_explorer["output_value"] = None
    CommitTransactionForExplorer().load(commit_transaction_for_explorer)


def test_commit_transaction_for_explorer_failure_inputs(
    commit_transaction_for_explorer,
):
    commit_transaction_for_explorer["input_values"] = []
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForExplorer().load(commit_transaction_for_explorer)
    assert err_info.value.messages["input_values"] == "Need at least one input value."

    commit_transaction_for_explorer["input_values"] = [1000]
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForExplorer().load(commit_transaction_for_explorer)
    assert (
        err_info.value.messages["input_utxos"]
        == "Number of input UTXO's and input values is different."
    )


def test_commit_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        CommitTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 8
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_values"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_value"][0] == "Missing data for required field."
    )
