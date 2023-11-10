import pytest
from marshmallow import ValidationError

from schemas.component.reveal_schema import (
    RevealTransactionForApi,
    RevealTransactionForBlock,
    RevealTransactionForDataRequest,
    RevealTransactionForExplorer,
)


@pytest.fixture
def reveal_transaction_for_api():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
        "address": "wit100000000000000000000000000000000r0v4g2",
        "reveal": "124205",
        "success": True,
    }


def test_reveal_transaction_for_api_success(reveal_transaction_for_api):
    RevealTransactionForApi().load(reveal_transaction_for_api)


def test_reveal_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForApi().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."


@pytest.fixture
def reveal_transaction_for_block():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "address": "wit100000000000000000000000000000000r0v4g2",
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "reveal": "124205",
        "success": True,
    }


def test_reveal_transaction_for_block_success(reveal_transaction_for_block):
    RevealTransactionForBlock().load(reveal_transaction_for_block)


def test_reveal_transaction_for_block_failure_hash(reveal_transaction_for_block):
    reveal_transaction_for_block[
        "data_request"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForBlock().load(reveal_transaction_for_block)
    assert (
        err_info.value.messages["data_request"][0] == "Hash is not a hexadecimal value."
    )


def test_reveal_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 6
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."


@pytest.fixture
def reveal_transaction_for_data_request():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "address": "wit100000000000000000000000000000000r0v4g2",
        "confirmed": True,
        "reverted": False,
        "reveal": "124205",
        "success": True,
        "error": False,
        "liar": False,
    }


def test_reveal_transaction_for_data_request_success(
    reveal_transaction_for_data_request,
):
    RevealTransactionForDataRequest().load(reveal_transaction_for_data_request)


def test_reveal_transaction_for_data_request_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForDataRequest().load(data)
    assert len(err_info.value.messages) == 11
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."
    assert err_info.value.messages["error"][0] == "Missing data for required field."
    assert err_info.value.messages["liar"][0] == "Missing data for required field."


@pytest.fixture
def reveal_transaction_for_explorer():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "address": "wit100000000000000000000000000000000r0v4g2",
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "reveal": bytearray([26, 0, 1, 229, 45]),
        "success": True,
    }


def test_reveal_transaction_for_explorer_success(reveal_transaction_for_explorer):
    RevealTransactionForExplorer().load(reveal_transaction_for_explorer)


def test_reveal_transaction_for_explorer_failure_hash(reveal_transaction_for_explorer):
    reveal_transaction_for_explorer[
        "data_request"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForExplorer().load(reveal_transaction_for_explorer)
    assert (
        err_info.value.messages["data_request"][0] == "Hash is not a hexadecimal value."
    )


def test_reveal_transaction_for_explorer_failure_bytearray(
    reveal_transaction_for_explorer,
):
    reveal_transaction_for_explorer["reveal"] = ""
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForExplorer().load(reveal_transaction_for_explorer)
    assert err_info.value.messages["reveal"][0] == "Input type is not a bytearray."


def test_reveal_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        RevealTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 6
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."
