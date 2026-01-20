import pytest
from marshmallow import ValidationError

from schemas.component.unstake_schema import (
    UnstakeTransactionForApi,
    UnstakeTransactionForBlock,
    UnstakeTransactionForExplorer,
)
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def unstake_transaction_for_api():
    return {
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 301,
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "unstake_value": 1_500,
        "fee": 1,
        "nonce": 1,
        "weight": 986,
        "priority": 1,
        "timestamp": 1_738_193_845,
        "confirmed": True,
        "reverted": False,
    }


def test_unstake_transaction_for_api_success(unstake_transaction_for_api):
    UnstakeTransactionForApi().load(unstake_transaction_for_api)


def test_unstake_transaction_for_api_failure_address(
    unstake_transaction_for_api,
):
    generic_address_test(
        unstake_transaction_for_api,
        ("validator", "withdrawer"),
        UnstakeTransactionForApi,
    )


def test_unstake_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        UnstakeTransactionForApi().load(data)
    assert len(err_info.value.messages) == 13
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["unstake_value"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["nonce"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["priority"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."


@pytest.fixture
def unstake_transaction_for_block():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 301,
        "timestamp": 1_738_193_845,
        "fee": 1,
        "weight": 1,
        "priority": 1,
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "unstake_value": 10_000_000_000_000,
        "nonce": 1,
    }


def test_unstake_transaction_for_block_success(
    unstake_transaction_for_block,
):
    UnstakeTransactionForBlock().load(unstake_transaction_for_block)


def test_unstake_transaction_for_block_failure_input_length(
    unstake_transaction_for_block,
):
    generic_address_test(
        unstake_transaction_for_block,
        ("validator", "withdrawer"),
        UnstakeTransactionForBlock,
    )


def test_unstake_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        UnstakeTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 10
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["priority"][0] == "Missing data for required field."
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["unstake_value"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["nonce"][0] == "Missing data for required field."


@pytest.fixture
def unstake_transaction_for_explorer():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 301,
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "unstake_value": 1000,
        "fee": 1,
        "nonce": 1,
        "weight": 626,
    }


def test_unstake_transaction_for_explorer_success(
    unstake_transaction_for_explorer,
):
    UnstakeTransactionForExplorer().load(unstake_transaction_for_explorer)


def test_unstake_transaction_for_explorer_failure_epoch(
    unstake_transaction_for_explorer,
):
    unstake_transaction_for_explorer["epoch"] = 280
    with pytest.raises(ValidationError) as err_info:
        UnstakeTransactionForExplorer().load(unstake_transaction_for_explorer)
    assert (
        err_info.value.messages["epoch"][0]
        == "Unstake transactions are not allowed yet."
    )


def test_unstake_transaction_for_explorer_failure_address(
    unstake_transaction_for_explorer,
):
    generic_address_test(
        unstake_transaction_for_explorer,
        ("validator", "withdrawer"),
        UnstakeTransactionForExplorer,
    )


def test_unstake_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        UnstakeTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 8
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["unstake_value"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["nonce"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
