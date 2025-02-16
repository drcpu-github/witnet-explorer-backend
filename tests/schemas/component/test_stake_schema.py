import pytest
from marshmallow import ValidationError

from schemas.component.stake_schema import (
    StakeTransactionForApi,
    StakeTransactionForBlock,
    StakeTransactionForExplorer,
)
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def stake_transaction_for_api():
    return {
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 101,
        "inputs": [
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
            },
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
            },
        ],
        "change_address": "wit100000000000000000000000000000000r0v4g2",
        "change_value": 499,
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "stake_value": 1_500,
        "fee": 1,
        "weight": 986,
        "priority": 1,
        "timestamp": 1602666090,
        "confirmed": True,
        "reverted": False,
    }


def test_stake_transaction_for_api_success(stake_transaction_for_api):
    StakeTransactionForApi().load(stake_transaction_for_api)


def test_stake_transaction_for_api_failure_epoch(stake_transaction_for_api):
    stake_transaction_for_api["epoch"] = 100
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForApi().load(stake_transaction_for_api)
    assert (
        err_info.value.messages["epoch"][0] == "Stake transactions are not allowed yet."
    )


def test_stake_transaction_for_api_failure_address(
    stake_transaction_for_api,
):
    generic_address_test(
        stake_transaction_for_api,
        ("change_address", "validator", "withdrawer"),
        StakeTransactionForApi,
    )


def test_stake_transaction_for_api_failure_value(
    stake_transaction_for_api,
):
    stake_transaction_for_api["change_value"] = 500
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForApi().load(stake_transaction_for_api)
    assert (
        err_info.value.messages["_schema"][0]
        == "Sum of inputs (2000) does not match sum of stake value, output value and fee (2001)."
    )


def test_stake_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForApi().load(data)
    assert len(err_info.value.messages) == 15
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["inputs"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["change_address"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["change_value"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["stake_value"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["priority"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."


@pytest.fixture
def stake_transaction_for_block():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 101,
        "timestamp": 1602666090,
        "fee": 1,
        "weight": 1,
        "priority": 1,
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "stake_value": 10_000_000_000_000,
    }


def test_stake_transaction_for_block_success(
    stake_transaction_for_block,
):
    StakeTransactionForBlock().load(stake_transaction_for_block)


def test_stake_transaction_for_block_failure_epoch(stake_transaction_for_block):
    stake_transaction_for_block["epoch"] = 100
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForBlock().load(stake_transaction_for_block)
    assert (
        err_info.value.messages["epoch"][0] == "Stake transactions are not allowed yet."
    )


def test_stake_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 9
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
        err_info.value.messages["stake_value"][0] == "Missing data for required field."
    )


@pytest.fixture
def stake_transaction_for_explorer():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 101,
        "input_addresses": [
            "wit100000000000000000000000000000000r0v4g2",
            "wit100000000000000000000000000000000r0v4g2",
        ],
        "input_values": [1000, 1000],
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
        "change_address": "wit100000000000000000000000000000000r0v4g2",
        "change_value": 1000,
        "fee": 1,
        "weight": 626,
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "stake_value": 10_000_000_000_000,
    }


def test_stake_transaction_for_explorer_success(
    stake_transaction_for_explorer,
):
    StakeTransactionForExplorer().load(stake_transaction_for_explorer)


def test_stake_transaction_for_explorer_failure_epoch(stake_transaction_for_explorer):
    stake_transaction_for_explorer["epoch"] = 100
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForExplorer().load(stake_transaction_for_explorer)
    assert (
        err_info.value.messages["epoch"][0] == "Stake transactions are not allowed yet."
    )


def test_stake_transaction_for_explorer_no_output_success(
    stake_transaction_for_explorer,
):
    stake_transaction_for_explorer["change_address"] = None
    stake_transaction_for_explorer["change_value"] = None
    StakeTransactionForExplorer().load(stake_transaction_for_explorer)


def test_stake_transaction_for_explorer_failure_input_lengths(
    stake_transaction_for_explorer,
):
    stake_transaction_for_explorer["input_addresses"] = [
        "wit100000000000000000000000000000000r0v4g2"
    ]
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForExplorer().load(stake_transaction_for_explorer)
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of input addresses, values and UTXO's is different."
    )


def test_stake_transaction_for_explorer_failure_address(
    stake_transaction_for_explorer,
):
    generic_address_test(
        stake_transaction_for_explorer,
        (("input_addresses",), "change_address", "validator", "withdrawer"),
        StakeTransactionForExplorer,
    )


def test_stake_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 12
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_values"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["change_address"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["change_value"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["stake_value"][0] == "Missing data for required field."
    )
