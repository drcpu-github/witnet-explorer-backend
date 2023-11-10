import pytest
from marshmallow import ValidationError

from schemas.component.value_transfer_schema import (
    AddressOutput,
    ValueTransferTransactionForApi,
    ValueTransferTransactionForBlock,
    ValueTransferTransactionForExplorer,
)


@pytest.fixture
def address_output():
    return {
        "address": "wit100000000000000000000000000000000r0v4g2",
        "value": 1,
        "timelock": 0,
        "locked": False,
    }


def test_address_output_success(address_output):
    AddressOutput().load(address_output)


def test_address_output_failure_address(address_output):
    address_output["address"] = "xit100000000000000000000000000000000r0v4g"
    with pytest.raises(ValidationError) as err_info:
        AddressOutput().load(address_output)
    assert len(err_info.value.messages["address"]) == 2
    assert (
        err_info.value.messages["address"][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["address"][1]
        == "Address does not start with wit1 string."
    )


def test_address_output_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        AddressOutput().load(data)
    assert len(err_info.value.messages) == 4
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["value"][0] == "Missing data for required field."
    assert err_info.value.messages["timelock"][0] == "Missing data for required field."
    assert err_info.value.messages["locked"][0] == "Missing data for required field."


@pytest.fixture
def value_transfer_transaction_for_api():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
        "input_addresses": [
            "wit100000000000000000000000000000000r0v4g2",
            "wit100000000000000000000000000000000r0v4g2",
        ],
        "input_utxos": [
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
                "input_utxo": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0",
            },
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
                "input_utxo": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0",
            },
        ],
        "inputs_merged": [
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 2000,
            }
        ],
        "output_addresses": [
            "wit100000000000000000000000000000000r0v4g2",
            "wit100000000000000000000000000000000r0v4g2",
        ],
        "output_values": [1000, 1000],
        "timelocks": [0, 0],
        "utxos": [
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
                "timelock": 0,
                "locked": False,
            },
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 1000,
                "timelock": 0,
                "locked": False,
            },
        ],
        "utxos_merged": [
            {
                "address": "wit100000000000000000000000000000000r0v4g2",
                "value": 2000,
                "timelock": 0,
                "locked": False,
            }
        ],
        "fee": 1,
        "value": 2000,
        "priority": 1,
        "weight": 986,
        "true_output_addresses": [],
        "change_output_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "true_value": 0,
        "change_value": 2000,
    }


def test_value_transfer_transaction_for_api_success(value_transfer_transaction_for_api):
    ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)


def test_value_transfer_transaction_for_api_failure_address(
    value_transfer_transaction_for_api,
):
    value_transfer_transaction_for_api["input_addresses"] = [
        "wit100000000000000000000000000000000r0v4g",
    ]
    value_transfer_transaction_for_api["output_addresses"] = [
        "xit100000000000000000000000000000000r0v4g2",
    ]
    value_transfer_transaction_for_api["true_output_addresses"] = [
        "wit100000000000000000000000000000000r0v4g",
    ]
    value_transfer_transaction_for_api["change_output_addresses"] = [
        "xit100000000000000000000000000000000r0v4g2",
    ]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)
    assert len(err_info.value.messages) == 4
    assert (
        err_info.value.messages["input_addresses"][0][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["output_addresses"][0][0]
        == "Address does not start with wit1 string."
    )
    assert (
        err_info.value.messages["true_output_addresses"][0][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["change_output_addresses"][0][0]
        == "Address does not start with wit1 string."
    )


def test_value_transfer_transaction_for_api_failure_input(
    value_transfer_transaction_for_api,
):
    value_transfer_transaction_for_api["input_utxos"] = []
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of input addresses and utxos is different."
    )


def test_value_transfer_transaction_for_api_failure_output(
    value_transfer_transaction_for_api,
):
    value_transfer_transaction_for_api["timelocks"] = []
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of output addresses, values and timelocks is different."
    )


def test_value_transfer_transaction_for_api_failure_output_addresses(
    value_transfer_transaction_for_api,
):
    value_transfer_transaction_for_api["true_output_addresses"] = [
        "wit100000000000000000000000000000000r0v4g3"
    ]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)
    assert (
        err_info.value.messages["_schema"][0]
        == "Output addresses do not match, output_addresses != true_output_addresses + change_output_addresses."
    )


def test_value_transfer_transaction_for_api_failure_value(
    value_transfer_transaction_for_api,
):
    value_transfer_transaction_for_api["true_value"] = 1000
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)
    assert (
        err_info.value.messages["_schema"][0]
        == "Values do not match: value != true_value + change_value."
    )


def test_value_transfer_transaction_for_api_failure_output_values(
    value_transfer_transaction_for_api,
):
    value_transfer_transaction_for_api["output_values"] = [1, 1000]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(value_transfer_transaction_for_api)
    assert (
        err_info.value.messages["_schema"][0]
        == "Sum of output values is different from the value field."
    )


def test_value_transfer_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForApi().load(data)
    assert len(err_info.value.messages) == 22
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["inputs_merged"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["timelocks"][0] == "Missing data for required field."
    assert err_info.value.messages["utxos"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["utxos_merged"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["value"][0] == "Missing data for required field."
    assert err_info.value.messages["priority"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["true_output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["change_output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["true_value"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["change_value"][0] == "Missing data for required field."
    )


@pytest.fixture
def value_transfer_transaction_for_block():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "unique_input_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "true_output_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "true_value": 1000,
        "fee": 1,
        "weight": 1,
        "priority": 1,
    }


def test_value_transfer_transaction_for_block_success(
    value_transfer_transaction_for_block,
):
    ValueTransferTransactionForBlock().load(value_transfer_transaction_for_block)


def test_value_transfer_transaction_for_block_failure_address(
    value_transfer_transaction_for_block,
):
    value_transfer_transaction_for_block["unique_input_addresses"] = [
        "wit100000000000000000000000000000000r0v4g"
    ]
    value_transfer_transaction_for_block["true_output_addresses"] = [
        "xit100000000000000000000000000000000r0v4g2"
    ]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForBlock().load(value_transfer_transaction_for_block)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["unique_input_addresses"][0][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["true_output_addresses"][0][0]
        == "Address does not start with wit1 string."
    )


def test_value_transfer_transaction_for_block_failure_input_length(
    value_transfer_transaction_for_block,
):
    value_transfer_transaction_for_block["unique_input_addresses"] = []
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForBlock().load(value_transfer_transaction_for_block)
    assert len(err_info.value.messages) == 1
    assert err_info.value.messages["_schema"][0] == "Need at least one input address."


def test_value_transfer_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["unique_input_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["true_output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["true_value"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["priority"][0] == "Missing data for required field."


@pytest.fixture
def value_transfer_transaction_for_explorer():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
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
        "fee": 1,
        "weight": 626,
        "output_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "output_values": [1000],
        "timelocks": [0],
    }


def test_value_transfer_transaction_for_explorer_success(
    value_transfer_transaction_for_explorer,
):
    ValueTransferTransactionForExplorer().load(value_transfer_transaction_for_explorer)


def test_value_transfer_transaction_for_explorer_failure_input_lengths(
    value_transfer_transaction_for_explorer,
):
    value_transfer_transaction_for_explorer["input_addresses"] = [
        "wit100000000000000000000000000000000r0v4g2"
    ]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForExplorer().load(
            value_transfer_transaction_for_explorer
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of input addresses, values and UTXO's is different."
    )

    value_transfer_transaction_for_explorer["input_addresses"] = [
        "wit100000000000000000000000000000000r0v4g2",
        "wit100000000000000000000000000000000r0v4g2",
    ]
    value_transfer_transaction_for_explorer["input_values"] = [1000]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForExplorer().load(
            value_transfer_transaction_for_explorer
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of input addresses, values and UTXO's is different."
    )


def test_value_transfer_transaction_for_explorer_failure_output_lengths(
    value_transfer_transaction_for_explorer,
):
    value_transfer_transaction_for_explorer["output_values"] = [1000, 1000]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForExplorer().load(
            value_transfer_transaction_for_explorer
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of output addresses, values and timelocks is different."
    )

    value_transfer_transaction_for_explorer["output_values"] = [1000]
    value_transfer_transaction_for_explorer["timelocks"] = [0, 0]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForExplorer().load(
            value_transfer_transaction_for_explorer
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "Number of output addresses, values and timelocks is different."
    )


def test_value_transfer_transaction_for_explorer_failure_address(
    value_transfer_transaction_for_explorer,
):
    value_transfer_transaction_for_explorer["input_addresses"] = [
        "wit100000000000000000000000000000000r0v4g"
    ]
    value_transfer_transaction_for_explorer["output_addresses"] = [
        "xit100000000000000000000000000000000r0v4g2"
    ]
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForExplorer().load(
            value_transfer_transaction_for_explorer
        )
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["input_addresses"][0][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["output_addresses"][0][0]
        == "Address does not start with wit1 string."
    )


def test_value_transfer_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        ValueTransferTransactionForExplorer().load(data)
    print(err_info.value.messages)
    assert len(err_info.value.messages) == 10
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
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["timelocks"][0] == "Missing data for required field."
