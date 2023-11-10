import copy

import pytest
from marshmallow import ValidationError

from schemas.include.input_utxo_schema import InputUtxo, InputUtxoList, InputUtxoPointer


@pytest.fixture
def input_utxo():
    return {
        "address": "wit100000000000000000000000000000000r0v4g2",
        "value": 1,
    }


def test_input_utxo_success(input_utxo):
    InputUtxo().load(input_utxo)


def test_input_utxo_failure_address(input_utxo):
    input_utxo["address"] = "xit100000000000000000000000000000000r0v4g"
    with pytest.raises(ValidationError) as err_info:
        InputUtxo().load(input_utxo)
    assert len(err_info.value.messages["address"]) == 2
    assert (
        err_info.value.messages["address"][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["address"][1]
        == "Address does not start with wit1 string."
    )


def test_input_utxo_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        InputUtxo().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["value"][0] == "Missing data for required field."


@pytest.fixture
def input_utxo_pointer():
    return {
        "address": "wit100000000000000000000000000000000r0v4g2",
        "value": 1,
        "input_utxo": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0",
    }


def test_input_utxo_pointer_success(input_utxo_pointer):
    InputUtxoPointer().load(input_utxo_pointer)


def test_input_utxo_pointer_failure_pointer(input_utxo_pointer):
    input_utxo_pointer[
        "input_utxo"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0"
    with pytest.raises(ValidationError) as err_info:
        InputUtxoPointer().load(input_utxo_pointer)
    assert (
        err_info.value.messages["input_utxo"][0] == "Hash is not a hexadecimal value."
    )


def test_input_utxo_pointer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        InputUtxoPointer().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["value"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_utxo"][0] == "Missing data for required field."
    )


def test_input_utxo_list_success():
    data = {
        "input_utxos": [
            (
                bytearray.fromhex(
                    "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                ),
                1,
            ),
            (
                bytearray.fromhex(
                    "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                ),
                2,
            ),
        ],
    }
    pointer = InputUtxoList().load(copy.deepcopy(data))
    assert pointer == data


def test_input_utxo_list_failure_length():
    data = {
        "input_utxos": [
            (
                bytearray.fromhex(
                    "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                ),
            ),
            (
                bytearray.fromhex(
                    "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                ),
            ),
        ],
    }
    with pytest.raises(ValidationError) as err_info:
        InputUtxoList().load(data)
    assert len(err_info.value.messages["_schema"]) == 2
    assert (
        err_info.value.messages["_schema"][0]
        == "Output pointer tuple 0 has an invalid length."
    )
    assert (
        err_info.value.messages["_schema"][1]
        == "Output pointer tuple 1 has an invalid length."
    )


def test_input_utxo_list_failure_hash():
    data = {
        "input_utxos": [
            (
                bytearray(
                    "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
                    "utf-8",
                ),
                1,
            ),
        ],
    }
    with pytest.raises(ValidationError) as err_info:
        InputUtxoList().load(data)
    # A non-hexadecimal value encoded and decoded will (almost) always result in a hash with an incorrect length
    assert (
        err_info.value.messages["input_utxos"][0][0][0]
        == "Hash does not contain 64 characters."
    )


def test_input_utxo_list_failure_index():
    data = {
        "input_utxos": [
            (
                bytearray.fromhex(
                    "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                ),
                "a",
            ),
        ],
    }
    with pytest.raises(ValidationError) as err_info:
        InputUtxoList().load(data)
    assert err_info.value.messages["input_utxos"][0][1][0] == "Not a valid integer."


def test_input_utxo_list_failure_no_inputs():
    data = {
        "input_utxos": [],
    }
    with pytest.raises(ValidationError) as err_info:
        InputUtxoList().load(data)
    assert err_info.value.messages["input_utxos"] == "Need at least one input UTXO."


def test_input_utxo_list_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        InputUtxoList().load(data)
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
