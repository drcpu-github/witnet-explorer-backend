import pytest
from marshmallow import ValidationError

from schemas.address.utxos_schema import (
    AddressUtxosArgs,
    AddressUtxosResponse,
    OutputPointer,
    Utxo,
)


def test_address_utxos_args_success():
    data = {
        "addresses": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh"
    }
    AddressUtxosArgs().load(data)


def test_address_utxos_args_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        AddressUtxosArgs().load(data)
    assert err_info.value.messages["addresses"][0] == "Missing data for required field."


def test_output_pointer_success():
    data = {
        "output_pointer": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:1",
    }
    OutputPointer().load(data)


def test_output_pointer_failure_colon():
    data = {
        "output_pointer": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
    }
    with pytest.raises(ValidationError) as err_info:
        OutputPointer().load(data)
    assert (
        err_info.value.messages["output_pointer"][0]
        == "Cannot split output pointer into transaction hash and output index."
    )


def test_output_pointer_failure_hash():
    data = {
        "output_pointer": "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678:1",
    }
    with pytest.raises(ValidationError) as err_info:
        OutputPointer().load(data)
    assert len(err_info.value.messages["output_pointer"]) == 2
    assert (
        err_info.value.messages["output_pointer"][0]
        == "Hash does not contain 64 characters."
    )
    assert (
        err_info.value.messages["output_pointer"][1]
        == "Hash is not a hexadecimal value."
    )


def test_output_pointer_failure_index():
    data = {
        "output_pointer": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:a",
    }
    with pytest.raises(ValidationError) as err_info:
        OutputPointer().load(data)
    assert (
        err_info.value.messages["output_pointer"][0]
        == "Cannot convert output index to an integer."
    )


def test_utxo_success():
    data = {
        "output_pointer": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:1",
        "timelock": 0,
        "utxo_mature": True,
        "value": 1000,
    }
    Utxo().load(data)


def test_utxo_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        Utxo().load(data)
    assert len(err_info.value.messages) == 4
    assert (
        err_info.value.messages["output_pointer"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["timelock"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["utxo_mature"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["value"][0] == "Missing data for required field."


def test_address_utxos_response_success():
    data = [
        {
            "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
            "utxos": [
                {
                    "output_pointer": "aefc88a00c6ba98fdec3622baec52925a77378c124df325c2bd8a8842e0c01ad:1",
                    "timelock": 0,
                    "utxo_mature": True,
                    "value": 1,
                },
                {
                    "output_pointer": "befc88a00c6ba98fdec3622baec52925a77378c124df325c2bd8a8842e0c01ad:1",
                    "timelock": 0,
                    "utxo_mature": True,
                    "value": 10,
                },
            ],
        },
        {
            "address": "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh",
            "utxos": [
                {
                    "output_pointer": "cefc88a00c6ba98fdec3622baec52925a77378c124df325c2bd8a8842e0c01ad:1",
                    "timelock": 0,
                    "utxo_mature": True,
                    "value": 100,
                },
                {
                    "output_pointer": "defc88a00c6ba98fdec3622baec52925a77378c124df325c2bd8a8842e0c01ad:1",
                    "timelock": 0,
                    "utxo_mature": True,
                    "value": 1000,
                },
            ],
        },
    ]
    AddressUtxosResponse(many=True).load(data)
