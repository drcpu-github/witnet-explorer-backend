import pytest
from marshmallow import ValidationError

from schemas.include.output_pointer_schema import OutputPointer


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
