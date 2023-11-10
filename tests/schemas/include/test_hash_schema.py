import pytest
from marshmallow import ValidationError

from schemas.include.hash_schema import HashSchema


def test_hash_success():
    data = {"hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"}
    HashSchema().load(data)


def test_hash_0x_success():
    data = {
        "hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    }
    hash_schema = HashSchema().load(data)
    assert (
        hash_schema["hash"]
        == "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    )


def test_hash_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        HashSchema().load(data)
    print(err_info.value.messages)
    assert err_info.value.messages["hash"][0] == "Missing data for required field."


def test_hash_failure_length():
    data = {"hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"}
    with pytest.raises(ValidationError) as err_info:
        HashSchema().load(data)
    assert err_info.value.messages["hash"][0] == "Hash does not contain 64 characters."


def test_hash_0x_failure_length():
    data = {"hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"}
    with pytest.raises(ValidationError) as err_info:
        HashSchema().load(data)
    assert err_info.value.messages["hash"][0] == "Hash does not contain 64 characters."


def test_hash_failure_hexadecimal():
    data = {"hash": "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"}
    with pytest.raises(ValidationError) as err_info:
        HashSchema().load(data)
    assert err_info.value.messages["hash"][0] == "Hash is not a hexadecimal value."
