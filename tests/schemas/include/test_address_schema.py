import pytest
from marshmallow import ValidationError

from schemas.include.address_schema import AddressSchema


def test_address_success():
    data = {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"}
    AddressSchema().load(data)


def test_address_failure_length():
    data = {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfs"}
    with pytest.raises(ValidationError) as err_info:
        AddressSchema().load(data)
    assert (
        err_info.value.messages["address"][0]
        == "Address does not contain 42 characters."
    )


def test_address_failure_wit1():
    data = {"address": "xit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"}
    with pytest.raises(ValidationError) as err_info:
        AddressSchema().load(data)
    assert (
        err_info.value.messages["address"][0]
        == "Address does not start with wit1 string."
    )
