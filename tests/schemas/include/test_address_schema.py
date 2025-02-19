import pytest
from marshmallow import ValidationError

from schemas.include.address_schema import AddressSchema


def test_address_success():
    data = {"address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"}
    AddressSchema().load(data)


def test_address_failure_length():
    data = {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfs"}
    with pytest.raises(ValidationError) as err_info:
        AddressSchema().load(data)
    assert (
        err_info.value.messages["address"][0]
        == "Mainnet address does not contain 42 characters."
    )


def test_address_failure_wit1():
    data = {"address": "xit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"}
    with pytest.raises(ValidationError) as err_info:
        AddressSchema().load(data)
    assert (
        err_info.value.messages["address"][0]
        == "Address does not start with wit1 / twit1 string."
    )


def generic_address_test(data_object, addresses, marshmallow_object):
    for address in addresses:
        # Test address prefix
        if type(address) is tuple:
            data_object[address[0]] = [
                "xit100000000000000000000000000000000r0v4g2",
            ]
        else:
            data_object[address] = "xit100000000000000000000000000000000r0v4g2"

        with pytest.raises(ValidationError) as err_info:
            marshmallow_object().load(data_object)

        if type(address) is tuple:
            error_message = err_info.value.messages[address[0]][0][0]
        else:
            error_message = err_info.value.messages[address][0]

        assert error_message == "Address does not start with wit1 / twit1 string."

        # Test mainnet address length
        if type(address) is tuple:
            data_object[address[0]] = [
                "wit100000000000000000000000000000000r0v4g",
            ]
        else:
            data_object[address] = "wit100000000000000000000000000000000r0v4g"

        with pytest.raises(ValidationError) as err_info:
            marshmallow_object().load(data_object)

        if type(address) is tuple:
            error_message = err_info.value.messages[address[0]][0][0]
        else:
            error_message = err_info.value.messages[address][0]

        assert error_message == "Mainnet address does not contain 42 characters."

        # Test testnet address length
        if type(address) is tuple:
            data_object[address[0]] = [
                "twit100000000000000000000000000000000r0v4g",
            ]
        else:
            data_object[address] = "twit100000000000000000000000000000000r0v4g"

        with pytest.raises(ValidationError) as err_info:
            marshmallow_object().load(data_object)

        if type(address) is tuple:
            error_message = err_info.value.messages[address[0]][0][0]
        else:
            error_message = err_info.value.messages[address][0]

        assert error_message == "Testnet address does not contain 43 characters."
