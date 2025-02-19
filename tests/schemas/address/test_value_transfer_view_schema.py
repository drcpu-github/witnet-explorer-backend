import pytest
from marshmallow import ValidationError

from schemas.address.value_transfer_view_schema import ValueTransferView
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def value_transfer_view():
    return {
        "direction": "out",
        "epoch": 1,
        "fee": 1,
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "input_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "locked": False,
        "output_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "priority": 1,
        "weight": 493,
        "timestamp": 1_738_180_845,
        "value": 1,
        "confirmed": True,
    }


def test_value_transfer_view_success(value_transfer_view):
    ValueTransferView().load(value_transfer_view)


def test_value_transfer_view_failure_direction(value_transfer_view):
    value_transfer_view["direction"] = "me"
    with pytest.raises(ValidationError) as err_info:
        ValueTransferView().load(value_transfer_view)
    assert err_info.value.messages["direction"][0] == "Must be one of: in, out, self."


def test_value_transfer_view_failure_address(value_transfer_view):
    generic_address_test(
        value_transfer_view,
        (
            ("input_addresses",),
            ("output_addresses",),
        ),
        ValueTransferView,
    )


def test_value_transfer_view_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        ValueTransferView().load(data)
    assert len(err_info.value.messages) == 12
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["direction"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["value"][0] == "Missing data for required field."
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["priority"][0] == "Missing data for required field."
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["locked"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
