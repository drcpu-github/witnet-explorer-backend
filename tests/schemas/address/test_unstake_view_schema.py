import pytest
from marshmallow import ValidationError

from schemas.address.unstake_view_schema import UnstakeView
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def unstake_view():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1_738_180_845,
        "direction": "in",
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "fee": 1,
        "unstake_value": 50000000000,
        "confirmed": True,
    }


def test_unstake_view_success(unstake_view):
    UnstakeView().load(unstake_view)


def test_unstake_view_failure_direction(unstake_view):
    unstake_view["direction"] = "inout"
    with pytest.raises(ValidationError) as err_info:
        UnstakeView().load(unstake_view)
    assert err_info.value.messages["direction"][0] == "Must be one of: in, out, self."


def test_unstake_view_failure_address(unstake_view):
    generic_address_test(
        unstake_view,
        ("validator", "withdrawer"),
        UnstakeView,
    )


def test_unstake_view_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        UnstakeView().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert err_info.value.messages["direction"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["unstake_value"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
