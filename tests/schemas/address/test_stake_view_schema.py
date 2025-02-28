import pytest
from marshmallow import ValidationError

from schemas.address.stake_view_schema import StakeView
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def stake_view():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1_738_180_845,
        "direction": "in",
        "validator": "wit100000000000000000000000000000000r0v4g2",
        "withdrawer": "wit100000000000000000000000000000000r0v4g2",
        "input_value": 50000000000,
        "stake_value": 100000000000,
        "confirmed": True,
    }


def test_stake_view_success(stake_view):
    StakeView().load(stake_view)


def test_stake_view_failure_direction(stake_view):
    stake_view["direction"] = "inout"
    with pytest.raises(ValidationError) as err_info:
        StakeView().load(stake_view)
    assert err_info.value.messages["direction"][0] == "Must be one of: in, out."


def test_stake_view_failure_address(stake_view):
    generic_address_test(
        stake_view,
        ("validator", "withdrawer"),
        StakeView,
    )


def test_stake_view_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        StakeView().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["direction"][0] == "Missing data for required field."
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_value"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["stake_value"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
