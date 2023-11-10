import pytest
from marshmallow import ValidationError

from schemas.address.mint_view_schema import MintView


@pytest.fixture
def mint_view():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1_602_666_090,
        "confirmed": True,
        "miner": "wit100000000000000000000000000000000r0v4g2",
        "output_value": 50000000000,
    }


def test_mmint_view_success(mint_view):
    MintView().load(mint_view)


def test_mmint_view_failure_address(mint_view):
    mint_view["miner"] = "wit100000000000000000000000000000000r0v4g"
    with pytest.raises(ValidationError) as err_info:
        MintView().load(mint_view)
    assert (
        err_info.value.messages["miner"][0] == "Address does not contain 42 characters."
    )


def test_mmint_view_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        MintView().load(data)
    assert len(err_info.value.messages) == 6
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_value"][0] == "Missing data for required field."
    )
