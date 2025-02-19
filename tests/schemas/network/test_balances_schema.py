import pytest
from marshmallow import ValidationError

from schemas.network.balances_schema import AddressBalance, NetworkBalancesResponse


def test_address_balance_success():
    data = {
        "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        "balance": 100,
        "label": "The best address",
    }
    AddressBalance().load(data)


def test_address_balance_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        AddressBalance().load(data)
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert err_info.value.messages["balance"][0] == "Missing data for required field."


def test_network_balances_success():
    data = {
        "balances": [
            {
                "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
                "balance": 100,
                "label": "The best address",
            },
            {
                "address": "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh",
                "balance": 10,
                "label": "The second best address",
            },
            {
                "address": "wit1drcpu3x42y5vp7w3pe203xrwpnth2pnt6c0dm9",
                "balance": 1,
                "label": "The third best address",
            },
        ],
        "total_items": 100,
        "total_balance_sum": 1000,
        "last_updated": 0,
    }
    NetworkBalancesResponse().load(data)


def test_network_balances_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkBalancesResponse().load(data)
    assert err_info.value.messages["balances"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["total_items"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["total_balance_sum"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["last_updated"][0] == "Missing data for required field."
    )
