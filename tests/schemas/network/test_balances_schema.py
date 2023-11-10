import pytest
from marshmallow import ValidationError

from schemas.network.balances_schema import AddressBalance, NetworkBalancesResponse


def test_address_balance_success():
    data = {
        "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
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
                "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
                "balance": 100,
                "label": "The best address",
            },
            {
                "address": "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6",
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
