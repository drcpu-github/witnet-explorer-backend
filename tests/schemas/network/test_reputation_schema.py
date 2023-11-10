import pytest
from marshmallow import ValidationError

from schemas.network.reputation_schema import NetworkReputationResponse, Reputation


def test_reputation_success():
    data = {
        "address": "wit10fqk62my0s32crswvzk34s70s4udefd2lr8vn0",
        "reputation": 9522,
        "eligibility": 0.1,
    }
    Reputation().load(data)


def test_reputation_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        Reputation().load(data)
    assert err_info.value.messages["address"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["reputation"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["eligibility"][0] == "Missing data for required field."
    )


def test_network_reputation_response_success():
    data = {
        "reputation": [
            {
                "address": "wit10fqk62my0s32crswvzk34s70s4udefd2lr8vn0",
                "reputation": 9522,
                "eligibility": 0.1,
            },
            {
                "address": "wit180kesj4frn3gfudhf6xr6n5w7w6vy35p4xt4rq",
                "reputation": 8663,
                "eligibility": 0.1,
            },
            {
                "address": "wit18m3tuqcppxk7fdwlld6lvf6k2z4re8jfl0zxz8",
                "reputation": 7467,
                "eligibility": 0.1,
            },
        ],
        "total_reputation": 1054523,
        "last_updated": 1691307721,
    }
    NetworkReputationResponse().load(data)


def test_network_reputation_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkReputationResponse().load(data)
    assert (
        err_info.value.messages["reputation"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["total_reputation"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["last_updated"][0] == "Missing data for required field."
    )
