import pytest
from marshmallow import ValidationError

from schemas.address.info_schema import AddressInfoArgs, AddressInfoResponse


def test_address_info_failure_malformed_address():
    data = {
        "addresses": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4c,xit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh"
    }
    with pytest.raises(ValidationError) as err_info:
        AddressInfoArgs().load(data)
    assert (
        err_info.value.messages["addresses"][0]
        == "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4c: Testnet address does not contain 43 characters, xit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh: Address does not start with wit1 / twit1 string."
    )


def test_address_info_failure_too_many():
    data = {
        "addresses": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp,twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    }
    with pytest.raises(ValidationError) as err_info:
        AddressInfoArgs().load(data)
    assert (
        err_info.value.messages["addresses"][0]
        == "Length of comma-separated address list cannot be more than 10."
    )


def test_address_info_response_success():
    data = [
        {
            "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
            "label": "",
            "active": 0,
            "block": 0,
            "mint": 0,
            "value_transfer": 0,
            "data_request": 0,
            "commit": 0,
            "reveal": 0,
            "tally": 0,
        },
        {
            "address": "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh",
            "label": "",
            "active": 0,
            "block": 0,
            "mint": 0,
            "value_transfer": 0,
            "data_request": 0,
            "commit": 0,
            "reveal": 0,
            "tally": 0,
        },
    ]
    AddressInfoResponse(many=True).load(data)


def test_address_info_response_failure_missing():
    data = [
        {
            "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
            "label": "",
            "active": 0,
            "block": 0,
            "mint": 0,
        },
        {
            "value_transfer": 0,
            "data_request": 0,
            "commit": 0,
            "reveal": 0,
            "tally": 0,
        },
    ]
    with pytest.raises(ValidationError) as err_info:
        AddressInfoResponse(many=True).load(data)
    assert (
        err_info.value.messages[0]["value_transfer"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[0]["data_request"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages[0]["commit"][0] == "Missing data for required field."
    assert err_info.value.messages[0]["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages[0]["tally"][0] == "Missing data for required field."
    assert (
        err_info.value.messages[1]["address"][0] == "Missing data for required field."
    )
    assert err_info.value.messages[1]["label"][0] == "Missing data for required field."
    assert err_info.value.messages[1]["active"][0] == "Missing data for required field."
    assert err_info.value.messages[1]["block"][0] == "Missing data for required field."
    assert err_info.value.messages[1]["mint"][0] == "Missing data for required field."
