import pytest
from marshmallow import ValidationError

from schemas.address.labels_schema import AddressLabelResponse


def test_address_label_response_success():
    data = [
        {
            "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
            "label": "The best address",
        },
        {
            "address": "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh",
            "label": "The second best address",
        },
    ]
    AddressLabelResponse(many=True).load(data)


def test_address_label_response_failure_missing():
    data = [
        {
            "address": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        },
        {
            "label": "The second best address",
        },
    ]
    with pytest.raises(ValidationError) as err_info:
        AddressLabelResponse(many=True).load(data)
    assert err_info.value.messages[0]["label"][0] == "Missing data for required field."
    assert (
        err_info.value.messages[1]["address"][0] == "Missing data for required field."
    )
