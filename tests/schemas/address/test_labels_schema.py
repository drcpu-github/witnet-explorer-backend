import pytest
from marshmallow import ValidationError

from schemas.address.labels_schema import AddressLabelResponse


def test_address_label_response_success():
    data = [
        {
            "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
            "label": "The best address",
        },
        {
            "address": "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6",
            "label": "The second best address",
        },
    ]
    AddressLabelResponse(many=True).load(data)


def test_address_label_response_failure_missing():
    data = [
        {
            "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
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
