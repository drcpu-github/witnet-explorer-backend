import pytest
from marshmallow import ValidationError

from schemas.address.details_view_schema import DetailsView


def test_address_details_response_success():
    data = {
        "balance": 0,
        "staked_validator": 0,
        "staked_withdrawer": 0,
        "label": "label",
    }
    DetailsView().load(data)


def test_address_details_response_no_label_success():
    data = {
        "balance": 0,
        "staked_validator": 0,
        "staked_withdrawer": 0,
        "label": None,
    }
    DetailsView().load(data)


def test_address_details_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DetailsView().load(data)
    assert len(err_info.value.messages) == 4
    assert err_info.value.messages["balance"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["staked_validator"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["staked_withdrawer"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["label"][0] == "Missing data for required field."
