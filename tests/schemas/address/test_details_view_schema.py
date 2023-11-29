import pytest
from marshmallow import ValidationError

from schemas.address.details_view_schema import DetailsView


def test_address_details_response_success():
    data = {
        "balance": 0,
        "reputation": 0,
        "eligibility": 0,
        "total_reputation": 0,
        "label": "drcpu0",
    }
    DetailsView().load(data)


def test_address_details_response_no_label_success():
    data = {
        "balance": 0,
        "reputation": 0,
        "eligibility": 0,
        "total_reputation": 0,
        "label": None,
    }
    DetailsView().load(data)


def test_address_details_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DetailsView().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["balance"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["reputation"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["eligibility"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["total_reputation"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["label"][0] == "Missing data for required field."
