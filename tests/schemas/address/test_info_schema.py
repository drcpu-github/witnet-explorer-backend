import pytest
from marshmallow import ValidationError

from schemas.address.info_schema import AddressInfoArgs, AddressInfoResponse


def test_address_info_failure_malformed_address():
    data = {
        "addresses": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfs,xit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6"
    }
    with pytest.raises(ValidationError) as err_info:
        AddressInfoArgs().load(data)
    assert (
        err_info.value.messages["addresses"][0]
        == "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfs: Address does not contain 42 characters, xit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6: Address does not start with wit1 string."
    )


def test_address_info_failure_too_many():
    data = {
        "addresses": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq,wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
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
            "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
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
            "address": "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6",
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
            "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
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
