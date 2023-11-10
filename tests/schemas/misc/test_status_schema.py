import pytest
from marshmallow import ValidationError

from schemas.misc.status_schema import DatabaseResponse, StatusResponse


def test_database_response_success():
    data = {
        "hash": "1e45f4982be86dd436bfbd393a9171b75e09d413eb2bf8970beed44d02d28a5a",
        "epoch": 1000,
    }
    DatabaseResponse().load(data)


def test_database_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DatabaseResponse().load(data)
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."


def test_status_response_success():
    data = {
        "message": "some backend services are down",
        "node_pool_message": {
            "status": "Synced",
            "epoch": 1000,
            "message": "fetched node pool status correctly",
        },
        "database_confirmed": {
            "hash": "1e45f4982be86dd436bfbd393a9171b75e09d413eb2bf8970beed44d02d28a5a",
            "epoch": 1000,
        },
        "database_unconfirmed": {
            "hash": "1e45f4982be86dd436bfbd393a9171b75e09d413eb2bf8970beed44d02d28a5a",
            "epoch": 1000,
        },
        "database_message": "",
        "expected_epoch": 1000,
    }
    StatusResponse().load(data)


def test_status_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        StatusResponse().load(data)
    assert err_info.value.messages["message"][0] == "Missing data for required field."
