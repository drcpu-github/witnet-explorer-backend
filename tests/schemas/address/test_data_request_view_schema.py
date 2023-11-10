import pytest
from marshmallow import ValidationError

from schemas.address.data_request_view_schema import (
    DataRequestCreatedView,
    DataRequestSolvedView,
)


def test_address_data_request_solved_response_success():
    data = [
        {
            "hash": "2bde8d42ee145837af6d96c6f39301a8e268ce4000ec8206730ba4fefb7f279f",
            "success": True,
            "epoch": 1,
            "timestamp": 1,
            "collateral": 1,
            "witness_reward": 2,
            "reveal": "",
            "error": False,
            "liar": False,
        },
        {
            "hash": "deb7803196cb03fed0747820e97605bddcecfc3c62188ab0916122a73b4cf972",
            "success": False,
            "epoch": 2,
            "timestamp": 1,
            "collateral": 1,
            "witness_reward": 2,
            "reveal": "",
            "error": False,
            "liar": True,
        },
    ]
    DataRequestSolvedView(many=True).load(data)


def test_address_data_request_solved_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestSolvedView().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["witness_reward"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["error"][0] == "Missing data for required field."
    assert err_info.value.messages["liar"][0] == "Missing data for required field."


def test_address_data_request_created_response_success():
    data = [
        {
            "hash": "2bde8d42ee145837af6d96c6f39301a8e268ce4000ec8206730ba4fefb7f279f",
            "success": True,
            "epoch": 1,
            "timestamp": 1,
            "total_fee": 1000,
            "witnesses": 10,
            "collateral": 1,
            "consensus_percentage": 2,
            "num_errors": 0,
            "num_liars": 0,
            "result": "",
        },
        {
            "hash": "deb7803196cb03fed0747820e97605bddcecfc3c62188ab0916122a73b4cf972",
            "success": False,
            "epoch": 2,
            "timestamp": 45,
            "total_fee": 1000,
            "witnesses": 10,
            "collateral": 1,
            "consensus_percentage": 2,
            "num_errors": 0,
            "num_liars": 10,
            "result": "",
        },
    ]
    DataRequestCreatedView(many=True).load(data)


def test_address_data_request_created_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestCreatedView().load(data)
    assert len(err_info.value.messages) == 11
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["total_fee"][0] == "Missing data for required field."
    assert err_info.value.messages["witnesses"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["consensus_percentage"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_errors"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["num_liars"][0] == "Missing data for required field."
    assert err_info.value.messages["result"][0] == "Missing data for required field."
