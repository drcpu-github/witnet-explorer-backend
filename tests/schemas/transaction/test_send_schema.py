import pytest
from marshmallow import ValidationError

from schemas.transaction.send_schema import ValueTransferArgs, ValueTransferResponse


def test_value_transfer_args_success(value_transfer):
    data = {}
    value_transfer_args = ValueTransferArgs().load(data)
    assert not value_transfer_args["test"]

    data = {"test": True}
    value_transfer_args = ValueTransferArgs().load(data)
    assert value_transfer_args["test"]


def test_value_transfer_response_success(value_transfer):
    data = {"result": "Succesfully sent value transfer."}
    ValueTransferResponse().load(data)


def test_value_transfer_response_failure_missing(value_transfer):
    data = {}
    with pytest.raises(ValidationError) as err_info:
        ValueTransferResponse().load(data)
    assert len(err_info.value.messages) == 1
    assert err_info.value.messages["result"][0] == "Missing data for required field."
