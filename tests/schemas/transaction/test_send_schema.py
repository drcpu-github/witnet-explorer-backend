import pytest
from marshmallow import ValidationError

from schemas.transaction.send_schema import SendArgs, SendResponse


def test_send_args_success():
    data = {}
    send_args = SendArgs().load(data)
    assert not send_args["test"]

    data = {"test": True}
    send_args = SendArgs().load(data)
    assert send_args["test"]


def test_send_response_success():
    data = {"result": "Succesfully sent value transfer."}
    SendResponse().load(data)


def test_send_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        SendResponse().load(data)
    assert len(err_info.value.messages) == 1
    assert err_info.value.messages["result"][0] == "Missing data for required field."
