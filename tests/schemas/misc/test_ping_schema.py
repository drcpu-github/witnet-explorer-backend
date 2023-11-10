import pytest
from marshmallow import ValidationError

from schemas.misc.ping_schema import PingResponse


def test_ping_response_success():
    data = {"response": "pong"}
    PingResponse().load(data)


def test_ping_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        PingResponse().load(data)
    assert err_info.value.messages["response"][0] == "Missing data for required field."


def test_ping_response_failure_not_equals():
    data = {"response": "pon"}
    with pytest.raises(ValidationError) as err_info:
        PingResponse().load(data)
    assert err_info.value.messages["response"][0] == "Must be equal to pong."
