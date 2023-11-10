import pytest
from marshmallow import ValidationError

from schemas.include.base_transaction_schema import (
    BaseApiTransaction,
    BaseTransaction,
    TimestampComponent,
)


def test_base_transaction_success():
    data = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
    }
    BaseTransaction().load(data)


def test_base_transaction_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BaseTransaction().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."


@pytest.fixture
def timestamp():
    return {
        "epoch": 1,
        "timestamp": 1_602_666_090,
    }


def test_time_component_success(timestamp):
    TimestampComponent().load(timestamp)


def test_time_component_failure_timestamp(timestamp):
    timestamp["timestamp"] = 1_602_666_045
    with pytest.raises(ValidationError) as err_info:
        TimestampComponent().load(timestamp)
    assert err_info.value.messages["_schema"][0] == "Incorrect transaction timestamp."


def test_time_component_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TimestampComponent().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."


def test_base_api_transaction_success():
    data = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1_602_666_090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
    }
    BaseApiTransaction().load(data)


def test_base_api_transaction_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BaseApiTransaction().load(data)
    assert len(err_info.value.messages) == 6
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."


def test_base_api_transaction_failure_timestamp():
    data = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1_602_666_045,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
    }
    with pytest.raises(ValidationError) as err_info:
        BaseApiTransaction().load(data)
    assert err_info.value.messages["_schema"][0] == "Incorrect transaction timestamp."
