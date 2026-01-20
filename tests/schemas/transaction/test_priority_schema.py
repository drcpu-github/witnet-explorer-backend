import pytest
from marshmallow import ValidationError

from schemas.transaction.priority_schema import (
    PriorityTime,
    TransactionPriority,
    TransactionPriorityArgs,
    TransactionPriorityResponse,
)


def test_transaction_priority_args_success():
    data = {}
    priority = TransactionPriorityArgs().load(data)
    assert priority["key"] == "all"

    data = {"key": "drt"}
    priority = TransactionPriorityArgs().load(data)
    assert priority["key"] == "drt"


def test_transaction_priority_args_failure_one_of():
    data = {"key": "al"}
    with pytest.raises(ValidationError) as err_info:
        TransactionPriorityArgs().load(data)
    assert err_info.value.messages["key"][0] == "Must be one of: all, drt, vtt, st, ut."


@pytest.fixture
def priority_time():
    return {"priority": 5, "time_to_block": 300}


def test_priority_time_success(priority_time):
    PriorityTime().load(priority_time)


@pytest.fixture
def transaction_priority():
    return {
        "stinky": {"priority": 1, "time_to_block": 625},
        "low": {"priority": 2, "time_to_block": 125},
        "medium": {"priority": 3, "time_to_block": 25},
        "high": {"priority": 4, "time_to_block": 5},
        "opulent": {"priority": 5, "time_to_block": 1},
    }


def test_transaction_priority_success(transaction_priority):
    TransactionPriority().load(transaction_priority)


def test_transaction_priority_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TransactionPriority().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["stinky"][0] == "Missing data for required field."
    assert err_info.value.messages["low"][0] == "Missing data for required field."
    assert err_info.value.messages["medium"][0] == "Missing data for required field."
    assert err_info.value.messages["high"][0] == "Missing data for required field."
    assert err_info.value.messages["opulent"][0] == "Missing data for required field."


def test_transaction_priority_response_success(transaction_priority):
    data = {
        "drt": transaction_priority,
        "vtt": transaction_priority,
        "st": transaction_priority,
        "ut": transaction_priority,
    }
    TransactionPriorityResponse().load(data)

    data = {
        "drt": transaction_priority,
    }
    TransactionPriorityResponse().load(data)

    data = {
        "vtt": transaction_priority,
    }
    TransactionPriorityResponse().load(data)

    data = {
        "st": transaction_priority,
    }
    TransactionPriorityResponse().load(data)

    data = {
        "ut": transaction_priority,
    }
    TransactionPriorityResponse().load(data)


def test_transaction_priority_response_failure_key(transaction_priority):
    data = {
        "xt": transaction_priority,
    }
    with pytest.raises(ValidationError) as err_info:
        TransactionPriorityResponse().load(data)
    assert err_info.value.messages["xt"][0] == "Unknown field."
