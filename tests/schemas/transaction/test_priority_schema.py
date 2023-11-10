import pytest
from marshmallow import ValidationError

from schemas.transaction.priority_schema import (
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
    assert err_info.value.messages["key"][0] == "Must be one of: all, drt, vtt."


def test_transaction_priority_response_success():
    data = {"priority": 5.0, "time_to_block": 300}
    TransactionPriority().load(data)

    data = {
        "drt_high": {"priority": 5.0, "time_to_block": 300},
        "drt_low": {"priority": 1.25, "time_to_block": 3600},
        "drt_medium": {"priority": 2.5, "time_to_block": 900},
        "drt_opulent": {"priority": 10.0, "time_to_block": 60},
        "drt_stinky": {"priority": 0.625, "time_to_block": 21600},
        "vtt_high": {"priority": 5.0, "time_to_block": 300},
        "vtt_low": {"priority": 1.25, "time_to_block": 3600},
        "vtt_medium": {"priority": 2.5, "time_to_block": 900},
        "vtt_opulent": {"priority": 10.0, "time_to_block": 60},
        "vtt_stinky": {"priority": 0.625, "time_to_block": 21600},
    }
    TransactionPriorityResponse().load(data)
