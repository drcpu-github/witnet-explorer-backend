import pytest
from marshmallow import ValidationError

from schemas.search.epoch_schema import SearchEpochArgs


def test_epoch_success():
    data = {"value": "1000000"}
    SearchEpochArgs().load(data)


def test_epoch_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        SearchEpochArgs().load(data)
    assert err_info.value.messages["value"][0] == "Missing data for required field."


def test_epoch_failure_type():
    data = {"value": "abc"}
    with pytest.raises(ValidationError) as err_info:
        SearchEpochArgs().load(data)
    assert err_info.value.messages["value"][0] == "Not a valid integer."
