import pytest
from marshmallow import ValidationError

from schemas.search.data_request_history_schema import (
    DataRequestHistory,
    DataRequestHistoryEntry,
    DataRequestHistoryParameters,
    DataRequestHistoryRAD,
)


@pytest.fixture
def data_request_history_entry():
    return {
        "epoch": 1,
        "timestamp": 1602666090,
        "success": True,
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "witnesses": 10,
        "witness_reward": 1000,
        "collateral": 1e10,
        "consensus_percentage": 70,
        "num_errors": 0,
        "num_liars": 0,
        "result": "325647",
        "confirmed": True,
        "reverted": False,
    }


def test_data_request_history_entry_success(data_request_history_entry):
    DataRequestHistoryEntry().load(data_request_history_entry)


def test_data_request_history_entry_success_minimal(data_request_history_entry):
    del data_request_history_entry["witnesses"]
    del data_request_history_entry["witness_reward"]
    del data_request_history_entry["collateral"]
    del data_request_history_entry["consensus_percentage"]
    DataRequestHistoryEntry().load(data_request_history_entry)


def test_data_request_history_entry_failure_hash(data_request_history_entry):
    data_request_history_entry[
        "data_request"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistoryEntry().load(data_request_history_entry)
    assert (
        err_info.value.messages["data_request"][0] == "Hash is not a hexadecimal value."
    )


def test_data_request_history_entry_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistoryEntry().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_errors"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["num_liars"][0] == "Missing data for required field."
    assert err_info.value.messages["result"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."


@pytest.fixture
def data_request_history_parameters():
    return {
        "witnesses": 10,
        "witness_reward": 1000,
        "collateral": 1e10,
        "consensus_percentage": 70,
    }


def test_data_request_history_parameters_success(data_request_history_parameters):
    DataRequestHistoryParameters().load(data_request_history_parameters)


def test_data_request_history_parameters_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistoryParameters().load(data)
    assert len(err_info.value.messages) == 4
    assert err_info.value.messages["witnesses"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["witness_reward"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["consensus_percentage"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def data_request_history_RAD(data_request_retrieval):
    return {
        "retrieve": [data_request_retrieval],
        "aggregate": "filter(DeviationStandard, 1.5).reduce(AverageMedian)",
        "tally": "filter(DeviationStandard, 2.5).reduce(AverageMedian)",
    }


def test_data_request_history_rad_success(data_request_history_RAD):
    DataRequestHistoryRAD().load(data_request_history_RAD)


def test_data_request_history_rad_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistoryRAD().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["retrieve"][0] == "Missing data for required field."
    assert err_info.value.messages["aggregate"][0] == "Missing data for required field."
    assert err_info.value.messages["tally"][0] == "Missing data for required field."


@pytest.fixture
def data_request_history(
    data_request_history_entry,
    data_request_history_RAD,
    data_request_history_parameters,
):
    return {
        "hash_type": "RAD_bytes_hash",
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "history": [data_request_history_entry, data_request_history_entry],
        "num_data_requests": 2,
        "first_epoch": 1,
        "last_epoch": 10,
        "RAD_data": data_request_history_RAD,
        "RAD_bytes_hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "data_request_parameters": data_request_history_parameters,
    }


def test_data_request_history_success(data_request_history):
    DataRequestHistory().load(data_request_history)


def test_data_request_history_success_minimal(data_request_history):
    del data_request_history["RAD_bytes_hash"]
    del data_request_history["data_request_parameters"]
    DataRequestHistory().load(data_request_history)


def test_data_request_history_failure_type(data_request_history):
    data_request_history["hash_type"] = "RO_bytes_hash"
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistory().load(data_request_history)
    assert (
        err_info.value.messages["hash_type"][0]
        == "Must be one of: RAD_bytes_hash, DRO_bytes_hash."
    )


def test_data_request_history_failure_hash(data_request_history):
    data_request_history[
        "hash"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    data_request_history[
        "RAD_bytes_hash"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistory().load(data_request_history)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["hash"][0] == "Hash is not a hexadecimal value."
    assert (
        err_info.value.messages["RAD_bytes_hash"][0]
        == "Hash is not a hexadecimal value."
    )


def test_data_request_history_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestHistory().load(data)
    assert len(err_info.value.messages) == 7
    assert err_info.value.messages["hash_type"][0] == "Missing data for required field."
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["history"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["num_data_requests"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["first_epoch"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["last_epoch"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["RAD_data"][0] == "Missing data for required field."
