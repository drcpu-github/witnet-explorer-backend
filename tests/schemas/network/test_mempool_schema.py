import pytest
from marshmallow import ValidationError

from schemas.network.mempool_schema import NetworkMempoolArgs, NetworkMempoolResponse


def test_network_mempool_args_success():
    data = {"transaction_type": "data_requests"}
    network_mempool = NetworkMempoolArgs().load(data)
    assert network_mempool["transaction_type"] == "data_requests"

    data = {"transaction_type": "data_requests", "start_epoch": 900, "stop_epoch": 1000}
    network_mempool = NetworkMempoolArgs().load(data)
    assert len(network_mempool) == 4
    assert network_mempool["transaction_type"] == "data_requests"
    assert network_mempool["start_epoch"] == 900
    assert network_mempool["stop_epoch"] == 1000
    assert network_mempool["granularity"] == 60


def test_network_mempool_args_round_success():
    data = {"transaction_type": "data_requests"}
    network_mempool = NetworkMempoolArgs().load(data)
    assert network_mempool["transaction_type"] == "data_requests"

    data = {
        "transaction_type": "data_requests",
        "start_epoch": 900,
        "stop_epoch": 1000,
        "granularity": 100,
    }
    network_mempool = NetworkMempoolArgs().load(data)
    assert len(network_mempool) == 4
    assert network_mempool["transaction_type"] == "data_requests"
    assert network_mempool["start_epoch"] == 900
    assert network_mempool["stop_epoch"] == 1000
    assert network_mempool["granularity"] == 120


def test_network_mempool_args_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkMempoolArgs().load(data)
    assert (
        err_info.value.messages["transaction_type"][0]
        == "Missing data for required field."
    )


def test_network_mempool_args_failure_not_one_of():
    data = {"transaction_type": "data_request"}
    with pytest.raises(ValidationError) as err_info:
        NetworkMempoolArgs().load(data)
    assert (
        err_info.value.messages["transaction_type"][0]
        == "Must be one of: data_requests, value_transfers."
    )


def test_network_mempool_args_failure_order():
    data = {"transaction_type": "data_requests", "start_epoch": 1000, "stop_epoch": 900}
    with pytest.raises(ValidationError) as err_info:
        NetworkMempoolArgs().load(data)
    assert (
        err_info.value.messages["_schema"][0]
        == "The stop_epoch parameter is smaller than the start_epoch parameter."
    )


def test_network_mempool_response_success():
    data = {"timestamp": 1000, "fee": [1, 2], "amount": [2, 5]}
    NetworkMempoolResponse().load(data)


def test_network_mempool_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkMempoolResponse().load(data)
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["amount"][0] == "Missing data for required field."
