import json


def test_mempool_data_requests_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    assert cache.get("network_mempool_data_requests_1696014000_1696017600") is not None
    response = client.get(
        "/api/network/mempool?transaction_type=data_requests&start_epoch=2074399&stop_epoch=2074479"
    )
    assert response.status_code == 200
    assert (
        json.loads(response.data)
        == network_mempool["network_mempool_data_requests_1696014000_1696017600"]
    )


def test_mempool_data_requests_not_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    cache.delete("network_mempool_data_requests_1696014000_1696017600")
    assert cache.get("network_mempool_data_requests_1696014000_1696017600") is None
    response = client.get(
        "/api/network/mempool?transaction_type=data_requests&start_epoch=2074399&stop_epoch=2074479"
    )
    assert response.status_code == 200
    assert (
        json.loads(response.data)
        == network_mempool["network_mempool_data_requests_1696014000_1696017600"]
    )
    assert cache.get("network_mempool_data_requests_1696014000_1696017600") is not None


def test_mempool_value_transfers_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    assert (
        cache.get("network_mempool_value_transfers_1696014000_1696017600") is not None
    )
    response = client.get(
        "/api/network/mempool?transaction_type=value_transfers&start_epoch=2074399&stop_epoch=2074479"
    )
    assert response.status_code == 200
    assert (
        json.loads(response.data)
        == network_mempool["network_mempool_value_transfers_1696014000_1696017600"]
    )


def test_mempool_value_transfers_not_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    cache.delete("network_mempool_value_transfers_1696014000_1696017600")
    assert cache.get("network_mempool_value_transfers_1696014000_1696017600") is None
    response = client.get(
        "/api/network/mempool?transaction_type=value_transfers&start_epoch=2074399&stop_epoch=2074479"
    )
    assert response.status_code == 200
    assert (
        json.loads(response.data)
        == network_mempool["network_mempool_value_transfers_1696014000_1696017600"]
    )
    assert (
        cache.get("network_mempool_value_transfers_1696014000_1696017600") is not None
    )
