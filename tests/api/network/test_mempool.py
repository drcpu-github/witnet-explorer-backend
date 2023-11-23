import json

from api.blueprints.network.mempool_blueprint import (
    build_priority_histogram,
    interpolate_and_transform,
)


def test_mempool_data_requests_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    cache_key = "network_mempool_data_requests_1696016115_1696017015_60"
    assert cache.get(cache_key) is not None
    response = client.get(
        "/api/network/mempool?transaction_type=data_requests&start_epoch=2074446&stop_epoch=2074466"
    )
    assert response.status_code == 200
    assert json.loads(response.data) == network_mempool[cache_key]


def test_mempool_data_requests_not_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    cache_key = "network_mempool_data_requests_1696016115_1696017015_60"
    cache.delete(cache_key)
    assert cache.get(cache_key) is None
    response = client.get(
        "/api/network/mempool?transaction_type=data_requests&start_epoch=2074446&stop_epoch=2074466"
    )
    assert response.status_code == 200
    assert json.loads(response.data) == network_mempool[cache_key]
    assert cache.get(cache_key) is not None


def test_mempool_value_transfers_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    cache_key = "network_mempool_value_transfers_1696016115_1696017015_60"
    assert cache.get(cache_key) is not None
    response = client.get(
        "/api/network/mempool?transaction_type=value_transfers&start_epoch=2074446&stop_epoch=2074466"
    )
    assert response.status_code == 200
    assert json.loads(response.data) == network_mempool[cache_key]


def test_mempool_value_transfers_not_cached(client, network_mempool):
    cache = client.application.extensions["cache"]
    cache_key = "network_mempool_value_transfers_1696016115_1696017015_60"
    cache.delete(cache_key)
    assert cache.get(cache_key) is None
    response = client.get(
        "/api/network/mempool?transaction_type=value_transfers&start_epoch=2074446&stop_epoch=2074466"
    )
    assert response.status_code == 200
    assert json.loads(response.data) == network_mempool[cache_key]
    assert cache.get(cache_key) is not None


def test_mempool_histogram():
    fees = [0, 100, 200, 300, 300, 400]
    weights = [100, 200, 50, 40, 70, 100]
    expected_histogram = {0: 1, 1: 1, 4: 3, 8: 1}
    histogram = build_priority_histogram(fees, weights)
    assert histogram == expected_histogram


def test_mempool_interpolation():
    start_timestamp = 120
    stop_timestamp = 540
    data = [
        [
            210,
            [0, 100, 200, 300, 300, 400],
            [100, 200, 50, 40, 70, 100],
        ],  # {0: 1, 1: 1, 4: 3, 8: 1}
        [
            225,
            [0, 100, 200, 300],
            [100, 200, 50, 70],
        ],  # {0: 1, 1: 1, 4: 2}
        [240, [], []],
        [300, [], []],
        [315, [], []],
        [
            435,
            [0, 100, 200, 300, 300, 400],
            [100, 200, 50, 40, 70, 100],
        ],  # {0: 1, 1: 1, 4: 3, 8: 1}
        [
            450,
            [0, 100, 200, 300, 300, 400],
            [100, 200, 50, 40, 70, 100],
        ],  # {0: 1, 1: 1, 4: 3, 8: 1}
    ]

    sample_rate_60 = 4
    expected_interpolation_60 = [
        {"timestamp": 120, "fee": [], "amount": []},
        {"timestamp": 180, "fee": [], "amount": []},
        {"timestamp": 240, "fee": [0, 1, 4, 8], "amount": [1, 1, 1, 1]},
        {"timestamp": 300, "fee": [], "amount": []},
        {"timestamp": 360, "fee": [], "amount": []},
        {"timestamp": 420, "fee": [], "amount": []},
        {"timestamp": 480, "fee": [0, 1, 4, 8], "amount": [1, 1, 2, 1]},
    ]
    interpolated_histogram_60 = interpolate_and_transform(
        start_timestamp,
        stop_timestamp,
        data,
        sample_rate_60,
        60,
    )
    assert expected_interpolation_60 == interpolated_histogram_60

    sample_rate_120 = 8
    expected_interpolation_120 = [
        {"timestamp": 120, "fee": [], "amount": []},
        {"timestamp": 240, "fee": [0, 1, 4, 8], "amount": [1, 1, 1, 1]},
        {"timestamp": 360, "fee": [], "amount": []},
        {"timestamp": 480, "fee": [0, 1, 4, 8], "amount": [1, 1, 1, 1]},
    ]
    interpolated_histogram_120 = interpolate_and_transform(
        start_timestamp,
        stop_timestamp,
        data,
        sample_rate_120,
        120,
    )
    assert expected_interpolation_120 == interpolated_histogram_120
