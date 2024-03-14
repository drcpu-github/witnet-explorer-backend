import json


def test_mempool_cached_all(client, mempool):
    assert client.application.extensions["cache"].get("transaction_mempool") is not None
    response = client.get("/api/transaction/mempool?type=all")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == mempool


def test_mempool_cached_drt(client, mempool):
    assert client.application.extensions["cache"].get("transaction_mempool") is not None
    response = client.get("/api/transaction/mempool?type=data_requests")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"data_request": mempool["data_request"]}


def test_mempool_cached_vtt(client, mempool):
    assert client.application.extensions["cache"].get("transaction_mempool") is not None
    response = client.get("/api/transaction/mempool?type=value_transfers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"value_transfer": mempool["value_transfer"]}


def test_mempool_not_cached(client, mempool):
    client.application.extensions["cache"].delete("transaction_mempool")
    assert client.application.extensions["cache"].get("transaction_mempool") is None
    response = client.get("/api/transaction/mempool?type=all")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == mempool
    assert client.application.extensions["cache"].get("transaction_mempool") is not None
