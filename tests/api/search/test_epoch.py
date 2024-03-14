import json


def test_search_epoch_all_cached(client, blocks):
    cache = client.application.extensions["cache"]
    assert cache.get("2002561") is not None
    hash_value = "6bf0bbafb380cced8134684c31028af6701905c223f4513f0c8d871c1beb8923"
    assert cache.get(hash_value) is not None
    response = client.get("/api/search/epoch?value=2002561")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == blocks[hash_value]["cache"]


def test_search_epoch_only_epoch_cached(client, blocks):
    cache = client.application.extensions["cache"]
    assert cache.get("2002561") is not None
    hash_value = "6bf0bbafb380cced8134684c31028af6701905c223f4513f0c8d871c1beb8923"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get("/api/search/epoch?value=2002561")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == blocks[hash_value]["cache"]


def test_search_epoch_not_cached(client, blocks):
    cache = client.application.extensions["cache"]
    epoch = "2002561"
    cache.delete(epoch)
    assert cache.get(epoch) is None
    hash_value = "6bf0bbafb380cced8134684c31028af6701905c223f4513f0c8d871c1beb8923"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get("/api/search/epoch?value=2002561")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == blocks[hash_value]["cache"]
    assert cache.get(hash_value) is not None
    assert cache.get(epoch) is not None
