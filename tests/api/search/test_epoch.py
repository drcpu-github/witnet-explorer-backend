import json


def test_search_epoch_all_cached(client, blocks):
    cache = client.application.extensions["cache"]
    for block_hash, block in blocks.items():
        epoch = block["processed"]["api"]["block"]["details"]["epoch"]
        assert cache.get(f"{epoch}") is not None
        assert cache.get(block_hash) is not None

        response = client.get(f"/api/search/epoch?value={epoch}")
        assert response.status_code == 200
        assert response.headers["x-version"] == "2.0.0"
        assert json.loads(response.data) == block["processed"]["api"]


def test_search_epoch_only_epoch_cached(client, blocks):
    cache = client.application.extensions["cache"]
    for block_hash, block in blocks.items():
        epoch = block["processed"]["api"]["block"]["details"]["epoch"]
        assert cache.get(f"{epoch}") is not None
        cache.delete(block_hash)
        assert cache.get(block_hash) is None

        response = client.get(f"/api/search/epoch?value={epoch}")
        assert response.status_code == 200
        assert response.headers["x-version"] == "2.0.0"
        assert json.loads(response.data) == block["processed"]["api"]
        assert cache.get(block_hash) is not None


def test_search_epoch_not_cached(client, blocks):
    cache = client.application.extensions["cache"]
    for block_hash, block in blocks.items():
        epoch = block["processed"]["api"]["block"]["details"]["epoch"]
        cache.delete(f"{epoch}")
        assert cache.get(f"{epoch}") is None
        cache.delete(block_hash)
        assert cache.get(block_hash) is None

        response = client.get(f"/api/search/epoch?value={epoch}")
        assert response.status_code == 200
        assert response.headers["x-version"] == "2.0.0"
        assert json.loads(response.data) == block["processed"]["api"]
        assert cache.get(block_hash) is not None
        assert cache.get(f"{epoch}") is not None
