import json


def test_blockchain_page_1662_cached(client, blockchain):
    cache = client.application.extensions["cache"]
    assert cache.get("blockchain_page-1662_page-size-50") is not None
    response = client.get("/api/network/blockchain?page=1662")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 115910,
        "total_pages": 2319,
        "first_page": 1,
        "last_page": 2319,
        "page": 1662,
        "previous_page": 1661,
        "next_page": 1663,
    }
    assert json.loads(response.data) == blockchain["blockchain_page-1662_page-size-50"]


def test_blockchain_page_1662_not_cached(client, blockchain):
    cache = client.application.extensions["cache"]
    cache.delete("blockchain_page-1662_page-size-50")
    assert cache.get("blockchain_page-1662_page-size-50") is None
    response = client.get("/api/network/blockchain?page=1662")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 115910,
        "total_pages": 2319,
        "first_page": 1,
        "last_page": 2319,
        "page": 1662,
        "previous_page": 1661,
        "next_page": 1663,
    }
    assert json.loads(response.data) == blockchain["blockchain_page-1662_page-size-50"]
    assert cache.get("blockchain_page-1662_page-size-50") is not None
