import json


def test_blockchain_page_1_cached(client, blockchain):
    cache = client.application.extensions["cache"]
    assert cache.get("blockchain_page-1_page-size-50") is not None
    response = client.get("/api/network/blockchain?page=1")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 2024100,
        "total_pages": 40482,
        "first_page": 1,
        "last_page": 40482,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == blockchain["blockchain_page-1_page-size-50"]


def test_blockchain_page_246_cached(client, blockchain):
    cache = client.application.extensions["cache"]
    assert cache.get("blockchain_page-246_page-size-50") is not None
    response = client.get("/api/network/blockchain?page=246")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 2024100,
        "total_pages": 40482,
        "first_page": 1,
        "last_page": 40482,
        "page": 246,
        "previous_page": 245,
        "next_page": 247,
    }
    assert json.loads(response.data) == blockchain["blockchain_page-246_page-size-50"]


def test_blockchain_page_1_not_cached(client, blockchain):
    cache = client.application.extensions["cache"]
    cache.delete("blockchain_page-1_page-size-50")
    assert cache.get("blockchain_page-1_page-size-50") is None
    response = client.get("/api/network/blockchain?page=1")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 2024100,
        "total_pages": 40482,
        "first_page": 1,
        "last_page": 40482,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == blockchain["blockchain_page-1_page-size-50"]
    assert cache.get("blockchain_page-1_page-size-50") is not None


def test_blockchain_page_246_not_cached(client, blockchain):
    cache = client.application.extensions["cache"]
    cache.delete("blockchain_page-246_page-size-50")
    assert cache.get("blockchain_page-246_page-size-50") is None
    response = client.get("/api/network/blockchain?page=246")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 2024100,
        "total_pages": 40482,
        "first_page": 1,
        "last_page": 40482,
        "page": 246,
        "previous_page": 245,
        "next_page": 247,
    }
    assert json.loads(response.data) == blockchain["blockchain_page-246_page-size-50"]
    assert cache.get("blockchain_page-246_page-size-50") is not None


def test_blockchain_page_2_not_cached_all_reverted(client):
    cache = client.application.extensions["cache"]
    assert cache.get("blockchain_page-2_page-size-50") is None
    response = client.get("/api/network/blockchain?page=2")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 2024100,
        "total_pages": 40482,
        "first_page": 1,
        "last_page": 40482,
        "page": 2,
        "previous_page": 1,
        "next_page": 3,
    }
    assert json.loads(response.data) == {
        "blockchain": [],
        "reverted": list(range(2024001, 2024051)),
        "total_epochs": 2024100,
    }
    assert cache.get("blockchain_page-2_page-size-50") is not None
