import json


def test_balances_page_1(client, balances):
    response = client.get("/api/network/balances?page=1&page_size=5")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 16,
        "total_pages": 4,
        "first_page": 1,
        "last_page": 4,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == {
        "balances": balances["balance-list_0-1000"]["balances"][:5],
        "total_items": 16,
        "total_balance_sum": 327182938,
        "last_updated": 1697007634,
    }


def test_balances_page_2(client, balances):
    response = client.get("/api/network/balances?page=2&page_size=5")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 16,
        "total_pages": 4,
        "first_page": 1,
        "last_page": 4,
        "page": 2,
        "previous_page": 1,
        "next_page": 3,
    }
    assert json.loads(response.data) == {
        "balances": balances["balance-list_0-1000"]["balances"][5:10],
        "total_items": 16,
        "total_balance_sum": 327182938,
        "last_updated": 1697007634,
    }


def test_balances_page_201(client, balances):
    response = client.get("/api/network/balances?page=201&page_size=5")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 16,
        "total_pages": 4,
        "first_page": 1,
        "last_page": 4,
    }
    assert json.loads(response.data) == {
        "balances": balances["balance-list_1000-2000"]["balances"][:5],
        "total_items": 16,
        "total_balance_sum": 327182938,
        "last_updated": 1697007634,
    }


def test_balances_not_cached(client):
    client.application.extensions["cache"].delete("balance-list_0-1000")
    assert client.application.extensions["cache"].get("balance-list_0-1000") is None
    response = client.get("/api/network/balances?page=1")
    assert response.status_code == 404
    assert (
        json.loads(response.data)["message"]
        == "Could not find required list of balances in memcached cache."
    )
