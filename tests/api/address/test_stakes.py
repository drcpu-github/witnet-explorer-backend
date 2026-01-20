import json


def test_stakes_cached_page_1(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    assert client.application.extensions["cache"].get(f"{address}_stakes") is not None
    response = client.get(f"/api/address/stakes?address={address}&page_size=5")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 10,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["stakes"][:5]


def test_stakes_cached_page_2(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    assert client.application.extensions["cache"].get(f"{address}_stakes") is not None
    response = client.get(f"/api/address/stakes?address={address}&page=2&page_size=5")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 10,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["stakes"][5:]


def test_stakes_not_cached_page_1(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    client.application.extensions["cache"].delete(f"{address}_stakes")
    assert client.application.extensions["cache"].get(f"{address}_stakes") is None
    response = client.get(f"/api/address/stakes?address={address}&page_size=5")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 10,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["stakes"][:5]


def test_stakes_not_cached_page_2(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    client.application.extensions["cache"].delete(f"{address}_stakes")
    assert client.application.extensions["cache"].get(f"{address}_stakes") is None
    response = client.get(f"/api/address/stakes?address={address}&page=2&page_size=5")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 10,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["stakes"][5:]
