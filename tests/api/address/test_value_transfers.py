import json


def test_value_transfers_cached_page_1(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    cache = client.application.extensions["cache"]
    assert cache.get(f"{address}_value-transfers") is not None
    response = client.get(
        f"/api/address/value-transfers?address={address}&page_size=10"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 15,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][:10]


def test_value_transfers_cached_page_2(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    cache = client.application.extensions["cache"]
    assert cache.get(f"{address}_value-transfers") is not None
    response = client.get(
        f"/api/address/value-transfers?address={address}&page=2&page_size=10"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 15,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][10:]


def test_value_transfers_not_cached_page_1(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    cache = client.application.extensions["cache"]
    cache.delete(f"{address}_value-transfers")
    assert cache.get(f"{address}_value-transfers") is None
    response = client.get(
        f"/api/address/value-transfers?address={address}&page_size=10"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 15,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][:10]


def test_value_transfers_not_cached_page_2(client, address_data):
    address = "twit1pc8jzqph4t0md02e6fgwgsw26yll20p98c3pgh"
    cache = client.application.extensions["cache"]
    cache.delete(f"{address}_value-transfers")
    assert cache.get(f"{address}_value-transfers") is None
    response = client.get(
        f"/api/address/value-transfers?address={address}&page=2&page_size=10"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 15,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][10:]
