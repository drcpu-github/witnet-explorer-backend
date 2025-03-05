import json


def test_mints_cached_page_1(client, address_data):
    address = "twit1a64uygwh3s650l88mexhw4kgnue45cuj83ghf5"
    assert client.application.extensions["cache"].get(f"{address}_mints") is not None
    response = client.get(f"/api/address/mints?address={address}&page_size=10")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 6,
        "total_pages": 1,
        "first_page": 1,
        "last_page": 1,
        "page": 1,
    }
    assert json.loads(response.data) == address_data[address]["mints"][:10]


def test_mints_not_cached_page_1(client, address_data):
    address = "twit1a64uygwh3s650l88mexhw4kgnue45cuj83ghf5"
    client.application.extensions["cache"].delete(f"{address}_mints")
    assert client.application.extensions["cache"].get(f"{address}_mints") is None
    response = client.get(f"/api/address/mints?address={address}&page_size=10")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 6,
        "total_pages": 1,
        "first_page": 1,
        "last_page": 1,
        "page": 1,
    }
    assert json.loads(response.data) == address_data[address]["mints"][:10]
