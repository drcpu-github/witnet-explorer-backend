import json


def test_unstakes_cached_page_1(client, address_data):
    address = "twit1f0am8c97q2ygkz3q6jyd2x29s8zaxqlxcqltxx"
    assert client.application.extensions["cache"].get(f"{address}_unstakes") is not None
    response = client.get(f"/api/address/unstakes?address={address}&page_size=5")
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
    assert json.loads(response.data) == address_data[address]["unstakes"][:5]


def test_unstakes_cached_page_2(client, address_data):
    address = "twit1f0am8c97q2ygkz3q6jyd2x29s8zaxqlxcqltxx"
    assert client.application.extensions["cache"].get(f"{address}_unstakes") is not None
    response = client.get(f"/api/address/unstakes?address={address}&page=2&page_size=5")
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
    assert json.loads(response.data) == address_data[address]["unstakes"][5:]


def test_unstakes_not_cached_page_1(client, address_data):
    address = "twit1f0am8c97q2ygkz3q6jyd2x29s8zaxqlxcqltxx"
    client.application.extensions["cache"].delete(f"{address}_unstakes")
    assert client.application.extensions["cache"].get(f"{address}_unstakes") is None
    response = client.get(f"/api/address/unstakes?address={address}&page_size=5")
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
    assert json.loads(response.data) == address_data[address]["unstakes"][:5]


def test_unstakes_not_cached_page_2(client, address_data):
    address = "twit1f0am8c97q2ygkz3q6jyd2x29s8zaxqlxcqltxx"
    client.application.extensions["cache"].delete(f"{address}_unstakes")
    assert client.application.extensions["cache"].get(f"{address}_unstakes") is None
    response = client.get(f"/api/address/unstakes?address={address}&page=2&page_size=5")
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
    assert json.loads(response.data) == address_data[address]["unstakes"][5:]
