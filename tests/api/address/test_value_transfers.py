import json


def test_value_transfers_cached_page_1(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    assert cache.get(f"{address}_value-transfers") is not None
    response = client.get(f"/api/address/value-transfers?address={address}&page_size=3")
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 5,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][:3]


def test_value_transfers_cached_page_2(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    assert cache.get(f"{address}_value-transfers") is not None
    response = client.get(
        f"/api/address/value-transfers?address={address}&page=2&page_size=3"
    )
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 5,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][3:]


def test_value_transfers_not_cached_page_1(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    cache.delete(f"{address}_value-transfers")
    assert cache.get(f"{address}_value-transfers") is None
    response = client.get(f"/api/address/value-transfers?address={address}&page_size=3")
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 5,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][:3]


def test_value_transfers_not_cached_page_2(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    cache.delete(f"{address}_value-transfers")
    assert cache.get(f"{address}_value-transfers") is None
    response = client.get(
        f"/api/address/value-transfers?address={address}&page=2&page_size=3"
    )
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 5,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["value-transfers"][3:]
