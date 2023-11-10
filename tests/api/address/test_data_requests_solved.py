import json


def test_data_requests_solved_cached_page_1(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    assert cache.get(f"{address}_data-requests-solved") is not None
    response = client.get(
        f"/api/address/data-requests-solved?address={address}&page_size=5"
    )
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 6,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert (
        json.loads(response.data) == address_data[address]["data-requests-solved"][:5]
    )


def test_data_requests_solved_cached_page_2(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    assert cache.get(f"{address}_data-requests-solved") is not None
    response = client.get(
        f"/api/address/data-requests-solved?address={address}&page=2&page_size=5"
    )
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 6,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert (
        json.loads(response.data) == address_data[address]["data-requests-solved"][5:]
    )


def test_data_requests_solved_not_cached_page_1(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    cache.delete(f"{address}_data-requests-solved")
    assert cache.get(f"{address}_data-requests-solved") is None
    response = client.get(
        f"/api/address/data-requests-solved?address={address}&page_size=5"
    )
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 6,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert (
        json.loads(response.data) == address_data[address]["data-requests-solved"][:5]
    )


def test_data_requests_solved_not_cached_page_2(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    cache = client.application.extensions["cache"]
    cache.delete(f"{address}_data-requests-solved")
    assert cache.get(f"{address}_data-requests-solved") is None
    response = client.get(
        f"/api/address/data-requests-solved?address={address}&page=2&page_size=5"
    )
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 6,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert (
        json.loads(response.data) == address_data[address]["data-requests-solved"][5:]
    )
