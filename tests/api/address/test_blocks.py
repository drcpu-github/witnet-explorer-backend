import json


def test_blocks_cached_page_1(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    assert client.application.extensions["cache"].get(f"{address}_blocks") is not None
    response = client.get(f"/api/address/blocks?address={address}&page_size=5")
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 7,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["blocks"][:5]


def test_blocks_cached_page_2(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    assert client.application.extensions["cache"].get(f"{address}_blocks") is not None
    response = client.get(f"/api/address/blocks?address={address}&page=2&page_size=5")
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 7,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["blocks"][5:]


def test_blocks_not_cached_page_1(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    client.application.extensions["cache"].delete(f"{address}_blocks")
    assert client.application.extensions["cache"].get(f"{address}_blocks") is None
    response = client.get(f"/api/address/blocks?address={address}&page_size=5")
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 7,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    assert json.loads(response.data) == address_data[address]["blocks"][:5]


def test_blocks_not_cached_page_2(client, address_data):
    address = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    client.application.extensions["cache"].delete(f"{address}_blocks")
    assert client.application.extensions["cache"].get(f"{address}_blocks") is None
    response = client.get(f"/api/address/blocks?address={address}&page=2&page_size=5")
    assert response.status_code == 200
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 7,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    assert json.loads(response.data) == address_data[address]["blocks"][5:]
