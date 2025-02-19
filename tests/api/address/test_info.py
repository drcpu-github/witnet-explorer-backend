import json


def test_info_existing_addresses(client):
    address_1 = "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33"
    address_2 = "twit1w9vaa7we6h8qyc3uawdwnp9n40602hdgsxkzf6"
    response = client.get(f"/api/address/info?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == [
        {
            "address": "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33",
            "label": "label 1",
            "active": 2024098,
            "block": 68,
            "mint": 68,
            "value_transfer": 90,
            "data_request": 4272,
            "commit": 1866,
            "reveal": 1866,
            "tally": 3051,
        },
        {
            "address": "twit1w9vaa7we6h8qyc3uawdwnp9n40602hdgsxkzf6",
            "label": "label 2",
            "active": 1657960,
            "block": 21,
            "mint": 21,
            "value_transfer": 29,
            "data_request": 2246,
            "commit": 658,
            "reveal": 658,
            "tally": 1406,
        },
    ]


def test_info_non_existing_addresses(client):
    address_1 = "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc32"
    address_2 = "twit1w9vaa7we6h8qyc3uawdwnp9n40602hdgsxkzf7"
    response = client.get(f"/api/address/info?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == []
