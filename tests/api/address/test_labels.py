import json


def test_info_existing_addresses(client):
    response = client.get("/api/address/labels")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == [
        {
            "address": "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33",
            "label": "label 1",
        },
        {
            "address": "twit1w9vaa7we6h8qyc3uawdwnp9n40602hdgsxkzf6",
            "label": "label 2",
        },
    ]
