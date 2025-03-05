import json


def test_details_existing_validator(client, address_data):
    address = "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    response = client.get(f"/api/address/details?address={address}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == address_data[address]["details"]


def test_details_non_existing_validator(client, address_data):
    address = "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh"
    response = client.get(f"/api/address/details?address={address}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == address_data[address]["details"]
