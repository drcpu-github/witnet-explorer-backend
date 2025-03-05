import json


def test_utxos_cached(client, address_data):
    address_1 = "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    address_2 = "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh"
    assert client.application.extensions["cache"].get(f"{address_1}_utxos") is not None
    assert client.application.extensions["cache"].get(f"{address_2}_utxos") is not None
    response = client.get(f"/api/address/utxos?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data)[0] == {
        "address": address_1,
        "utxos": address_data[address_1]["utxos"],
    }
    assert json.loads(response.data)[1] == {
        "address": address_2,
        "utxos": address_data[address_2]["utxos"],
    }


def test_utxos_mixed_cached(client, address_data):
    address_1 = "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    address_2 = "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh"
    client.application.extensions["cache"].delete(f"{address_1}_utxos")
    assert client.application.extensions["cache"].get(f"{address_1}_utxos") is None
    assert client.application.extensions["cache"].get(f"{address_2}_utxos") is not None
    response = client.get(f"/api/address/utxos?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data)[0] == {
        "address": address_1,
        "utxos": address_data[address_1]["utxos"],
    }
    assert json.loads(response.data)[1] == {
        "address": address_2,
        "utxos": address_data[address_2]["utxos"],
    }


def test_utxos_not_cached(client, address_data):
    address_1 = "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    address_2 = "twit19kzspg5tdgh0yqry6czn50fxj52mrxjnmytzqh"
    client.application.extensions["cache"].delete(f"{address_1}_utxos")
    client.application.extensions["cache"].delete(f"{address_2}_utxos")
    assert client.application.extensions["cache"].get(f"{address_1}_utxos") is None
    assert client.application.extensions["cache"].get(f"{address_2}_utxos") is None
    response = client.get(f"/api/address/utxos?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data)[0] == {
        "address": address_1,
        "utxos": address_data[address_1]["utxos"],
    }
    assert json.loads(response.data)[1] == {
        "address": address_2,
        "utxos": address_data[address_2]["utxos"],
    }
