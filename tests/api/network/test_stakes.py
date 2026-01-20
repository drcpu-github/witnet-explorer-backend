import json


def test_network_stakes_cached_all(client, network_stakes):
    assert client.application.extensions["cache"].get("network_stakes_all") is not None
    response = client.get("/api/network/stakes")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == network_stakes["all"]


def test_network_stakes_cached_validator(client, network_stakes):
    key = "validator=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    assert client.application.extensions["cache"].get("network_stakes_all") is not None
    response = client.get(f"/api/network/stakes?{key}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == network_stakes[key]


def test_network_stakes_cached_withdrawer(client, network_stakes):
    key = "withdrawer=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    assert client.application.extensions["cache"].get("network_stakes_all") is not None
    response = client.get(f"/api/network/stakes?{key}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == network_stakes[key]


def test_network_stakes_cached_validator_withdrawer(client, network_stakes):
    key = "validator=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp_withdrawer=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    assert client.application.extensions["cache"].get("network_stakes_all") is not None
    response = client.get(f"/api/network/stakes?{key.replace('_', '&')}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == network_stakes[key]


def test_network_stakes_not_cached_all(client, network_stakes):
    client.application.extensions["cache"].delete("network_stakes_all")
    assert client.application.extensions["cache"].get("network_stakes_all") is None
    response = client.get("/api/network/stakes")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    # Cannot compare timestamps between a mockup cached version and a fake API call
    response = json.loads(response.data)
    del response["last_updated"]
    del network_stakes["all"]["last_updated"]
    assert response == network_stakes["all"]
    assert client.application.extensions["cache"].get("network_stakes_all") is not None


def test_network_stakes_not_cached_validator(client, network_stakes):
    key = "validator=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    client.application.extensions["cache"].delete("network_stakes_all")
    assert client.application.extensions["cache"].get("network_stakes_all") is None
    response = client.get(f"/api/network/stakes?{key}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    # Cannot compare timestamps between a mockup cached version and a fake API call
    response = json.loads(response.data)
    del response["last_updated"]
    del network_stakes[key]["last_updated"]
    assert response == network_stakes[key]
    assert client.application.extensions["cache"].get("network_stakes_all") is not None


def test_network_stakes_not_cached_withdrawer(client, network_stakes):
    key = "withdrawer=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    client.application.extensions["cache"].delete("network_stakes_all")
    assert client.application.extensions["cache"].get("network_stakes_all") is None
    response = client.get(f"/api/network/stakes?{key}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    # Cannot compare timestamps between a mockup cached version and a fake API call
    response = json.loads(response.data)
    del response["last_updated"]
    del network_stakes[key]["last_updated"]
    assert response == network_stakes[key]
    assert client.application.extensions["cache"].get("network_stakes_all") is not None


def test_network_stakes_not_cached_validator_withdrawer(client, network_stakes):
    key = "validator=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp_withdrawer=twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    client.application.extensions["cache"].delete("network_stakes_all")
    assert client.application.extensions["cache"].get("network_stakes_all") is None
    response = client.get(f"/api/network/stakes?{key.replace('_', '&')}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    # Cannot compare timestamps between a mockup cached version and a fake API call
    response = json.loads(response.data)
    del response["last_updated"]
    del network_stakes[key]["last_updated"]
    assert response == network_stakes[key]
    assert client.application.extensions["cache"].get("network_stakes_all") is not None
