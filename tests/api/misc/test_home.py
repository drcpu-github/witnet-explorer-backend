import json


def test_home_cached(client, home):
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/home")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == home


def test_home_cached_network_stats(client, home):
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/home?key=network_stats")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"network_stats": home["network_stats"]}


def test_home_cached_supply_info(client, home):
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/home?key=supply_info")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"supply_info": home["supply_info"]}


def test_home_cached_blocks(client, home):
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/home?key=blocks")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"latest_blocks": home["latest_blocks"]}


def test_home_cached_data_requests(client, home):
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/home?key=data_requests")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "latest_data_requests": home["latest_data_requests"]
    }


def test_home_cached_value_transfers(client, home):
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/home?key=value_transfers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "latest_value_transfers": home["latest_value_transfers"]
    }


def test_home_not_cached(client):
    client.application.extensions["cache"].delete("home")
    assert client.application.extensions["cache"].get("home") is None
    response = client.get("/api/home")
    assert response.status_code == 404
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)["message"]
        == "Could not find homepage data in the cache."
    )
