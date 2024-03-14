import json


def test_supply_info_blocks_minted_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=blocks_minted")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["blocks_minted"]))


def test_supply_info_blocks_minted_reward_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=blocks_minted_reward")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(
        int(supply_info["blocks_minted_reward"] / 1e9)
    )


def test_supply_info_blocks_missing_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=blocks_missing")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["blocks_missing"]))


def test_supply_info_blocks_missing_reward_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=blocks_missing_reward")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(
        int(supply_info["blocks_missing_reward"] / 1e9)
    )


def test_supply_info_current_locked_supply_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=current_locked_supply")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(
        int(supply_info["current_locked_supply"] / 1e9)
    )


def test_supply_info_current_time_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=current_time")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["current_time"]))


def test_supply_info_current_unlocked_supply_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=current_unlocked_supply")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(
        int(supply_info["current_unlocked_supply"] / 1e9)
    )


def test_supply_info_epoch_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=epoch")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["epoch"]))


def test_supply_info_in_flight_requests_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=in_flight_requests")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["in_flight_requests"]))


def test_supply_info_locked_wits_by_requests_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=locked_wits_by_requests")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(
        int(supply_info["locked_wits_by_requests"] / 1e9)
    )


def test_supply_info_maximum_supply_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=maximum_supply")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["maximum_supply"] / 1e9))


def test_supply_info_current_supply_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=current_supply")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["current_supply"] / 1e9))


def test_supply_info_total_supply_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=total_supply")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(int(supply_info["total_supply"] / 1e9))


def test_supply_info_supply_burned_lies_cached(client, home):
    supply_info = home["supply_info"]
    assert client.application.extensions["cache"].get("home") is not None
    response = client.get("/api/network/supply?key=supply_burned_lies")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == str(
        int(supply_info["supply_burned_lies"] / 1e9)
    )


def test_supply_info_not_cached(client, home):
    client.application.extensions["cache"].delete("home")
    assert client.application.extensions["cache"].get("home") is None
    response = client.get("/api/network/supply?key=total_supply")
    assert response.status_code == 404
    assert (
        json.loads(response.data)["message"]
        == "Could not find supply info data in the cache."
    )
