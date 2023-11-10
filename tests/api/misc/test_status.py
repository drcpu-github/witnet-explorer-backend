import json


def test_status_cached(client, status):
    assert client.application.extensions["cache"].get("status") is not None
    response = client.get("/api/status")
    assert response.status_code == 200
    assert json.loads(response.data) == status


def test_status_not_cached(client, status):
    client.application.extensions["cache"].delete("status")
    assert client.application.extensions["cache"].get("status") is None
    response = client.get("/api/status")
    assert response.status_code == 200
    json_data = json.loads(response.data)
    assert json_data["database_confirmed"] == status["database_confirmed"]
    assert json_data["database_unconfirmed"] == status["database_unconfirmed"]
    # The status message calculates the expected live epoch versus the last saved one which results
    # in the status result always being in error, but that is irrelevant for testing the code
    assert json_data["database_message"] == "database processes have probably crashed"
    assert json_data["message"] == "some backend services are down"
    assert json_data["node_pool_message"] == status["node_pool_message"]
    assert client.application.extensions["cache"].get("status") is not None
