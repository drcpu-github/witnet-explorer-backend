import json


def test_reputation_cached(client, reputation):
    response = client.get("/api/network/reputation")
    assert response.status_code == 200
    assert json.loads(response.data) == reputation


def test_reputation_not_cached(client, reputation):
    client.application.extensions["cache"].delete("reputation")
    assert client.application.extensions["cache"].get("reputation") is None
    response = client.get("/api/network/reputation")
    assert response.status_code == 200
    assert json.loads(response.data)["reputation"] == reputation["reputation"]
    assert (
        json.loads(response.data)["total_reputation"] == reputation["total_reputation"]
    )
    assert client.application.extensions["cache"].get("reputation") is not None
