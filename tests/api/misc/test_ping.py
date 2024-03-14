import json


def test_ping(client):
    response = client.get("/api/ping")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"response": "pong"}
