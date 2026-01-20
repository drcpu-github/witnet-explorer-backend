import json


def test_priority_cached_all(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=all")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == priority


def test_priority_cached_drt(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=drt")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"drt": priority["drt"]}


def test_priority_cached_vtt(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=vtt")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"vtt": priority["vtt"]}


def test_priority_cached_st(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=st")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"st": priority["st"]}


def test_priority_cached_ut(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=ut")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"ut": priority["ut"]}


def test_priority_not_cached_all(client, priority):
    client.application.extensions["cache"].delete("priority")
    assert client.application.extensions["cache"].get("priority") is None
    response = client.get("/api/transaction/priority?key=all")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == priority
    assert client.application.extensions["cache"].get("priority") is not None


def test_priority_not_cached_drt(client, priority):
    client.application.extensions["cache"].delete("priority")
    assert client.application.extensions["cache"].get("priority") is None
    response = client.get("/api/transaction/priority?key=drt")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"drt": priority["drt"]}
    assert client.application.extensions["cache"].get("priority") is not None


def test_priority_not_cached_vtt(client, priority):
    client.application.extensions["cache"].delete("priority")
    assert client.application.extensions["cache"].get("priority") is None
    response = client.get("/api/transaction/priority?key=vtt")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"vtt": priority["vtt"]}
    assert client.application.extensions["cache"].get("priority") is not None


def test_priority_not_cached_st(client, priority):
    client.application.extensions["cache"].delete("priority")
    assert client.application.extensions["cache"].get("priority") is None
    response = client.get("/api/transaction/priority?key=st")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"st": priority["st"]}
    assert client.application.extensions["cache"].get("priority") is not None


def test_priority_not_cached_ut(client, priority):
    client.application.extensions["cache"].delete("priority")
    assert client.application.extensions["cache"].get("priority") is None
    response = client.get("/api/transaction/priority?key=ut")
    assert response.status_code == 200
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"ut": priority["ut"]}
    assert client.application.extensions["cache"].get("priority") is not None
