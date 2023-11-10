import json


def test_tapi_cached(client, tapi):
    cache = client.application.extensions["cache"]
    assert cache.get("tapi-13") is not None
    response = client.get("/api/network/tapi")
    assert response.status_code == 200
    assert json.loads(response.data) == [tapi["tapi-13"]]


def test_tapi_not_cached(client, tapi):
    cache = client.application.extensions["cache"]
    cache.delete("tapi-13")
    assert cache.get("tapi-13") is None
    response = client.get("/api/network/tapi")
    assert response.status_code == 200
    assert json.loads(response.data) == [tapi["tapi-13"]]


def test_tapi_all_cached(client, tapi):
    cache = client.application.extensions["cache"]
    assert cache.get("tapi-13") is not None
    response = client.get("/api/network/tapi?return_all=true")
    assert response.status_code == 200
    assert json.loads(response.data) == [tapi["tapi-7"], tapi["tapi-13"]]


def test_tapi_all_not_cached(client, tapi):
    cache = client.application.extensions["cache"]
    cache.delete("tapi-13")
    assert cache.get("tapi-13") is None
    response = client.get("/api/network/tapi?return_all=true")
    assert response.status_code == 200
    assert json.loads(response.data) == [tapi["tapi-7"], tapi["tapi-13"]]
