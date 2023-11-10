import json


def test_priority_cached_all(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=all")
    assert response.status_code == 200
    assert json.loads(response.data) == priority


def test_priority_cached_drt(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=drt")
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "drt_high": priority["drt_high"],
        "drt_low": priority["drt_low"],
        "drt_medium": priority["drt_medium"],
        "drt_opulent": priority["drt_opulent"],
        "drt_stinky": priority["drt_stinky"],
    }


def test_priority_cached_vtt(client, priority):
    assert client.application.extensions["cache"].get("priority") is not None
    response = client.get("/api/transaction/priority?key=vtt")
    assert response.status_code == 200
    assert json.loads(response.data) == {
        "vtt_high": priority["vtt_high"],
        "vtt_low": priority["vtt_low"],
        "vtt_medium": priority["vtt_medium"],
        "vtt_opulent": priority["vtt_opulent"],
        "vtt_stinky": priority["vtt_stinky"],
    }


def test_priority_not_cached(client, priority):
    client.application.extensions["cache"].delete("priority")
    assert client.application.extensions["cache"].get("priority") is None
    response = client.get("/api/transaction/priority?key=all")
    assert response.status_code == 200
    assert json.loads(response.data) == priority
    assert client.application.extensions["cache"].get("priority") is not None
