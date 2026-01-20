import json

from util.blockchain_functions import calculate_current_epoch


def test_version_cached_all(client, version):
    assert client.application.extensions["cache"].get("network_version_all") is not None
    response = client.get("/api/network/version?key=all")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == version


def test_version_cached_current(client, version):
    assert (
        client.application.extensions["cache"].get("network_version_current")
        is not None
    )
    response = client.get("/api/network/version?key=current")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == {
        "current_version": version["current_version"],
        "current_epoch": version["current_epoch"],
    }


def test_version_all_not_cached(client, version):
    client.application.extensions["cache"].delete("network_version_all")
    assert client.application.extensions["cache"].get("network_version_all") is None
    response = client.get("/api/network/version?key=all")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    # Update current epoch
    version["current_epoch"] = calculate_current_epoch()
    assert json.loads(response.data) == version
    assert client.application.extensions["cache"].get("network_version_all") is not None


def test_version_current_not_cached(client, version):
    client.application.extensions["cache"].delete("network_version_current")
    assert client.application.extensions["cache"].get("network_version_current") is None
    response = client.get("/api/network/version?key=current")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == {
        "current_version": version["current_version"],
        "current_epoch": calculate_current_epoch(),
    }
    assert (
        client.application.extensions["cache"].get("network_version_current")
        is not None
    )
