import json


def test_nonce(client, address_data):
    validator = "twit1najvm34rta4vnkpfax8kk0vhpntg5lgdz8wc33"
    withdrawer = "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp"
    response = client.get(
        f"/api/transaction/nonce?validator={validator}&withdrawer={withdrawer}"
    )
    assert response.status_code == 200
    assert response.headers["X-Version"] == "1.0.0"
    assert json.loads(response.data) == {
        "validator": validator,
        "withdrawer": withdrawer,
        "nonce": 2078,
    }
