import json


def test_send_test(client, value_transfer):
    response = client.post(
        "/api/transaction/send",
        json={
            "test": True,
            "transaction": {
                "ValueTransfer": value_transfer,
            },
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"result": "Value transfer is valid."}


def test_send(client, value_transfer):
    response = client.post(
        "/api/transaction/send",
        json={
            "transaction": {
                "ValueTransfer": value_transfer,
            }
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {"result": "Succesfully sent value transfer."}
