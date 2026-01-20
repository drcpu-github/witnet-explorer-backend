import json


def test_send_value_transfer_test(client, value_transfer):
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
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"result": "Transaction is valid."}


def test_send_value_transfer(client, value_transfer):
    response = client.post(
        "/api/transaction/send",
        json={
            "transaction": {
                "ValueTransfer": value_transfer,
            }
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"result": "Succesfully sent transaction."}


def test_send_stake_test(client, stake):
    response = client.post(
        "/api/transaction/send",
        json={
            "test": True,
            "transaction": {
                "Stake": stake,
            },
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"result": "Transaction is valid."}


def test_send_stake(client, stake):
    response = client.post(
        "/api/transaction/send",
        json={
            "transaction": {
                "Stake": stake,
            }
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"result": "Succesfully sent transaction."}


def test_send_unstake_test(client, unstake):
    response = client.post(
        "/api/transaction/send",
        json={
            "test": True,
            "transaction": {
                "Unstake": unstake,
            },
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"result": "Transaction is valid."}


def test_send_unstake(client, unstake):
    response = client.post(
        "/api/transaction/send",
        json={
            "transaction": {
                "Unstake": unstake,
            }
        },
    )
    assert response.status_code == 201
    assert response.headers["x-version"] == "2.0.0"
    assert json.loads(response.data) == {"result": "Succesfully sent transaction."}
