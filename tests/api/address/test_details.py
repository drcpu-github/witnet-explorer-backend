import json


def test_details(client, address_data):
    address = "wit10a4ynl3v39jpps9v2wlddg638xz55jzgr5algm"
    response = client.get(f"/api/address/details?address={address}")
    assert response.status_code == 200
    assert json.loads(response.data) == address_data[address]["details"]
