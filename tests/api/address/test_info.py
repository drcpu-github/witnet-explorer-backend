import json


def test_info_existing_addresses(client):
    address_1 = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq"
    address_2 = "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6"
    response = client.get(f"/api/address/info?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert json.loads(response.data) == [
        {
            "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
            "label": "drcpu0",
            "active": 2024098,
            "block": 68,
            "mint": 68,
            "value_transfer": 90,
            "data_request": 4272,
            "commit": 1866,
            "reveal": 1866,
            "tally": 3051,
        },
        {
            "address": "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6",
            "label": "drcpu2",
            "active": 1657960,
            "block": 21,
            "mint": 21,
            "value_transfer": 29,
            "data_request": 2246,
            "commit": 658,
            "reveal": 658,
            "tally": 1406,
        },
    ]


def test_info_non_existing_addresses(client):
    address_1 = "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsr"
    address_2 = "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca7"
    response = client.get(f"/api/address/info?addresses={address_1},{address_2}")
    assert response.status_code == 200
    assert json.loads(response.data) == []
