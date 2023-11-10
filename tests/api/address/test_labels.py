import json


def test_info_existing_addresses(client):
    response = client.get("/api/address/labels")
    assert response.status_code == 200
    assert json.loads(response.data) == [
        {
            "address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq",
            "label": "drcpu0",
        },
        {
            "address": "wit1drcpu2gf386tm29mh62cce0seun76rrvk5nca6",
            "label": "drcpu2",
        },
    ]
