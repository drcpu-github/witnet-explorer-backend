import json


def test_search_hash_data_request_pending(client):
    hash_value = "1bcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "response_type": "pending",
        "pending": "Data request is pending.",
    }


def test_search_hash_value_transfer_pending(client):
    hash_value = "2bcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "response_type": "pending",
        "pending": "Value transfer is pending.",
    }


def test_search_block_cached(client, blocks):
    cache = client.application.extensions["cache"]
    hash_value = "6bf0bbafb380cced8134684c31028af6701905c223f4513f0c8d871c1beb8923"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == blocks[hash_value]["cache"]


def test_search_block_not_cached(client, blocks):
    cache = client.application.extensions["cache"]
    hash_value = "6bf0bbafb380cced8134684c31028af6701905c223f4513f0c8d871c1beb8923"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == blocks[hash_value]["cache"]


def test_search_mint_cached(client, mints):
    cache = client.application.extensions["cache"]
    hash_value = "eb88c7b07f771c4957c11a5c51947af7fe98b46b2639dfecd5b2fda6a72dba84"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == mints[hash_value]


def test_search_mint_not_cached(client, mints):
    cache = client.application.extensions["cache"]
    hash_value = "eb88c7b07f771c4957c11a5c51947af7fe98b46b2639dfecd5b2fda6a72dba84"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == mints[hash_value]


def test_search_value_transfer_cached(client, value_transfers):
    cache = client.application.extensions["cache"]
    hash_value = "be1e17a260527823272dce0094dc624bbd0850f7141d064320e182fecc78c95a"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == value_transfers[hash_value]


def test_search_value_transfer_not_cached(client, value_transfers):
    cache = client.application.extensions["cache"]
    hash_value = "be1e17a260527823272dce0094dc624bbd0850f7141d064320e182fecc78c95a"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == value_transfers[hash_value]


def test_search_data_request_simple_not_cached(client, data_requests):
    cache = client.application.extensions["cache"]
    hash_value = "c1140872c3ca99771c7c16d9b7f1273647ebaa78c6f3cc55a1b47b446a5859f5"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == data_requests[hash_value]["api"]


def test_search_commit_simple_cached(client, commits):
    cache = client.application.extensions["cache"]
    hash_value = "da2f005ca235bd788c77af499561f94edb8c27bbf097ee59033556d9c9766b84"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == commits[hash_value]


def test_search_commit_simple_not_cached(client, commits):
    cache = client.application.extensions["cache"]
    hash_value = "da2f005ca235bd788c77af499561f94edb8c27bbf097ee59033556d9c9766b84"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == commits[hash_value]
    assert cache.get(hash_value) is not None


def test_search_reveal_simple_cached(client, reveals):
    cache = client.application.extensions["cache"]
    hash_value = "63f3715662464a26ba98dceaf765df4024170424b5a50ed320d6d3eec6052c62"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == reveals[hash_value]


def test_search_reveal_simple_not_cached(client, reveals):
    cache = client.application.extensions["cache"]
    hash_value = "63f3715662464a26ba98dceaf765df4024170424b5a50ed320d6d3eec6052c62"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == reveals[hash_value]
    assert cache.get(hash_value) is not None


def test_search_tally_simple_cached(client, tallies):
    cache = client.application.extensions["cache"]
    hash_value = "b47d06d2a6627f15736d2ca02bf3b184e4349754272d8298567a9e844947f6a0"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == tallies[hash_value]


def test_search_tally_simple_not_cached(client, tallies):
    cache = client.application.extensions["cache"]
    hash_value = "b47d06d2a6627f15736d2ca02bf3b184e4349754272d8298567a9e844947f6a0"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}&simple=true")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == tallies[hash_value]
    assert cache.get(hash_value) is not None


def test_search_data_request_report_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    hash_value = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == data_request_reports[hash_value]


def test_search_data_request_report_not_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    hash_value = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == data_request_reports[hash_value]
    assert cache.get(hash_value) is not None


def test_search_data_request_report_from_commit_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    assert cache.get(dr_hash) is not None
    commit_hash = "563eba0199a23283c0764bd8690522666ba56a5024eef7cc6f253be53efacb6a"
    response = client.get(f"/api/search/hash?value={commit_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "commit"
    assert json.loads(response.data) == data_request_reports[dr_hash]


def test_search_data_request_report_from_commit_not_cached(
    client, data_request_reports
):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    cache.delete(dr_hash)
    assert cache.get(dr_hash) is None
    commit_hash = "563eba0199a23283c0764bd8690522666ba56a5024eef7cc6f253be53efacb6a"
    response = client.get(f"/api/search/hash?value={commit_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "commit"
    assert json.loads(response.data) == response_data
    assert cache.get(dr_hash) is not None


def test_search_data_request_report_from_cached_commit_cached(
    client, data_request_reports
):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    commit_hash = "563eba0199a23283c0764bd8690522666ba56a5024eef7cc6f253be53efacb6a"
    assert cache.get(commit_hash) is not None
    response = client.get(f"/api/search/hash?value={commit_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "commit"
    assert json.loads(response.data) == response_data


def test_search_data_request_report_from_reveal_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    assert cache.get(dr_hash) is not None
    reveal_hash = "0e7ea734b1ad24e69406f2059888041e353cb9fabe1b4f1345fe230c3dbbc9ac"
    response = client.get(f"/api/search/hash?value={reveal_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "reveal"
    assert json.loads(response.data) == data_request_reports[dr_hash]


def test_search_data_request_report_from_reveal_not_cached(
    client, data_request_reports
):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    cache.delete(dr_hash)
    assert cache.get(dr_hash) is None
    reveal_hash = "0e7ea734b1ad24e69406f2059888041e353cb9fabe1b4f1345fe230c3dbbc9ac"
    response = client.get(f"/api/search/hash?value={reveal_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "reveal"
    assert json.loads(response.data) == response_data
    assert cache.get(dr_hash) is not None


def test_search_data_request_report_from_cached_reveal_cached(
    client, data_request_reports
):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    reveal_hash = "0e7ea734b1ad24e69406f2059888041e353cb9fabe1b4f1345fe230c3dbbc9ac"
    assert cache.get(reveal_hash) is not None
    response = client.get(f"/api/search/hash?value={reveal_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "reveal"
    assert json.loads(response.data) == response_data


def test_search_data_request_report_from_tally_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    assert cache.get(dr_hash) is not None
    tally_hash = "dcb4f1ebde98b4ba0c819fca0cc339993322e67900ba53d9b5534afba844af11"
    response = client.get(f"/api/search/hash?value={tally_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "tally"
    assert json.loads(response.data) == data_request_reports[dr_hash]


def test_search_data_request_report_from_tally_not_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    cache.delete(dr_hash)
    assert cache.get(dr_hash) is None
    tally_hash = "dcb4f1ebde98b4ba0c819fca0cc339993322e67900ba53d9b5534afba844af11"
    response = client.get(f"/api/search/hash?value={tally_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "tally"
    assert json.loads(response.data) == response_data
    assert cache.get(dr_hash) is not None


def test_search_data_request_report_from_cached_tally_cached(
    client, data_request_reports
):
    cache = client.application.extensions["cache"]
    dr_hash = "713973d2f0b4fef783bc2c31b0efa1f931e12add19cad72870d09a2517e711b6"
    tally_hash = "dcb4f1ebde98b4ba0c819fca0cc339993322e67900ba53d9b5534afba844af11"
    assert cache.get(tally_hash) is not None
    response = client.get(f"/api/search/hash?value={tally_hash}")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    # Need to replace the transaction type when building the data request report from the database
    response_data = data_request_reports[dr_hash]
    response_data["data_request_report"]["transaction_type"] = "tally"
    assert json.loads(response.data) == response_data


def test_search_data_request_history_DRO_page_1(client, data_request_history_dro):
    hash_value = "0332cb684de3bb0e9b2b0d8b43524eed7fc51b00fefa038ee3bf6f6ac9c7cc82"
    response = client.get(f"/api/search/hash?value={hash_value}&page_size=5")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 4,
        "total_pages": 1,
        "first_page": 1,
        "last_page": 1,
        "page": 1,
    }
    assert json.loads(response.data) == data_request_history_dro


def test_search_data_request_history_RAD_page_1(client, data_request_history_rad):
    hash_value = "1a643dcd0299ee7982ede4387580ff406207930a6b11fd14d2e9ec5dccab476a"
    response = client.get(f"/api/search/hash?value={hash_value}&page_size=5")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 8,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 1,
        "next_page": 2,
    }
    data_request_history_rad["data_request_history"][
        "history"
    ] = data_request_history_rad["data_request_history"]["history"][:5]
    assert json.loads(response.data) == data_request_history_rad


def test_search_data_request_history_RAD_page_2(client, data_request_history_rad):
    hash_value = "1a643dcd0299ee7982ede4387580ff406207930a6b11fd14d2e9ec5dccab476a"
    response = client.get(f"/api/search/hash?value={hash_value}&page_size=5&page=2")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.headers["X-Pagination"]) == {
        "total": 8,
        "total_pages": 2,
        "first_page": 1,
        "last_page": 2,
        "page": 2,
        "previous_page": 1,
    }
    data_request_history_rad["data_request_history"][
        "history"
    ] = data_request_history_rad["data_request_history"]["history"][5:]
    assert json.loads(response.data) == data_request_history_rad
