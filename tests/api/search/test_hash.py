import json


def test_search_hash_data_request_pending(client):
    hash_value = "000827cdc18272dca45f77507a511db718b0dd1d801a6801bb0649d079e32240"
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "pending",
        "pending": "Data request transaction is pending.",
    }


def test_search_hash_value_transfer_pending(client):
    hash_value = "5b24740a09fe304484ac89facd4349d33444bcfc228fd03d02d4a1a5dc326ddc"
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "pending",
        "pending": "Value transfer transaction is pending.",
    }


def test_search_hash_stake_pending(client):
    hash_value = "11d2381ef9ee5d46edf674f39297719bcf370d413d8c9d1cf34a39f2998ecd25"
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "pending",
        "pending": "Stake transaction is pending.",
    }


def test_search_hash_unstake_pending(client):
    hash_value = "6e8161fedf39f7a7a40d1c3bb9127d608b58891c66189765c7c1b18646002dcd"
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "pending",
        "pending": "Unstake transaction is pending.",
    }


def test_search_block_cached(client, blocks):
    cache = client.application.extensions["cache"]
    for block_hash, block in blocks.items():
        assert cache.get(block_hash) is not None
        response = client.get(f"/api/search/hash?value={block_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == block["processed"]["api"]


def test_search_block_not_cached(client, blocks):
    cache = client.application.extensions["cache"]
    for block_hash, block in blocks.items():
        print(block_hash)
        cache.delete(block_hash)
        assert cache.get(block_hash) is None
        response = client.get(f"/api/search/hash?value={block_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == block["processed"]["api"]


def test_search_mint_cached(client, mints):
    cache = client.application.extensions["cache"]
    for mint_hash, mint in mints.items():
        assert cache.get(mint_hash) is not None
        response = client.get(f"/api/search/hash?value={mint_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "mint",
            "mint": mint["processed"]["database"],
        }


def test_search_mint_not_cached(client, mints):
    cache = client.application.extensions["cache"]
    for mint_hash, mint in mints.items():
        cache.delete(mint_hash)
        assert cache.get(mint_hash) is None
        response = client.get(f"/api/search/hash?value={mint_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "mint",
            "mint": mint["processed"]["database"],
        }
        assert cache.get(mint_hash) is not None


def test_search_value_transfer_cached(client, value_transfers):
    cache = client.application.extensions["cache"]
    for value_transfer_hash, value_transfer in value_transfers.items():
        assert cache.get(value_transfer_hash) is not None
        response = client.get(f"/api/search/hash?value={value_transfer_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "value_transfer",
            "value_transfer": value_transfer["processed"]["database"],
        }


def test_search_value_transfer_not_cached(client, value_transfers):
    cache = client.application.extensions["cache"]
    for value_transfer_hash, value_transfer in value_transfers.items():
        cache.delete(value_transfer_hash)
        assert cache.get(value_transfer_hash) is None
        response = client.get(f"/api/search/hash?value={value_transfer_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "value_transfer",
            "value_transfer": value_transfer["processed"]["database"],
        }
        assert cache.get(value_transfer_hash) is not None


def test_search_data_request_simple_cached(client, data_requests):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request in data_requests.items():
        assert cache.get(data_request_hash) is not None
        response = client.get(f"/api/search/hash?value={data_request_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "data_request",
            "data_request": data_request["processed"]["database"],
        }


def test_search_data_request_simple_not_cached(client, data_requests):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request in data_requests.items():
        cache.delete(data_request_hash)
        assert cache.get(data_request_hash) is None
        response = client.get(f"/api/search/hash?value={data_request_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "data_request",
            "data_request": data_request["processed"]["database"],
        }


def test_search_commit_simple_cached(client, commits):
    cache = client.application.extensions["cache"]
    for commit_hash, commit in commits.items():
        assert cache.get(commit_hash) is not None
        response = client.get(f"/api/search/hash?value={commit_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "commit",
            "commit": commit["processed"]["database"],
        }


def test_search_commit_simple_not_cached(client, commits):
    cache = client.application.extensions["cache"]
    for commit_hash, commit in commits.items():
        cache.delete(commit_hash)
        assert cache.get(commit_hash) is None
        response = client.get(f"/api/search/hash?value={commit_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "commit",
            "commit": commit["processed"]["database"],
        }
        assert cache.get(commit_hash) is not None


def test_search_reveal_simple_cached(client, reveals):
    cache = client.application.extensions["cache"]
    for reveal_hash, reveal in reveals.items():
        assert cache.get(reveal_hash) is not None
        response = client.get(f"/api/search/hash?value={reveal_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "reveal",
            "reveal": reveal["processed"]["database"],
        }


def test_search_reveal_simple_not_cached(client, reveals):
    cache = client.application.extensions["cache"]
    for reveal_hash, reveal in reveals.items():
        cache.delete(reveal_hash)
        assert cache.get(reveal_hash) is None
        response = client.get(f"/api/search/hash?value={reveal_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "reveal",
            "reveal": reveal["processed"]["database"],
        }
        assert cache.get(reveal_hash) is not None


def test_search_tally_simple_cached(client, tallies):
    cache = client.application.extensions["cache"]
    for tally_hash, tally in tallies.items():
        assert cache.get(tally_hash) is not None
        response = client.get(f"/api/search/hash?value={tally_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "tally",
            "tally": tally["processed"]["database"],
        }


def test_search_tally_simple_not_cached(client, tallies):
    cache = client.application.extensions["cache"]
    for tally_hash, tally in tallies.items():
        cache.delete(tally_hash)
        assert cache.get(tally_hash) is None
        response = client.get(f"/api/search/hash?value={tally_hash}&simple=true")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "tally",
            "tally": tally["processed"]["database"],
        }
        assert cache.get(tally_hash) is not None


def test_search_data_request_report_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        assert cache.get(data_request_hash) is not None
        response = client.get(f"/api/search/hash?value={data_request_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }


def test_search_data_request_report_not_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        cache.delete(data_request_hash)
        assert cache.get(data_request_hash) is None
        response = client.get(f"/api/search/hash?value={data_request_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }
        assert cache.get(data_request_hash) is not None


def test_search_data_request_report_from_commit_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        assert cache.get(data_request_hash) is not None
        # No commits for this data request, skip the test
        if len(data_request_report["commits"]) == 0:
            continue
        commit_hash = data_request_report["commits"][0]["hash"]
        response = client.get(f"/api/search/hash?value={commit_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "commit"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }


def test_search_data_request_report_from_commit_not_cached(
    client,
    data_request_reports,
):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        cache.delete(data_request_hash)
        assert cache.get(data_request_hash) is None
        # No commits for this data request, skip the test
        if len(data_request_report["commits"]) == 0:
            continue
        commit_hash = data_request_report["commits"][0]["hash"]
        response = client.get(f"/api/search/hash?value={commit_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "commit"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }
        assert cache.get(data_request_hash) is not None


def test_search_data_request_report_from_cached_commit_cached(
    client,
    commits,
    data_request_reports,
):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        # No commits for this data request, skip the test
        if len(data_request_report["commits"]) == 0:
            continue
        commit_hash = data_request_report["commits"][0]["hash"]
        assert cache.get(commit_hash) is not None
        response = client.get(f"/api/search/hash?value={commit_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "commit"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }
        assert cache.get(data_request_hash) is not None


def test_search_data_request_report_from_reveal_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        assert cache.get(data_request_hash) is not None
        # No reveals for this data request, skip the test
        if len(data_request_report["reveals"]) == 0:
            continue
        reveal_hash = data_request_report["reveals"][0]["hash"]
        response = client.get(f"/api/search/hash?value={reveal_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "reveal"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }


def test_search_data_request_report_from_reveal_not_cached(
    client,
    data_request_reports,
):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        cache.delete(data_request_hash)
        assert cache.get(data_request_hash) is None
        # No reveals for this data request, skip the test
        if len(data_request_report["reveals"]) == 0:
            continue
        reveal_hash = data_request_report["reveals"][0]["hash"]
        response = client.get(f"/api/search/hash?value={reveal_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "reveal"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }
        assert cache.get(data_request_hash) is not None


def test_search_data_request_report_from_cached_reveal_cached(
    client,
    reveals,
    data_request_reports,
):
    cache = client.application.extensions["cache"]
    for _, data_request_report in data_request_reports.items():
        # No reveals for this data request, skip the test
        if len(data_request_report["reveals"]) == 0:
            continue
        reveal_hash = data_request_report["reveals"][0]["hash"]
        assert cache.get(reveal_hash) is not None
        response = client.get(f"/api/search/hash?value={reveal_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "reveal"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }


def test_search_data_request_report_from_tally_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        assert cache.get(data_request_hash) is not None
        tally_hash = data_request_report["tally"]["hash"]
        response = client.get(f"/api/search/hash?value={tally_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "tally"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }


def test_search_data_request_report_from_tally_not_cached(client, data_request_reports):
    cache = client.application.extensions["cache"]
    for data_request_hash, data_request_report in data_request_reports.items():
        cache.delete(data_request_hash)
        assert cache.get(data_request_hash) is None
        tally_hash = data_request_report["tally"]["hash"]
        response = client.get(f"/api/search/hash?value={tally_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "tally"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }
        assert cache.get(data_request_hash) is not None


def test_search_data_request_report_from_cached_tally_cached(
    client,
    tallies,
    data_request_reports,
):
    cache = client.application.extensions["cache"]
    for _, data_request_report in data_request_reports.items():
        tally_hash = data_request_report["tally"]["hash"]
        assert cache.get(tally_hash) is not None
        response = client.get(f"/api/search/hash?value={tally_hash}")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        # Need to replace the transaction type when building the data request report from the database
        data_request_report["transaction_type"] = "tally"
        assert json.loads(response.data) == {
            "response_type": "data_request_report",
            "data_request_report": data_request_report,
        }


def test_search_stake_cached(client, stakes):
    cache = client.application.extensions["cache"]
    hash_value = "7fdbfba5239650557dae61e0ae94bec383dcdc96a2e8b7f77a36b916ad1b0a0b"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "stake",
        "stake": stakes[hash_value]["processed"]["database"],
    }


def test_search_stake_not_cached(client, stakes):
    cache = client.application.extensions["cache"]
    hash_value = "7fdbfba5239650557dae61e0ae94bec383dcdc96a2e8b7f77a36b916ad1b0a0b"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "stake",
        "stake": stakes[hash_value]["processed"]["database"],
    }


def test_search_stake_no_change_cached(client, stakes):
    cache = client.application.extensions["cache"]
    hash_value = "0ea59be6f3c154836b26387dcdfbb5372f47a338706b8f108da342b699f0f1b3"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "stake",
        "stake": stakes[hash_value]["processed"]["database"],
    }


def test_search_stake_no_change_not_cached(client, stakes):
    cache = client.application.extensions["cache"]
    hash_value = "0ea59be6f3c154836b26387dcdfbb5372f47a338706b8f108da342b699f0f1b3"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "stake",
        "stake": stakes[hash_value]["processed"]["database"],
    }


def test_search_unstake_cached(client, unstakes):
    cache = client.application.extensions["cache"]
    hash_value = "c66e40e48763678b4ada2442e59b5a04b69a620a2bf72799abf0cd652a5a68e7"
    assert cache.get(hash_value) is not None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "unstake",
        "unstake": unstakes[hash_value]["processed"]["database"],
    }


def test_search_unstake_not_cached(client, unstakes):
    cache = client.application.extensions["cache"]
    hash_value = "c66e40e48763678b4ada2442e59b5a04b69a620a2bf72799abf0cd652a5a68e7"
    cache.delete(hash_value)
    assert cache.get(hash_value) is None
    response = client.get(f"/api/search/hash?value={hash_value}")
    assert response.status_code == 200
    assert response.headers["X-Version"] == "2.0.0"
    assert json.loads(response.data) == {
        "response_type": "unstake",
        "unstake": unstakes[hash_value]["processed"]["database"],
    }


def test_search_data_request_history_DRO_page_1(client, data_request_history_dro):
    for dro_hash, dro_history in data_request_history_dro.items():
        response = client.get(f"/api/search/hash?value={dro_hash}&page_size=5")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.headers["X-Pagination"]) == {
            "total": 7,
            "total_pages": 2,
            "first_page": 1,
            "last_page": 2,
            "page": 1,
            "next_page": 2,
        }
        dro_history["history"] = dro_history["history"][:5]
        assert json.loads(response.data) == {
            "response_type": "data_request_history",
            "data_request_history": dro_history,
        }


def test_search_data_request_history_DRO_page_2(client, data_request_history_dro):
    for dro_hash, dro_history in data_request_history_dro.items():
        response = client.get(f"/api/search/hash?value={dro_hash}&page_size=5&page=2")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.headers["X-Pagination"]) == {
            "total": 7,
            "total_pages": 2,
            "first_page": 1,
            "last_page": 2,
            "page": 2,
            "previous_page": 1,
        }
        dro_history["history"] = dro_history["history"][5:]
        assert json.loads(response.data) == {
            "response_type": "data_request_history",
            "data_request_history": dro_history,
        }


def test_search_data_request_history_RAD_page_1(client, data_request_history_rad):
    for rad_hash, rad_history in data_request_history_rad.items():
        response = client.get(f"/api/search/hash?value={rad_hash}&page_size=5")
        assert response.status_code == 200
        assert response.headers["X-Version"] == "2.0.0"
        assert json.loads(response.headers["X-Pagination"]) == {
            "total": 3,
            "total_pages": 1,
            "first_page": 1,
            "last_page": 1,
            "page": 1,
        }
        assert json.loads(response.data) == {
            "response_type": "data_request_history",
            "data_request_history": rad_history,
        }
