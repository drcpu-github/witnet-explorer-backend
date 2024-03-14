import json


def test_list_rollbacks_all_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("list-rollbacks_None_None") is not None
    response = client.get("/api/network/statistics?key=list-rollbacks")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == network_statistics["list-rollbacks_None_None"]


def test_list_rollbacks_all_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("list-rollbacks_None_None")
    assert cache.get("list-rollbacks_None_None") is None
    response = client.get("/api/network/statistics?key=list-rollbacks")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == network_statistics["list-rollbacks_None_None"]
    assert cache.get("list-rollbacks_None_None") is not None


def test_list_rollbacks_first_part(client, network_statistics):
    response = client.get(
        "/api/network/statistics?key=list-rollbacks&start_epoch=1443900&stop_epoch=1445000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "start_epoch": 1443000,
        "stop_epoch": 1445000,
        "list_rollbacks": [
            network_statistics["list-rollbacks_None_None"]["list_rollbacks"][0],
            network_statistics["list-rollbacks_None_None"]["list_rollbacks"][1],
        ],
    }


def test_list_rollbacks_mid_part(client, network_statistics):
    response = client.get(
        "/api/network/statistics?key=list-rollbacks&start_epoch=1442500&stop_epoch=1443000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "start_epoch": 1442000,
        "stop_epoch": 1443000,
        "list_rollbacks": [
            network_statistics["list-rollbacks_None_None"]["list_rollbacks"][1],
            network_statistics["list-rollbacks_None_None"]["list_rollbacks"][2],
        ],
    }


def test_list_rollbacks_last_part(client, network_statistics):
    response = client.get(
        "/api/network/statistics?key=list-rollbacks&start_epoch=1430000&stop_epoch=1439999"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == {
        "start_epoch": 1430000,
        "stop_epoch": 1440000,
        "list_rollbacks": [
            network_statistics["list-rollbacks_None_None"]["list_rollbacks"][2],
        ],
    }


def test_miners_top_100_all_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("top-100-miners_None_None") is not None
    response = client.get("/api/network/statistics?key=top-100-miners")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == network_statistics["top-100-miners_None_None"]


def test_miners_top_100_all_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("top-100-miners_None_None")
    assert cache.get("top-100-miners_None_None") is None
    response = client.get("/api/network/statistics?key=top-100-miners")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert json.loads(response.data) == network_statistics["top-100-miners_None_None"]
    assert cache.get("top-100-miners_None_None") is not None


def test_data_request_solvers_top_100_all_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("top-100-data-request-solvers_None_None") is not None
    response = client.get("/api/network/statistics?key=top-100-data-request-solvers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["top-100-data-request-solvers_None_None"]
    )


def test_data_request_solvers_top_100_all_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("top-100-data-request-solvers_None_None")
    assert cache.get("top-100-data-request-solvers_None_None") is None
    response = client.get("/api/network/statistics?key=top-100-data-request-solvers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["top-100-data-request-solvers_None_None"]
    )
    assert cache.get("top-100-data-request-solvers_None_None") is not None


def test_num_unique_miners_all_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("num-unique-miners_None_None") is not None
    response = client.get("/api/network/statistics?key=num-unique-miners")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data) == network_statistics["num-unique-miners_None_None"]
    )


def test_num_unique_miners_all_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("num-unique-miners_None_None")
    assert cache.get("num-unique-miners_None_None") is None
    response = client.get("/api/network/statistics?key=num-unique-miners")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data) == network_statistics["num-unique-miners_None_None"]
    )
    assert cache.get("num-unique-miners_None_None") is not None


def test_num_unique_data_request_solvers_top_100_all_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("num-unique-data-request-solvers_None_None") is not None
    response = client.get("/api/network/statistics?key=num-unique-data-request-solvers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["num-unique-data-request-solvers_None_None"]
    )


def test_num_unique_data_request_solvers_top_100_all_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("num-unique-data-request-solvers_None_None")
    assert cache.get("num-unique-data-request-solvers_None_None") is None
    response = client.get("/api/network/statistics?key=num-unique-data-request-solvers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["num-unique-data-request-solvers_None_None"]
    )
    assert cache.get("num-unique-data-request-solvers_None_None") is not None


def test_miners_top_100_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("top-100-miners_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=top-100-miners&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["top-100-miners_1000000_1002000"]
    )


def test_miners_top_100_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("top-100-miners_1000000_1002000")
    assert cache.get("top-100-miners_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=top-100-miners&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["top-100-miners_1000000_1002000"]
    )
    assert cache.get("top-100-miners_1000000_1002000") is not None


def test_data_request_solvers_top_100_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("top-100-data-request-solvers_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=top-100-data-request-solvers&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["top-100-data-request-solvers_1000000_1002000"]
    )


def test_data_request_solvers_top_100_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("top-100-data-request-solvers_1000000_1002000")
    assert cache.get("top-100-data-request-solvers_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=top-100-data-request-solvers&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["top-100-data-request-solvers_1000000_1002000"]
    )
    assert cache.get("top-100-data-request-solvers_1000000_1002000") is not None


def test_num_unique_miners_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("num-unique-miners_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=num-unique-miners&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["num-unique-miners_1000000_1002000"]
    )


def test_num_unique_miners_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("num-unique-miners_1000000_1002000")
    assert cache.get("num-unique-miners_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=num-unique-miners&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["num-unique-miners_1000000_1002000"]
    )
    assert cache.get("num-unique-miners_1000000_1002000") is not None


def test_num_unique_data_request_solvers_top_100_period_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    assert cache.get("num-unique-data-request-solvers_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=num-unique-data-request-solvers&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["num-unique-data-request-solvers_1000000_1002000"]
    )


def test_num_unique_data_request_solvers_top_100_period_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("num-unique-data-request-solvers_1000000_1002000")
    assert cache.get("num-unique-data-request-solvers_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=num-unique-data-request-solvers&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["num-unique-data-request-solvers_1000000_1002000"]
    )
    assert cache.get("num-unique-data-request-solvers_1000000_1002000") is not None


def test_staking_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("percentile-staking-balances_None_None") is not None
    response = client.get("/api/network/statistics?key=percentile-staking-balances")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["percentile-staking-balances_None_None"]
    )


def test_staking_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("percentile-staking-balances_None_None")
    assert cache.get("percentile-staking-balances_None_None") is None
    response = client.get("/api/network/statistics?key=percentile-staking-balances")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["percentile-staking-balances_None_None"]
    )
    assert cache.get("percentile-staking-balances_None_None") is not None


def test_histogram_data_requests_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-requests_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-requests&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-requests_1000000_1002000"]
    )


def test_histogram_data_requests_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-requests_1000000_1002000")
    assert cache.get("histogram-data-requests_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-requests&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-requests_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_data_requests_most_recent_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-requests_1965000_2025000") is not None
    response = client.get("/api/network/statistics?key=histogram-data-requests")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-requests_1965000_2025000"]
    )


def test_histogram_data_requests_most_recent_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-requests_1965000_2025000")
    assert cache.get("histogram-data-requests_1965000_2025000") is None
    response = client.get("/api/network/statistics?key=histogram-data-requests")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-requests_1965000_2025000"]
    )
    assert cache.get("histogram-data-requests_1965000_2025000") is not None


def test_histogram_data_request_composition_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-composition_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-composition&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-composition_1000000_1002000"]
    )


def test_histogram_data_request_composition_period_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-composition_1000000_1002000")
    assert cache.get("histogram-data-request-composition_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-composition&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-composition_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_data_request_composition_most_recent_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-composition_1965000_2025000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-composition"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-composition_1965000_2025000"]
    )


def test_histogram_data_request_composition_most_recent_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-composition_1965000_2025000")
    assert cache.get("histogram-data-request-composition_1965000_2025000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-composition"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-composition_1965000_2025000"]
    )
    assert cache.get("histogram-data-request-composition_1965000_2025000") is not None


def test_histogram_data_request_witness_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-witness_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-witness&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-witness_1000000_1002000"]
    )


def test_histogram_data_request_witness_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-witness_1000000_1002000")
    assert cache.get("histogram-data-request-witness_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-witness&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-witness_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_data_request_witness_most_recent_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-witness_1965000_2025000") is not None
    response = client.get("/api/network/statistics?key=histogram-data-request-witness")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-witness_1965000_2025000"]
    )


def test_histogram_data_request_witness_most_recent_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-witness_1965000_2025000")
    assert cache.get("histogram-data-request-witness_1965000_2025000") is None
    response = client.get("/api/network/statistics?key=histogram-data-request-witness")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-witness_1965000_2025000"]
    )
    assert cache.get("histogram-data-request-witness_1965000_2025000") is not None


def test_histogram_data_request_collateral_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-collateral_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-collateral&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-collateral_1000000_1002000"]
    )


def test_histogram_data_request_collateral_period_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-collateral_1000000_1002000")
    assert cache.get("histogram-data-request-collateral_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-collateral&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-collateral_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_data_request_collateral_most_recent_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-collateral_1965000_2025000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-collateral"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-collateral_1965000_2025000"]
    )


def test_histogram_data_request_collateral_most_recent_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-collateral_1965000_2025000")
    assert cache.get("histogram-data-request-collateral_1965000_2025000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-collateral"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-collateral_1965000_2025000"]
    )
    assert cache.get("histogram-data-request-collateral_1965000_2025000") is not None


def test_histogram_data_request_reward_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-reward_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-reward&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-reward_1000000_1002000"]
    )


def test_histogram_data_request_reward_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-reward_1000000_1002000")
    assert cache.get("histogram-data-request-reward_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-reward&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-reward_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_data_request_reward_most_recent_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-reward_1965000_2025000") is not None
    response = client.get("/api/network/statistics?key=histogram-data-request-reward")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-reward_1965000_2025000"]
    )


def test_histogram_data_request_reward_most_recent_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-reward_1965000_2025000")
    assert cache.get("histogram-data-request-reward_1965000_2025000") is None
    response = client.get("/api/network/statistics?key=histogram-data-request-reward")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-reward_1965000_2025000"]
    )
    assert cache.get("histogram-data-request-reward_1965000_2025000") is not None


def test_histogram_data_request_lie_rate_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-lie-rate_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-lie-rate&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-lie-rate_1000000_1002000"]
    )


def test_histogram_data_request_lie_rate_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-lie-rate_1000000_1002000")
    assert cache.get("histogram-data-request-lie-rate_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-data-request-lie-rate&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-lie-rate_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_data_request_lie_rate_most_recent_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-data-request-lie-rate_1965000_2025000") is not None
    response = client.get("/api/network/statistics?key=histogram-data-request-lie-rate")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-lie-rate_1965000_2025000"]
    )


def test_histogram_data_request_lie_rate_most_recent_not_cached(
    client, network_statistics
):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-data-request-lie-rate_1965000_2025000")
    assert cache.get("histogram-data-request-lie-rate_1965000_2025000") is None
    response = client.get("/api/network/statistics?key=histogram-data-request-lie-rate")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-data-request-lie-rate_1965000_2025000"]
    )
    assert cache.get("histogram-data-request-lie-rate_1965000_2025000") is not None


def test_histogram_burn_rate_rate_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-burn-rate_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-burn-rate&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-burn-rate_1000000_1002000"]
    )


def test_histogram_burn_rate_rate_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-burn-rate_1000000_1002000")
    assert cache.get("histogram-burn-rate_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-burn-rate&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-burn-rate_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_burn_rate_rate_most_recent_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-burn-rate_1965000_2025000") is not None
    response = client.get("/api/network/statistics?key=histogram-burn-rate")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-burn-rate_1965000_2025000"]
    )


def test_histogram_burn_rate_rate_most_recent_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-burn-rate_1965000_2025000")
    assert cache.get("histogram-burn-rate_1965000_2025000") is None
    response = client.get("/api/network/statistics?key=histogram-burn-rate")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-burn-rate_1965000_2025000"]
    )
    assert cache.get("histogram-burn-rate_1965000_2025000") is not None


def test_histogram_value_transfers_period_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-value-transfers_1000000_1002000") is not None
    response = client.get(
        "/api/network/statistics?key=histogram-value-transfers&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-value-transfers_1000000_1002000"]
    )


def test_histogram_value_transfers_period_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-value-transfers_1000000_1002000")
    assert cache.get("histogram-value-transfers_1000000_1002000") is None
    response = client.get(
        "/api/network/statistics?key=histogram-value-transfers&start_epoch=1000000&stop_epoch=1002000"
    )
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-value-transfers_1000000_1002000"]
    )
    assert cache.get("histogram-data-requests_1000000_1002000") is not None


def test_histogram_value_transfers_most_recent_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    assert cache.get("histogram-value-transfers_1965000_2025000") is not None
    response = client.get("/api/network/statistics?key=histogram-value-transfers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-value-transfers_1965000_2025000"]
    )


def test_histogram_value_transfers_most_recent_not_cached(client, network_statistics):
    cache = client.application.extensions["cache"]
    cache.delete("histogram-value-transfers_1965000_2025000")
    assert cache.get("histogram-value-transfers_1965000_2025000") is None
    response = client.get("/api/network/statistics?key=histogram-value-transfers")
    assert response.status_code == 200
    assert response.headers["x-version"] == "1.0.0"
    assert (
        json.loads(response.data)
        == network_statistics["histogram-value-transfers_1965000_2025000"]
    )
    assert cache.get("histogram-value-transfers_1965000_2025000") is not None
