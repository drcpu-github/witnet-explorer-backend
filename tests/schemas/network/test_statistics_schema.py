import pytest
from marshmallow import ValidationError

from schemas.network.statistics_schema import (
    NetworkStatisticsArgs,
    NetworkStatisticsResponse,
)

valid_keys = [
    "list-rollbacks",
    "num-unique-miners",
    "num-unique-data-request-solvers",
    "top-100-miners",
    "top-100-data-request-solvers",
    "percentile-staking-balances",
    "histogram-data-requests",
    "histogram-data-request-composition",
    "histogram-data-request-witness",
    "histogram-data-request-lie-rate",
    "histogram-burn-rate",
    "histogram-data-request-collateral",
    "histogram-data-request-reward",
    "histogram-value-transfers",
]


def test_network_statistics_success():
    data = {"key": "list-rollbacks"}
    NetworkStatisticsArgs().load(data)


def test_network_statistics_failure_one_of():
    data = {"key": "list-rollback"}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsArgs().load(data)
    assert (
        err_info.value.messages["key"][0] == f"Must be one of: {', '.join(valid_keys)}."
    )


def test_network_statistics_failure_order():
    data = {"key": "list-rollbacks", "start_epoch": "1000000", "stop_epoch": "0"}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsArgs().load(data)
    assert (
        err_info.value.messages["_schema"][0]
        == "The stop_epoch parameter is smaller than the start_epoch parameter."
    )


def test_network_staking_success():
    data = {
        "staking": {"ars": [1, 2, 3], "trs": [4, 5, 6], "percentiles": [30, 60, 90]}
    }
    NetworkStatisticsResponse().load(data)


def test_network_staking_failure_missing():
    data = {"staking": {}}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data)
    assert (
        err_info.value.messages["staking"]["ars"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["staking"]["trs"][0]
        == "Missing data for required field."
    )


def test_network_rollback_success():
    data = {
        "list_rollbacks": [
            {"timestamp": 0, "epoch_from": 0, "epoch_to": 9, "length": 10},
            {"timestamp": 100, "epoch_from": 100, "epoch_to": 199, "length": 100},
        ]
    }
    NetworkStatisticsResponse().load(data)


def test_network_rollback_failure_length():
    data = {
        "list_rollbacks": [
            {"timestamp": 0, "epoch_from": 0, "epoch_to": 9, "length": 9}
        ]
    }
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data)
    assert (
        err_info.value.messages["list_rollbacks"][0]["_schema"][0]
        == "Incorrect rollback length: 9 - 0 + 1 != 9."
    )


def test_network_rollback_failure_missing():
    data = {"list_rollbacks": [{}]}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data)
    key = "list_rollbacks"
    assert (
        err_info.value.messages[key][0]["timestamp"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][0]["epoch_from"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][0]["epoch_to"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][0]["length"][0]
        == "Missing data for required field."
    )


def test_top_100_success():
    data = {
        "top_100_miners": [
            {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq", "amount": 30},
            {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq", "amount": 20},
            {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq", "amount": 10},
        ]
    }
    NetworkStatisticsResponse().load(data)
    data = {
        "top_100_data_request_solvers": [
            {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq", "amount": 30},
            {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq", "amount": 20},
            {"address": "wit1drcpu0xc2akfcqn8r69vw70pj8fzjhjypdcfsq", "amount": 10},
        ]
    }
    NetworkStatisticsResponse().load(data)


def test_top_100_failure_address():
    data = {
        "top_100_miners": [
            {"address": "1", "amount": 30},
            {"address": "2", "amount": 20},
            {"address": "3", "amount": 10},
        ]
    }
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data)
    key = "top_100_miners"
    assert (
        err_info.value.messages[key][0]["address"][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages[key][0]["address"][1]
        == "Address does not start with wit1 string."
    )
    assert (
        err_info.value.messages[key][1]["address"][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages[key][1]["address"][1]
        == "Address does not start with wit1 string."
    )
    assert (
        err_info.value.messages[key][2]["address"][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages[key][2]["address"][1]
        == "Address does not start with wit1 string."
    )


def test_top_100_failure_required():
    data = {"top_100_miners": [{}, {}, {}]}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data)
    key = "top_100_miners"
    assert (
        err_info.value.messages[key][0]["address"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][0]["amount"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["address"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["amount"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][2]["address"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][2]["amount"][0]
        == "Missing data for required field."
    )


def test_num_unique_success():
    data = {"num_unique_miners": 1000}
    NetworkStatisticsResponse().load(data)
    data = {"num_unique_data_request_solvers": 1000}
    NetworkStatisticsResponse().load(data)


@pytest.fixture
def data_requests():
    return {
        "histogram_data_requests": [
            {"total": 1000, "failure": 100},
            {"total": 1000, "failure": 10},
        ],
        "histogram_period": 1000,
    }


def test_histogram_data_requests_success(data_requests):
    NetworkStatisticsResponse().load(data_requests)


def test_histogram_data_requests_failure_period(data_requests):
    del data_requests["histogram_period"]
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data_requests)
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )


def test_histogram_data_requests_failure_missing(data_requests):
    data_requests["histogram_data_requests"][1] = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data_requests)
    assert (
        err_info.value.messages["histogram_data_requests"][1]["total"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["histogram_data_requests"][1]["failure"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def data_request_composition():
    return {
        "histogram_data_request_composition": [
            {"total": 1000, "http_get": 100, "http_post": 100, "rng": 0},
            {"total": 1000, "http_get": 100, "http_post": 1000, "rng": 10},
        ],
        "histogram_period": 1000,
    }


def test_histogram_data_request_composition_success(data_request_composition):
    NetworkStatisticsResponse().load(data_request_composition)


def test_histogram_data_request_composition_failure_period(data_request_composition):
    del data_request_composition["histogram_period"]
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data_request_composition)
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )


def test_histogram_data_request_composition_failure_missing(data_request_composition):
    key = "histogram_data_request_composition"
    data_request_composition[key][1] = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data_request_composition)
    assert (
        err_info.value.messages[key][1]["total"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["http_get"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["http_post"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["rng"][0] == "Missing data for required field."
    )


@pytest.fixture
def generic_dictionary():
    return [{"1": 10, "100": 10, "1000": 10}, {"1": 20, "100": 20, "1000": 20}]


def test_histogram_generic_dictionary_success(generic_dictionary):
    NetworkStatisticsResponse().load(
        {"histogram_data_request_witness": generic_dictionary, "histogram_period": 1000}
    )
    NetworkStatisticsResponse().load(
        {
            "histogram_data_request_collateral": generic_dictionary,
            "histogram_period": 1000,
        }
    )
    NetworkStatisticsResponse().load(
        {"histogram_data_request_reward": generic_dictionary, "histogram_period": 1000}
    )


def test_histogram_failure_period_missing(generic_dictionary):
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(
            {"histogram_data_request_witness": generic_dictionary}
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(
            {"histogram_data_request_collateral": generic_dictionary}
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(
            {"histogram_data_request_reward": generic_dictionary}
        )
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )


@pytest.fixture
def data_request_lie_rate():
    return {
        "histogram_data_request_lie_rate": [
            {
                "witnessing_acts": 100,
                "errors": 10,
                "no_reveal_lies": 10,
                "out_of_consensus_lies": 10,
            },
            {
                "witnessing_acts": 200,
                "errors": 20,
                "no_reveal_lies": 20,
                "out_of_consensus_lies": 20,
            },
        ],
        "histogram_period": 1000,
    }


def test_histogram_data_request_lie_rate_success(data_request_lie_rate):
    NetworkStatisticsResponse().load(data_request_lie_rate)


def test_histogram_data_request_lie_rate_failure_period(data_request_lie_rate):
    del data_request_lie_rate["histogram_period"]
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data_request_lie_rate)
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )


def test_histogram_data_request_lie_rate_failure_missing(data_request_lie_rate):
    key = "histogram_data_request_lie_rate"
    data_request_lie_rate[key][1] = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(data_request_lie_rate)
    assert (
        err_info.value.messages[key][1]["witnessing_acts"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["errors"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["no_reveal_lies"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages[key][1]["out_of_consensus_lies"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def supply_burn_rate():
    return {
        "histogram_burn_rate": [
            {"reverted": 100, "lies": 10},
            {"reverted": 100, "lies": 10},
        ],
        "histogram_period": 1000,
    }


def test_histogram_supply_burn_rate_success(supply_burn_rate):
    NetworkStatisticsResponse().load(supply_burn_rate)


def test_histogram_supply_burn_rate_failure_period(supply_burn_rate):
    del supply_burn_rate["histogram_period"]
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(supply_burn_rate)
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )


def test_histogram_supply_burn_rate_failure_missing(supply_burn_rate):
    supply_burn_rate["histogram_burn_rate"][1] = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(supply_burn_rate)
    assert (
        err_info.value.messages["histogram_burn_rate"][1]["reverted"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["histogram_burn_rate"][1]["lies"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def value_transfers():
    return {
        "histogram_value_transfers": [
            {"value_transfers": 100},
            {"value_transfers": 200},
        ],
        "histogram_period": 1000,
    }


def test_histogram_value_transfers_success(value_transfers):
    NetworkStatisticsResponse().load(value_transfers)


def test_histogram_value_transfers_failure_period(value_transfers):
    del value_transfers["histogram_period"]
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(value_transfers)
    assert (
        err_info.value.messages["_schema"][0]
        == "The histogram_period field is required when a histogram_* field is supplied."
    )


def test_histogram_value_transfers_failure_missing(value_transfers):
    value_transfers["histogram_value_transfers"][1] = {}
    with pytest.raises(ValidationError) as err_info:
        NetworkStatisticsResponse().load(value_transfers)
    assert (
        err_info.value.messages["histogram_value_transfers"][1]["value_transfers"][0]
        == "Missing data for required field."
    )
