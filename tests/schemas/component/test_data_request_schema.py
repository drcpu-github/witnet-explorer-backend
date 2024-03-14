import pytest
from marshmallow import ValidationError

from schemas.component.data_request_schema import (
    DataRequest,
    DataRequestRetrieval,
    DataRequestTransactionForApi,
    DataRequestTransactionForBlock,
    DataRequestTransactionForExplorer,
)


@pytest.fixture
def data_request():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "input_addresses": [
            "wit100000000000000000000000000000000r0v4g2",
            "wit100000000000000000000000000000000r0v4g2",
        ],
        "witnesses": 10,
        "witness_reward": 100,
        "commit_and_reveal_fee": 1,
        "consensus_percentage": 70,
        "dro_fee": 1,
        "miner_fee": 1,
        "collateral": 1e9,
        "RAD_bytes_hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "DRO_bytes_hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "weight": 1,
    }


def test_data_request_success(data_request):
    DataRequest().load(data_request)


def test_data_request_failure_address(data_request):
    data_request["input_addresses"] = ["wit100000000000000000000000000000000r0v4g"]
    with pytest.raises(ValidationError) as err_info:
        DataRequest().load(data_request)
    assert (
        err_info.value.messages["input_addresses"][0][0]
        == "Address does not contain 42 characters."
    )


def test_data_request_failure_hash(data_request):
    data_request["RAD_bytes_hash"] = (
        "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"
    )
    data_request["DRO_bytes_hash"] = (
        "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    )
    with pytest.raises(ValidationError) as err_info:
        DataRequest().load(data_request)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["RAD_bytes_hash"][0]
        == "Hash does not contain 64 characters."
    )
    assert (
        err_info.value.messages["DRO_bytes_hash"][0]
        == "Hash is not a hexadecimal value."
    )


def test_data_request_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequest().load(data)
    assert len(err_info.value.messages) == 13
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["witnesses"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["witness_reward"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["commit_and_reveal_fee"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["consensus_percentage"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["dro_fee"][0] == "Missing data for required field."
    assert err_info.value.messages["miner_fee"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["RAD_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["DRO_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["weight"][0] == "Missing data for required field."


@pytest.fixture
def data_request_transaction_for_api(data_request):
    data_request.update(
        {
            "timestamp": 1602666090,
            "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
            "confirmed": True,
            "reverted": False,
            "input_utxos": [
                {
                    "address": "wit100000000000000000000000000000000r0v4g2",
                    "value": 1000,
                    "input_utxo": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0",
                },
                {
                    "address": "wit100000000000000000000000000000000r0v4g2",
                    "value": 1000,
                    "input_utxo": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789:0",
                },
            ],
            "priority": 1,
            "retrieve": [
                {
                    "kind": "HTTP-GET",
                    "url": "https://data.gateapi.io/api2/1/ticker/wit_usdt",
                    "headers": [""],
                    "body": "",
                    "script": "StringParseJSONMap().MapGetFloat(last).FloatMultiply(1000000).FloatRound()",
                },
            ],
            "aggregate": "filter(DeviationStandard, 1.4).reduce(AverageMedian)",
            "tally": "filter(DeviationStandard, 2.5).reduce(AverageMedian)",
        }
    )
    return data_request


def test_data_request_transaction_for_api_success(data_request_transaction_for_api):
    DataRequestTransactionForApi().load(data_request_transaction_for_api)


def test_data_request_transaction_for_api_failure_too_few_inputs(
    data_request_transaction_for_api,
):
    data_request_transaction_for_api["input_addresses"] = []
    data_request_transaction_for_api["input_utxos"] = []
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForApi().load(data_request_transaction_for_api)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["input_addresses"] == "Need at least one input address."
    )
    assert err_info.value.messages["input_utxos"] == "Need at least one input utxo."


def test_data_request_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForApi().load(data)
    assert len(err_info.value.messages) == 22
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["witnesses"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["witness_reward"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["commit_and_reveal_fee"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["consensus_percentage"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["dro_fee"][0] == "Missing data for required field."
    assert err_info.value.messages["miner_fee"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["RAD_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["DRO_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["priority"][0] == "Missing data for required field."
    assert err_info.value.messages["retrieve"][0] == "Missing data for required field."
    assert err_info.value.messages["aggregate"][0] == "Missing data for required field."
    assert err_info.value.messages["tally"][0] == "Missing data for required field."


@pytest.fixture
def data_request_transaction_for_block(data_request):
    data_request.update(
        {
            "kinds": ["HTTP-GET"],
            "urls": ["https://data.gateapi.io/api2/1/ticker/wit_usdt"],
            "headers": [[""]],
            "bodies": [""],
            "scripts": [
                "StringParseJSONMap().MapGetFloat(last).FloatMultiply(1000000).FloatRound()"
            ],
            "aggregate_filters": "filter(DeviationStandard, 1.4)",
            "aggregate_reducer": "reduce(AverageMedian)",
            "tally_filters": "filter(DeviationStandard, 2.5)",
            "tally_reducer": "reduce(AverageMedian)",
        }
    )
    return data_request


def test_data_request_transaction_for_block_success(data_request_transaction_for_block):
    DataRequestTransactionForBlock().load(data_request_transaction_for_block)


def test_data_request_transaction_for_block_success_none(
    data_request_transaction_for_block,
):
    data_request_transaction_for_block["urls"] = [None]
    DataRequestTransactionForBlock().load(data_request_transaction_for_block)


def test_data_request_transaction_for_block_failure_kind(
    data_request_transaction_for_block,
):
    data_request_transaction_for_block["kinds"] = ["RN"]
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForBlock().load(data_request_transaction_for_block)
    assert (
        err_info.value.messages["kinds"][0][0]
        == "Must be one of: HTTP-GET, HTTP-POST, RNG."
    )


def test_data_request_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 22
    assert err_info.value.messages["kinds"][0] == "Missing data for required field."
    assert err_info.value.messages["urls"][0] == "Missing data for required field."
    assert err_info.value.messages["headers"][0] == "Missing data for required field."
    assert err_info.value.messages["bodies"][0] == "Missing data for required field."
    assert err_info.value.messages["scripts"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["aggregate_filters"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["aggregate_reducer"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["tally_filters"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["tally_reducer"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["witnesses"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["witness_reward"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["commit_and_reveal_fee"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["consensus_percentage"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["dro_fee"][0] == "Missing data for required field."
    assert err_info.value.messages["miner_fee"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["RAD_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["DRO_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["weight"][0] == "Missing data for required field."


@pytest.fixture
def data_request_transaction_for_explorer(data_request):
    data_request.update(
        {
            "input_utxos": [
                (
                    bytearray.fromhex(
                        "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                    ),
                    0,
                ),
                (
                    bytearray.fromhex(
                        "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
                    ),
                    1,
                ),
            ],
            "input_values": [1000, 1000],
            "output_address": "wit100000000000000000000000000000000r0v4g2",
            "output_value": 100,
            "kinds": ["HTTP-GET"],
            "urls": ["https://data.gateapi.io/api2/1/ticker/wit_usdt"],
            "headers": [[""]],
            "bodies": [bytearray()],
            "scripts": [
                bytearray(
                    # fmt: off
                    [
                        132, 24, 119, 130, 24, 100, 100, 108, 97, 115, 116, 130, 24, 87,
                        26, 0, 15, 66, 64, 24, 91,
                    ]
                    # fmt: on
                )
            ],
            "aggregate_filters": [
                (5, bytearray([251, 63, 246, 102, 102, 102, 102, 102, 102]))
            ],
            "aggregate_reducer": [5],
            "tally_filters": [(5, bytearray([250, 64, 32, 0, 0]))],
            "tally_reducer": [5],
        }
    )
    return data_request


def test_data_request_transaction_for_explorer_success(
    data_request_transaction_for_explorer,
):
    DataRequestTransactionForExplorer().load(data_request_transaction_for_explorer)


def test_data_request_transaction_for_explorer_success_none(
    data_request_transaction_for_explorer,
):
    data_request_transaction_for_explorer["output_address"] = None
    data_request_transaction_for_explorer["output_value"] = None
    data_request_transaction_for_explorer["urls"] = [None]
    DataRequestTransactionForExplorer().load(data_request_transaction_for_explorer)


def test_data_request_transaction_for_explorer_failure_address(
    data_request_transaction_for_explorer,
):
    data_request_transaction_for_explorer["output_address"] = (
        "wit100000000000000000000000000000000r0v4g"
    )
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForExplorer().load(data_request_transaction_for_explorer)
    assert (
        err_info.value.messages["output_address"][0]
        == "Address does not contain 42 characters."
    )


def test_data_request_transaction_for_explorer_failure_kind(
    data_request_transaction_for_explorer,
):
    data_request_transaction_for_explorer["kinds"] = ["HTTP"]
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForExplorer().load(data_request_transaction_for_explorer)
    assert (
        err_info.value.messages["kinds"][0][0]
        == "Must be one of: HTTP-GET, HTTP-POST, RNG."
    )


def test_data_request_transaction_for_explorer_failure_url(
    data_request_transaction_for_explorer,
):
    data_request_transaction_for_explorer["urls"] = [
        "data.gateapi.io/api2/1/ticker/wit_usdt"
    ]
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForExplorer().load(data_request_transaction_for_explorer)
    assert err_info.value.messages["urls"][0][0] == "Not a valid URL."


def test_data_request_transaction_for_explorer_failure_bytearray_field(
    data_request_transaction_for_explorer,
):
    data_request_transaction_for_explorer["bodies"] = [""]
    data_request_transaction_for_explorer["scripts"] = [""]
    data_request_transaction_for_explorer["aggregate_filters"] = [(5, "")]
    data_request_transaction_for_explorer["tally_filters"] = [(5, "")]
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForExplorer().load(data_request_transaction_for_explorer)
    assert len(err_info.value.messages) == 4
    assert err_info.value.messages["bodies"][0][0] == "Input type is not a bytearray."
    assert err_info.value.messages["scripts"][0][0] == "Input type is not a bytearray."
    assert (
        err_info.value.messages["aggregate_filters"][0][1][0]
        == "Input type is not a bytearray."
    )
    assert (
        err_info.value.messages["tally_filters"][0][1][0]
        == "Input type is not a bytearray."
    )


def test_data_request_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 26
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["witnesses"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["witness_reward"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["commit_and_reveal_fee"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["consensus_percentage"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["dro_fee"][0] == "Missing data for required field."
    assert err_info.value.messages["miner_fee"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["collateral"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["RAD_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["DRO_bytes_hash"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["input_utxos"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["input_values"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_address"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_value"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["kinds"][0] == "Missing data for required field."
    assert err_info.value.messages["urls"][0] == "Missing data for required field."
    assert err_info.value.messages["headers"][0] == "Missing data for required field."
    assert err_info.value.messages["bodies"][0] == "Missing data for required field."
    assert err_info.value.messages["scripts"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["aggregate_filters"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["aggregate_reducer"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["tally_filters"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["tally_reducer"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def data_request_retrieval():
    return {
        "kind": "HTTP-GET",
        "url": "https://data.gateapi.io/api2/1/ticker/wit_usdt",
        "headers": [""],
        "body": "",
        "script": "StringParseJSONMap().MapGetFloat(last).FloatMultiply(1000000).FloatRound()",
    }


def test_data_request_retrieval_success(data_request_retrieval):
    DataRequestRetrieval().load(data_request_retrieval)


def test_data_request_retrieval_success_none(data_request_retrieval):
    data_request_retrieval["url"] = None
    DataRequestRetrieval().load(data_request_retrieval)


def test_data_request_retrieval_failure_kind(data_request_retrieval):
    data_request_retrieval["kind"] = "HTTP"
    with pytest.raises(ValidationError) as err_info:
        DataRequestRetrieval().load(data_request_retrieval)
    assert (
        err_info.value.messages["kind"][0]
        == "Must be one of: HTTP-GET, HTTP-POST, RNG."
    )


def test_data_request_retrieval_failure_url(data_request_retrieval):
    data_request_retrieval["url"] = "data.gateapi.io/api2/1/ticker/wit_usdt"
    with pytest.raises(ValidationError) as err_info:
        DataRequestRetrieval().load(data_request_retrieval)
    assert err_info.value.messages["url"][0] == "Not a valid URL."


def test_data_request_retrieval_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestRetrieval().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["kind"][0] == "Missing data for required field."
    assert err_info.value.messages["url"][0] == "Missing data for required field."
    assert err_info.value.messages["headers"][0] == "Missing data for required field."
    assert err_info.value.messages["body"][0] == "Missing data for required field."
    assert err_info.value.messages["script"][0] == "Missing data for required field."
