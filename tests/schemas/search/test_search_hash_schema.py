import pytest
from marshmallow import ValidationError

from schemas.search.hash_schema import SearchHashArgs, SearchHashResponse


def test_search_hash_success():
    data = {"value": "68486981072edc18d6f0da44bb128e9c62ad3ffd877c4abc30a905fb7bd067b4"}
    SearchHashArgs().load(data)


def test_search_hash_failure_all():
    data = {"value": "0xabcdefghijklmnopqrstuvwxyz"}
    with pytest.raises(ValidationError) as err_info:
        SearchHashArgs().load(data)
    assert len(err_info.value.messages["value"]) == 2
    assert err_info.value.messages["value"][0] == "Hash does not contain 64 characters."
    assert err_info.value.messages["value"][1] == "Hash is not a hexadecimal value."


def test_search_hash_simple_success():
    data = {
        "value": "3e7a834a9817709dd0d63631aa62cb0306882f80f33da03effcd1fa41f3e46ab",
        "simple": "true",
    }
    SearchHashArgs().load(data)


def test_search_hash_failure_type():
    data = {
        "value": "399c06f98a8682b9689cd51099f2177844d2d34f1c90b2a3c50c630c8b6a6ef1",
        "simple": "abc",
    }
    with pytest.raises(ValidationError) as err_info:
        SearchHashArgs().load(data)
    assert err_info.value.messages["simple"][0] == "Not a valid boolean."


search_hash_response_types = [
    "pending",
    "block",
    "mint",
    "value_transfer",
    "data_request",
    "commit",
    "reveal",
    "tally",
    "data_request_report",
    "data_request_history",
]


def test_search_hash_response_failure_one_of():
    data = {
        "response_type": "pendin",
    }
    with pytest.raises(ValidationError) as err_info:
        SearchHashResponse().load(data)
    assert (
        err_info.value.messages["response_type"][0]
        == f"Must be one of: {', '.join(search_hash_response_types)}."
    )


def test_search_hash_response_failure_response_type(block_for_api):
    data = {
        "block": block_for_api,
    }
    with pytest.raises(ValidationError) as err_info:
        SearchHashResponse().load(data)
    assert (
        err_info.value.messages["response_type"][0]
        == "Missing data for required field."
    )


def test_search_hash_response_block_success(block_for_api):
    data = {
        "response_type": "block",
        "block": block_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_mint_success(mint_transaction_for_api):
    data = {
        "response_type": "mint",
        "mint": mint_transaction_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_value_transfer_success(
    value_transfer_transaction_for_api,
):
    data = {
        "response_type": "value_transfer",
        "value_transfer": value_transfer_transaction_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_data_request_success(data_request_transaction_for_api):
    data = {
        "response_type": "data_request",
        "data_request": data_request_transaction_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_commit_success(commit_transaction_for_api):
    data = {
        "response_type": "commit",
        "commit": commit_transaction_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_reveal_success(reveal_transaction_for_api):
    data = {
        "response_type": "reveal",
        "reveal": reveal_transaction_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_tally_success(tally_transaction_for_api):
    data = {
        "response_type": "tally",
        "tally": tally_transaction_for_api,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_data_request_history_success(data_request_history):
    data = {
        "response_type": "data_request_history",
        "data_request_history": data_request_history,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_data_request_report_success(data_request_report):
    data = {
        "response_type": "data_request_report",
        "data_request_report": data_request_report,
    }
    SearchHashResponse().load(data)


def test_search_hash_response_failure_type(value_transfer_transaction_for_api):
    data = {
        "response_type": "block",
        "value_transfer": value_transfer_transaction_for_api,
    }
    with pytest.raises(ValidationError) as err_info:
        SearchHashResponse().load(data)
    assert (
        err_info.value.messages["_schema"][0]
        == "Data for response type not found in response."
    )
