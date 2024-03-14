import pytest
from marshmallow import ValidationError

from schemas.component.block_schema import (
    BlockDetails,
    BlockForApi,
    BlockForExplorer,
    BlockTransactionsForApi,
    BlockTransactionsForExplorer,
)


@pytest.fixture
def block_details():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "data_request_weight": 1000,
        "value_transfer_weight": 1000,
        "weight": 0,
        "confirmed": True,
        "reverted": False,
    }


def test_block_details_success(block_details):
    BlockDetails().load(block_details)


def test_block_details_failure_hash(block_details):
    block_details["hash"] = (
        "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    )
    with pytest.raises(ValidationError) as err_info:
        BlockDetails().load(block_details)
    assert err_info.value.messages["hash"][0] == "Hash is not a hexadecimal value."


def test_block_details_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockDetails().load(data)
    assert len(err_info.value.messages) == 8
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request_weight"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["value_transfer_weight"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["weight"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."


@pytest.fixture
def block_transactions_for_block(
    mint_transaction_for_block,
    value_transfer_transaction_for_block,
    data_request_transaction_for_block,
    commit_transaction_for_block,
    reveal_transaction_for_block,
    tally_transaction_for_block,
):
    return {
        "mint": mint_transaction_for_block,
        "value_transfer": [value_transfer_transaction_for_block],
        "data_request": [data_request_transaction_for_block],
        "commit": {
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789": [
                commit_transaction_for_block
            ],
        },
        "reveal": {
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789": [
                reveal_transaction_for_block
            ],
        },
        "tally": [tally_transaction_for_block],
        "number_of_commits": 0,
        "number_of_reveals": 0,
    }


def test_block_transactions_for_block_success(block_transactions_for_block):
    BlockTransactionsForApi().load(block_transactions_for_block)


def test_block_transactions_for_block_failure_hash(
    block_transactions_for_block,
    commit_transaction_for_block,
    reveal_transaction_for_block,
):
    faulty_hash = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    block_transactions_for_block["commit"] = {
        faulty_hash: [commit_transaction_for_block],
    }
    block_transactions_for_block["reveal"] = {
        faulty_hash: [reveal_transaction_for_block],
    }
    with pytest.raises(ValidationError) as err_info:
        BlockTransactionsForApi().load(block_transactions_for_block)
    assert (
        err_info.value.messages["commit"][faulty_hash]["key"][0]
        == "Hash is not a hexadecimal value."
    )
    assert (
        err_info.value.messages["reveal"][faulty_hash]["key"][0]
        == "Hash is not a hexadecimal value."
    )


def test_block_transactions_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockTransactionsForApi().load(data)
    assert len(err_info.value.messages) == 8
    assert err_info.value.messages["mint"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["value_transfer"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["commit"][0] == "Missing data for required field."
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["tally"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["number_of_commits"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["number_of_reveals"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def block_transactions_for_explorer(
    mint_transaction_for_explorer,
    value_transfer_transaction_for_explorer,
    data_request_transaction_for_explorer,
    commit_transaction_for_explorer,
    reveal_transaction_for_explorer,
    tally_transaction_for_explorer,
):
    return {
        "mint": mint_transaction_for_explorer,
        "value_transfer": [value_transfer_transaction_for_explorer],
        "data_request": [data_request_transaction_for_explorer],
        "commit": [commit_transaction_for_explorer],
        "reveal": [reveal_transaction_for_explorer],
        "tally": [tally_transaction_for_explorer],
    }


def test_block_transactions_for_explorer_success(block_transactions_for_explorer):
    BlockTransactionsForExplorer().load(block_transactions_for_explorer)


def test_block_transactions_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockTransactionsForExplorer().load(data)
    assert len(err_info.value.messages) == 6
    assert err_info.value.messages["mint"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["value_transfer"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["commit"][0] == "Missing data for required field."
    assert err_info.value.messages["reveal"][0] == "Missing data for required field."
    assert err_info.value.messages["tally"][0] == "Missing data for required field."


@pytest.fixture
def block_for_api(block_details, block_transactions_for_block):
    return {
        "details": block_details,
        "transactions": block_transactions_for_block,
    }


def test_block_for_api_success(block_for_api):
    BlockForApi().load(block_for_api)


def test_block_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockForApi().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["details"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["transactions"][0] == "Missing data for required field."
    )


@pytest.fixture
def block_for_explorer(block_details, block_transactions_for_explorer):
    return {
        "details": block_details,
        "transactions": block_transactions_for_explorer,
        "tapi": [0] * 32,
    }


def test_block_for_explorer_success(block_for_explorer):
    BlockForExplorer().load(block_for_explorer)


def test_block_for_explorer_success_none(block_for_explorer):
    block_for_explorer["tapi"] = None
    BlockForExplorer().load(block_for_explorer)


def test_block_for_explorer_failure_tapi_length(block_for_explorer):
    block_for_explorer["tapi"] = []
    with pytest.raises(ValidationError) as err_info:
        BlockForExplorer().load(block_for_explorer)
    assert (
        err_info.value.messages["_schema"][0]
        == "TAPI signal vector does not have a length of 32."
    )


def test_block_for_explorer_failure_tapi_bits(block_for_explorer):
    block_for_explorer["tapi"] = [0, 1, 2]
    with pytest.raises(ValidationError) as err_info:
        BlockForExplorer().load(block_for_explorer)
    print(err_info.value.messages)
    assert (
        err_info.value.messages["tapi"][2][0]
        == "Must be greater than or equal to 0 and less than or equal to 1."
    )


def test_block_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        BlockForExplorer().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["details"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["transactions"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["tapi"][0] == "Missing data for required field."
