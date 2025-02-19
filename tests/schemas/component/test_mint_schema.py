import pytest
from marshmallow import ValidationError

from schemas.component.mint_schema import (
    MintTransaction,
    MintTransactionForApi,
    MintTransactionForBlock,
    MintTransactionForExplorer,
)
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def mint_transaction():
    return {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "miner": "wit100000000000000000000000000000000r0v4g2",
        "output_addresses": [
            "wit100000000000000000000000000000000r0v4g2",
            "wit100000000000000000000000000000000r0v4g2",
        ],
        "output_values": [50000000000, 200000000000],
    }


def test_mint_transaction_success(mint_transaction):
    MintTransaction().load(mint_transaction)


def test_mint_transaction_failure_too_few_outputs(mint_transaction):
    mint_transaction["output_addresses"] = []
    mint_transaction["output_values"] = []
    with pytest.raises(ValidationError) as err_info:
        MintTransaction().load(mint_transaction)
    assert (
        err_info.value.messages["output_addresses"]
        == "Need at least one output address."
    )


def test_mint_transaction_failure_outputs_mismatch(mint_transaction):
    mint_transaction["output_addresses"] = [
        "wit100000000000000000000000000000000r0v4g2",
        "wit100000000000000000000000000000000r0v4g2",
    ]
    mint_transaction["output_values"] = [50000000000]
    with pytest.raises(ValidationError) as err_info:
        MintTransaction().load(mint_transaction)
    assert (
        err_info.value.messages["output_values"]
        == "Number of output addresses and values is different."
    )


def test_mint_transaction_failure_address(mint_transaction):
    mint_transaction["output_values"] = [1]
    generic_address_test(
        mint_transaction,
        (
            "miner",
            ("output_addresses",),
        ),
        MintTransaction,
    )


def test_mint_transaction_failure_wit2_epoch(mint_transaction):
    mint_transaction["epoch"] = 280
    mint_transaction["output_addresses"] = []
    mint_transaction["output_values"] = []
    with pytest.raises(ValidationError) as err_info:
        # Outputs are required before wit/2 is activated
        MintTransaction().load(mint_transaction)
    assert (
        err_info.value.messages["output_addresses"]
        == "Need at least one output address."
    )

    # No outputs are needed anymore after wit/2 is activated
    mint_transaction["epoch"] = 300
    MintTransaction().load(mint_transaction)


def test_mint_transaction_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        MintTransaction().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def mint_transaction_for_api(mint_transaction):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1_738_180_845,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
    }
    transaction.update(mint_transaction)
    return transaction


def test_mint_transaction_for_api_success(mint_transaction_for_api):
    MintTransactionForApi().load(mint_transaction_for_api)


def test_mint_transaction_for_api_failure_required():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        MintTransactionForApi().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def mint_transaction_for_block(mint_transaction):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
    }
    transaction.update(mint_transaction)
    return transaction


def test_mint_transaction_for_block_success(mint_transaction_for_block):
    MintTransactionForBlock().load(mint_transaction_for_block)


def test_mint_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        MintTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def mint_transaction_for_explorer(mint_transaction):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
    }
    transaction.update(mint_transaction)
    return transaction


def test_mint_transaction_for_explorer_success(mint_transaction_for_explorer):
    MintTransactionForExplorer().load(mint_transaction_for_explorer)


def test_mint_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        MintTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 5
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["miner"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )
