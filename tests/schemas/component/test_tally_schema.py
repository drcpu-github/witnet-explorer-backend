import pytest
from marshmallow import ValidationError

from schemas.component.tally_schema import (
    TallyAddresses,
    TallyOutput,
    TallySummary,
    TallyTransactionForApi,
    TallyTransactionForBlock,
    TallyTransactionForDataRequest,
    TallyTransactionForExplorer,
)


@pytest.fixture
def tally_output():
    return {
        "output_addresses": [
            "wit100000000000000000000000000000000r0v4g2",
            "wit100000000000000000000000000000000r0v4g2",
        ],
        "output_values": [1000, 1000],
    }


def test_tally_output_success(tally_output):
    TallyOutput().load(tally_output)


def test_tally_output_failure_too_few_outputs(tally_output):
    tally_output["output_addresses"] = []
    with pytest.raises(ValidationError) as err_info:
        TallyOutput().load(tally_output)
    assert (
        err_info.value.messages["output_addresses"]
        == "Need at least one output address."
    )


def test_tally_otuput_failure_output_sizes(tally_output):
    tally_output["output_values"] = [1000]
    with pytest.raises(ValidationError) as err_info:
        TallyOutput().load(tally_output)
    assert (
        err_info.value.messages["output_values"]
        == "Size of output addresses and output values does not match."
    )


def test_tally_output_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallyOutput().load(data)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def tally_addresses():
    return {
        "error_addresses": ["wit100000000000000000000000000000000r0v4g2"],
        "liar_addresses": ["wit100000000000000000000000000000000r0v4g2"],
    }


def test_tally_addresses_success(tally_addresses):
    TallyAddresses().load(tally_addresses)


def test_tally_addresses_failure_address(tally_addresses):
    tally_addresses["error_addresses"] = ["wit100000000000000000000000000000000r0v4g"]
    tally_addresses["liar_addresses"] = ["wit100000000000000000000000000000000r0v4g"]
    with pytest.raises(ValidationError) as err_info:
        TallyAddresses().load(tally_addresses)
    assert (
        err_info.value.messages["error_addresses"][0][0]
        == "Address does not contain 42 characters."
    )
    assert (
        err_info.value.messages["liar_addresses"][0][0]
        == "Address does not contain 42 characters."
    )


def test_tally_addresses_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallyAddresses().load(data)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["error_addresses"][0]
        == "Missing data for required field."
    )


@pytest.fixture
def tally_summary():
    return {
        "num_error_addresses": 1,
        "num_liar_addresses": 1,
        "tally": "124205",
        "success": True,
    }


def test_tally_summary_success(tally_summary):
    TallySummary().load(tally_summary)


def test_tally_summary_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallySummary().load(data)
    assert len(err_info.value.messages) == 4
    assert (
        err_info.value.messages["num_error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_liar_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["tally"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."


@pytest.fixture
def tally_transaction_for_api(tally_output, tally_addresses, tally_summary):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
    }
    transaction.update(tally_output)
    transaction.update(tally_addresses)
    transaction.update(tally_summary)
    return transaction


def test_tally_transaction_for_api_success(tally_transaction_for_api):
    TallyTransactionForApi().load(tally_transaction_for_api)


def test_tally_transaction_for_api_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForApi().load(data)
    assert len(err_info.value.messages) == 15
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["liar_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_liar_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["tally"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."


@pytest.fixture
def tally_transaction_for_block(tally_output, tally_summary):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
    }
    transaction.update(tally_output)
    transaction.update(tally_summary)
    return transaction


def test_tally_transaction_for_block_success(tally_transaction_for_block):
    TallyTransactionForBlock().load(tally_transaction_for_block)


def test_tally_transaction_for_block_failure_hash(tally_transaction_for_block):
    tally_transaction_for_block[
        "data_request"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForBlock().load(tally_transaction_for_block)
    assert (
        err_info.value.messages["data_request"][0] == "Hash is not a hexadecimal value."
    )


def test_tally_transaction_for_block_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForBlock().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_liar_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["tally"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."


@pytest.fixture
def tally_transaction_for_data_request(tally_addresses, tally_summary):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "timestamp": 1602666090,
        "block": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "confirmed": True,
        "reverted": False,
    }
    transaction.update(tally_addresses)
    transaction.update(tally_summary)
    return transaction


def test_tally_transaction_for_data_request_success(tally_transaction_for_data_request):
    TallyTransactionForDataRequest().load(tally_transaction_for_data_request)


def test_tally_transaction_for_data_request_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForDataRequest().load(data)
    assert len(err_info.value.messages) == 12
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert err_info.value.messages["timestamp"][0] == "Missing data for required field."
    assert err_info.value.messages["block"][0] == "Missing data for required field."
    assert err_info.value.messages["confirmed"][0] == "Missing data for required field."
    assert err_info.value.messages["reverted"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["liar_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["num_liar_addresses"][0]
        == "Missing data for required field."
    )
    assert err_info.value.messages["tally"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."


@pytest.fixture
def tally_transaction_for_explorer(tally_output, tally_addresses):
    transaction = {
        "hash": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "epoch": 1,
        "data_request": "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789",
        "tally": bytearray([26, 0, 1, 229, 45]),
        "success": True,
    }
    transaction.update(tally_output)
    transaction.update(tally_addresses)
    return transaction


def test_tally_transaction_for_explorer_success(tally_transaction_for_explorer):
    TallyTransactionForExplorer().load(tally_transaction_for_explorer)


def test_tally_transaction_for_explorer_failure_hash(tally_transaction_for_explorer):
    tally_transaction_for_explorer[
        "data_request"
    ] = "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForExplorer().load(tally_transaction_for_explorer)
    assert (
        err_info.value.messages["data_request"][0] == "Hash is not a hexadecimal value."
    )


def test_tally_transaction_for_explorer_failure_bytearray(
    tally_transaction_for_explorer,
):
    tally_transaction_for_explorer["tally"] = ""
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForExplorer().load(tally_transaction_for_explorer)
    assert err_info.value.messages["tally"][0] == "Input type is not a bytearray."


def test_tally_transaction_for_explorer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TallyTransactionForExplorer().load(data)
    assert len(err_info.value.messages) == 9
    assert err_info.value.messages["hash"][0] == "Missing data for required field."
    assert err_info.value.messages["epoch"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["tally"][0] == "Missing data for required field."
    assert err_info.value.messages["success"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["output_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["output_values"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["error_addresses"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["liar_addresses"][0]
        == "Missing data for required field."
    )
