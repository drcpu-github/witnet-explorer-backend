import pytest
from marshmallow import ValidationError

from schemas.transaction.mempool_schema import (
    TransactionMempoolArgs,
    TransactionMempoolResponse,
)


def test_transaction_mempool_args_success():
    data = {"type": "all"}
    transaction_mempool = TransactionMempoolArgs().load(data)
    assert transaction_mempool["type"] == "all"

    data = {"type": "data_request"}
    transaction_mempool = TransactionMempoolArgs().load(data)
    assert transaction_mempool["type"] == "data_request"


def test_transaction_mempool_args_failure_not_one_off():
    data = {"type": "al"}
    with pytest.raises(ValidationError) as err_info:
        TransactionMempoolArgs().load(data)
    assert (
        err_info.value.messages["type"][0]
        == "Must be one of: all, data_request, value_transfer, stake, unstake."
    )


def test_transaction_mempool_response_success():
    data = {
        "data_request": [
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
        ],
        "value_transfer": [
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
        ],
        "stake": ["abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"],
        "unstake": ["abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"],
    }
    TransactionMempoolResponse().load(data)

    data = {"data_request": []}
    TransactionMempoolResponse().load(data)

    data = {"value_transfer": []}
    TransactionMempoolResponse().load(data)

    data = {"stake": []}
    TransactionMempoolResponse().load(data)

    data = {"unstake": []}
    TransactionMempoolResponse().load(data)


def test_transaction_mempool_response_failure_length():
    data = {
        "data_request": [
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"
        ],
        "value_transfer": [
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"
        ],
        "stake": ["abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"],
        "unstake": ["abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef012345678"],
    }
    with pytest.raises(ValidationError) as err_info:
        TransactionMempoolResponse().load(data)
    assert len(err_info.value.messages) == 4
    assert (
        err_info.value.messages["data_request"][0][0]
        == "Hash does not contain 64 characters."
    )
    assert (
        err_info.value.messages["value_transfer"][0][0]
        == "Hash does not contain 64 characters."
    )
    assert (
        err_info.value.messages["stake"][0][0] == "Hash does not contain 64 characters."
    )
    assert (
        err_info.value.messages["unstake"][0][0]
        == "Hash does not contain 64 characters."
    )


def test_transaction_mempool_response_failure_hexadecimal():
    data = {
        "data_request": [
            "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
        ],
        "value_transfer": [
            "zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"
        ],
        "stake": ["zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"],
        "unstake": ["zbcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdef0123456789"],
    }
    with pytest.raises(ValidationError) as err_info:
        TransactionMempoolResponse().load(data)
    assert len(err_info.value.messages) == 4
    assert (
        err_info.value.messages["data_request"][0][0]
        == "Hash is not a hexadecimal value."
    )
    assert (
        err_info.value.messages["value_transfer"][0][0]
        == "Hash is not a hexadecimal value."
    )
    assert err_info.value.messages["stake"][0][0] == "Hash is not a hexadecimal value."
    assert (
        err_info.value.messages["unstake"][0][0] == "Hash is not a hexadecimal value."
    )
