import pytest
from marshmallow import ValidationError

from schemas.transaction.nonce_schema import (
    TransactionNonceArgs,
    TransactionNonceResponse,
)
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def nonce_args():
    return {
        "validator": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        "withdrawer": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
    }


def test_transaction_nonce_args_success(nonce_args):
    TransactionNonceArgs().load(nonce_args)


def test_transaction_nonce_args_failure_address(nonce_args):
    generic_address_test(
        nonce_args,
        ("validator", "withdrawer"),
        TransactionNonceArgs,
    )


def test_transaction_nonce_args_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TransactionNonceArgs().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )


@pytest.fixture
def nonce_response():
    return {
        "validator": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        "withdrawer": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        "nonce": 1,
    }


def test_transaction_nonce_response_success(nonce_response):
    TransactionNonceResponse().load(nonce_response)


def test_transaction_nonce_response_failure_address(nonce_response):
    generic_address_test(
        nonce_response,
        ("validator", "withdrawer"),
        TransactionNonceResponse,
    )


def test_transaction_nonce_response_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TransactionNonceResponse().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["validator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawer"][0] == "Missing data for required field."
    )
    assert err_info.value.messages["nonce"][0] == "Missing data for required field."
