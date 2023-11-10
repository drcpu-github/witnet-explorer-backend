import pytest
from marshmallow import ValidationError

from schemas.search.data_request_report_schema import DataRequestReport


@pytest.fixture
def data_request_report(
    data_request_transaction_for_api,
    commit_transaction_for_data_request,
    reveal_transaction_for_data_request,
    tally_transaction_for_data_request,
):
    return {
        "transaction_type": "data_request",
        "data_request": data_request_transaction_for_api,
        "commits": [
            commit_transaction_for_data_request,
            commit_transaction_for_data_request,
        ],
        "reveals": [
            reveal_transaction_for_data_request,
            reveal_transaction_for_data_request,
        ],
        "tally": tally_transaction_for_data_request,
    }


def test_data_request_report_success(data_request_report):
    DataRequestReport().load(data_request_report)


def test_data_request_report_failure_type(data_request_report):
    data_request_report["transaction_type"] = "data_request_txn"
    with pytest.raises(ValidationError) as err_info:
        DataRequestReport().load(data_request_report)
    assert (
        err_info.value.messages["transaction_type"][0]
        == "Must be one of: data_request, commit, reveal, tally."
    )


def test_data_request_report_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        DataRequestReport().load(data)
    assert len(err_info.value.messages) == 2
    assert (
        err_info.value.messages["transaction_type"][0]
        == "Missing data for required field."
    )
    assert (
        err_info.value.messages["data_request"][0] == "Missing data for required field."
    )
