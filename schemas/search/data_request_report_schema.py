from marshmallow import Schema, fields, validate

from schemas.component.commit_schema import CommitTransactionForDataRequest
from schemas.component.data_request_schema import DataRequestTransactionForApi
from schemas.component.reveal_schema import RevealTransactionForDataRequest
from schemas.component.tally_schema import TallyTransactionForDataRequest


class DataRequestReport(Schema):
    transaction_type = fields.Str(
        validate=validate.OneOf(["data_request", "commit", "reveal", "tally"]),
        required=True,
    )
    data_request = fields.Nested(DataRequestTransactionForApi, required=True)
    commits = fields.List(fields.Nested(CommitTransactionForDataRequest))
    reveals = fields.List(fields.Nested(RevealTransactionForDataRequest))
    tally = fields.Nested(TallyTransactionForDataRequest, allow_none=True)
