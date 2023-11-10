from marshmallow import Schema, fields, validate

from schemas.include.validation_functions import is_valid_hash


class TransactionMempoolArgs(Schema):
    transaction_type = fields.Str(
        validate=validate.OneOf(["all", "data_requests", "value_transfers"]),
        data_key="type",
        attribute="type",
        required=True,
    )


class TransactionMempoolResponse(Schema):
    data_request = fields.List(fields.Str(validate=is_valid_hash))
    value_transfer = fields.List(fields.Str(validate=is_valid_hash))
