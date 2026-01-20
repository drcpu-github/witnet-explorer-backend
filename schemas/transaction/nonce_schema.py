from marshmallow import Schema, fields, validate

from schemas.include.validation_functions import is_valid_address


class TransactionNonceArgs(Schema):
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)


class TransactionNonceResponse(Schema):
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    nonce = fields.Int(validate=validate.Range(min=0), required=True)
