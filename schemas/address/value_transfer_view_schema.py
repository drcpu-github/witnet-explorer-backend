from marshmallow import fields, validate

from schemas.include.base_transaction_schema import BaseTransaction, TimestampComponent
from schemas.include.validation_functions import is_valid_address


class ValueTransferView(BaseTransaction, TimestampComponent):
    direction = fields.Str(
        validate=validate.OneOf(["in", "out", "self"]), required=True
    )
    input_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    output_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    value = fields.Int(validate=validate.Range(min=0), required=True)
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)
    priority = fields.Int(validate=validate.Range(min=0), required=True)
    locked = fields.Boolean(required=True)
    confirmed = fields.Boolean(required=True)
