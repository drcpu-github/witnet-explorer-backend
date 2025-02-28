from marshmallow import fields, validate

from schemas.include.base_transaction_schema import BaseTransaction, TimestampComponent
from schemas.include.validation_functions import is_valid_address


class StakeView(BaseTransaction, TimestampComponent):
    direction = fields.Str(validate=validate.OneOf(["in", "out"]), required=True)
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    input_value = fields.Int(validate=validate.Range(min=0), required=True)
    stake_value = fields.Int(validate=validate.Range(min=0), required=True)
    confirmed = fields.Boolean(required=True)
