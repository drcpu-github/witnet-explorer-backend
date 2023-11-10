from marshmallow import fields, validate

from schemas.include.base_transaction_schema import BaseTransaction, TimestampComponent
from schemas.include.validation_functions import is_valid_address


class MintView(BaseTransaction, TimestampComponent):
    miner = fields.Str(validate=is_valid_address, required=True)
    output_value = fields.Int(validate=validate.Range(min=1), required=True)
    confirmed = fields.Boolean(required=True)
