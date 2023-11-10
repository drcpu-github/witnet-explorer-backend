from marshmallow import Schema, fields, validate

from schemas.include.address_schema import AddressSchema


class AddressBalance(AddressSchema):
    balance = fields.Int(
        validate=validate.Range(min=1, max=2500000000),
        required=True,
    )
    label = fields.Str()


class NetworkBalancesResponse(Schema):
    balances = fields.List(fields.Nested(AddressBalance), required=True)
    total_items = fields.Int(required=True)
    total_balance_sum = fields.Int(
        validate=validate.Range(min=1, max=2500000000), required=True
    )
    last_updated = fields.Int(required=True)
