from marshmallow import Schema, fields, validate

from schemas.include.address_schema import AddressSchema


class Reputation(AddressSchema):
    reputation = fields.Int(required=True, validate=validate.Range(min=0))
    eligibility = fields.Float(required=True, validate=validate.Range(min=0))


class NetworkReputationResponse(Schema):
    reputation = fields.List(fields.Nested(Reputation), required=True)
    total_reputation = fields.Int(required=True, validate=validate.Range(min=0))
    last_updated = fields.Int(required=True, validate=validate.Range(min=0))
