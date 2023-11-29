from marshmallow import Schema, fields, validate


class DetailsView(Schema):
    balance = fields.Int(validate=validate.Range(min=0), required=True)
    reputation = fields.Int(validate=validate.Range(min=0), required=True)
    eligibility = fields.Int(validate=validate.Range(min=0), required=True)
    total_reputation = fields.Int(validate=validate.Range(min=0), required=True)
    label = fields.String(allow_none=True, required=True)
