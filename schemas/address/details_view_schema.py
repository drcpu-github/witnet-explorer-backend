from marshmallow import Schema, fields, validate


class DetailsView(Schema):
    balance = fields.Int(validate=validate.Range(min=0), required=True)
    staked_validator = fields.Int(validate=validate.Range(min=0), required=True)
    staked_withdrawer = fields.Int(validate=validate.Range(min=0), required=True)
    label = fields.String(allow_none=True, required=True)
