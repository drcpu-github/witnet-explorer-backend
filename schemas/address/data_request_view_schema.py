from marshmallow import fields, validate

from schemas.include.hash_schema import HashSchema


class DataRequestCreatedView(HashSchema):
    success = fields.Boolean(required=True)
    epoch = fields.Int(validate=validate.Range(min=0), required=True)
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)
    total_fee = fields.Int(validate=validate.Range(min=0), required=True)
    witnesses = fields.Int(validate=validate.Range(min=0, max=125), required=True)
    collateral = fields.Int(validate=validate.Range(min=0), required=True)
    consensus_percentage = fields.Int(validate=validate.Range(min=0), required=True)
    num_errors = fields.Int(validate=validate.Range(min=0, max=125), required=True)
    num_liars = fields.Int(validate=validate.Range(min=0, max=125), required=True)
    result = fields.Str(required=True)


class DataRequestSolvedView(HashSchema):
    success = fields.Boolean(required=True)
    epoch = fields.Int(validate=validate.Range(min=0), required=True)
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)
    collateral = fields.Int(validate=validate.Range(min=0), required=True)
    witness_reward = fields.Int(validate=validate.Range(min=0), required=True)
    reveal = fields.Str(required=True)
    error = fields.Boolean(required=True)
    liar = fields.Boolean(required=True)
