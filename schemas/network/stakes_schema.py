from marshmallow import Schema, fields, validate

from schemas.include.validation_functions import is_valid_address


class NetworkStakesArgs(Schema):
    validator = fields.Str(validate=is_valid_address)
    withdrawer = fields.Str(validate=is_valid_address)


class NetworkStake(Schema):
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    current_stake = fields.Int(validate=validate.Range(min=0), required=True)
    time_weighted_stake = fields.Int(validate=validate.Range(min=0), required=True)
    nonce = fields.Int(validate=validate.Range(min=0), required=True)
    blocks = fields.Int(validate=validate.Range(min=0), required=True)
    rewards = fields.Int(validate=validate.Range(min=0), required=True)
    data_requests = fields.Int(validate=validate.Range(min=0), required=True)
    lies = fields.Int(validate=validate.Range(min=0), required=True)
    genesis = fields.Int(validate=validate.Range(min=0), required=True)
    last_active = fields.Int(validate=validate.Range(min=0), required=True)


class NetworkStakesResponse(Schema):
    stakes = fields.List(fields.Nested(NetworkStake), required=True)
    last_updated = fields.Int(validate=validate.Range(min=0), required=True)
