from marshmallow import fields, validate

from schemas.include.hash_schema import HashSchema
from schemas.include.validation_functions import is_valid_address


class BlockView(HashSchema):
    miner = fields.Str(validate=is_valid_address, required=True)
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)
    epoch = fields.Int(validate=validate.Range(min=0), required=True)
    block_reward = fields.Int(validate=validate.Range(min=0), required=True)
    block_fees = fields.Int(validate=validate.Range(min=0), required=True)
    value_transfers = fields.Int(validate=validate.Range(min=0), required=True)
    data_requests = fields.Int(validate=validate.Range(min=0), required=True)
    commits = fields.Int(validate=validate.Range(min=0), required=True)
    reveals = fields.Int(validate=validate.Range(min=0), required=True)
    tallies = fields.Int(validate=validate.Range(min=0), required=True)
    confirmed = fields.Boolean(required=True)
