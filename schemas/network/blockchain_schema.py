from marshmallow import Schema, fields, validate

from schemas.include.validation_functions import is_valid_address, is_valid_hash


class BlockchainBlock(Schema):
    hash_value = fields.Str(
        validate=is_valid_hash,
        data_key="hash",
        attribute="hash",
        required=True,
    )
    miner = fields.Str(validate=is_valid_address, required=True)
    value_transfers = fields.Int(required=True, validate=validate.Range(min=0))
    data_requests = fields.Int(required=True, validate=validate.Range(min=0))
    commits = fields.Int(required=True, validate=validate.Range(min=0))
    reveals = fields.Int(required=True, validate=validate.Range(min=0))
    tallies = fields.Int(required=True, validate=validate.Range(min=0))
    fees = fields.Int(required=True, validate=validate.Range(min=0))
    epoch = fields.Int(required=True, validate=validate.Range(min=0))
    timestamp = fields.Int(required=True, validate=validate.Range(min=0))
    confirmed = fields.Boolean(required=True)


class NetworkBlockchainResponse(Schema):
    blockchain = fields.List(fields.Nested(BlockchainBlock), required=True)
    reverted = fields.List(fields.Int(), required=True)
    total_epochs = fields.Int(required=True, validate=validate.Range(min=0))
