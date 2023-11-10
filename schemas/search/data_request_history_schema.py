from marshmallow import Schema, fields, validate

from schemas.component.data_request_schema import DataRequestRetrieval
from schemas.include.base_transaction_schema import TimestampComponent
from schemas.include.validation_functions import is_valid_hash


class DataRequestHistoryEntry(TimestampComponent):
    success = fields.Boolean(required=True)
    data_request = fields.String(validate=is_valid_hash, required=True)
    witnesses = fields.Integer(validate=validate.Range(min=1, max=125))
    witness_reward = fields.Integer(validate=validate.Range(min=0))
    collateral = fields.Integer(validate=validate.Range(min=1e9))
    consensus_percentage = fields.Integer(validate=validate.Range(min=50, max=100))
    num_errors = fields.Integer(validate=validate.Range(min=0), required=True)
    num_liars = fields.Integer(validate=validate.Range(min=0), required=True)
    result = fields.Str(required=True)
    confirmed = fields.Boolean(required=True)
    reverted = fields.Boolean(required=True)


class DataRequestHistoryParameters(Schema):
    witnesses = fields.Integer(validate=validate.Range(min=1, max=125), required=True)
    witness_reward = fields.Integer(validate=validate.Range(min=0), required=True)
    collateral = fields.Integer(validate=validate.Range(min=1e9), required=True)
    consensus_percentage = fields.Integer(
        validate=validate.Range(min=50, max=100), required=True
    )


class DataRequestHistoryRAD(Schema):
    retrieve = fields.List(fields.Nested(DataRequestRetrieval), required=True)
    aggregate = fields.Str(required=True)
    tally = fields.Str(required=True)


class DataRequestHistory(Schema):
    hash_type = fields.String(
        validate=validate.OneOf(["RAD_bytes_hash", "DRO_bytes_hash"]), required=True
    )
    hash_value = fields.String(
        validate=is_valid_hash, data_key="hash", attribute="hash", required=True
    )
    history = fields.List(fields.Nested(DataRequestHistoryEntry), required=True)
    num_data_requests = fields.Int(validate=validate.Range(min=1), required=True)
    first_epoch = fields.Int(validate=validate.Range(min=0), required=True)
    last_epoch = fields.Int(validate=validate.Range(min=0), required=True)
    RAD_data = fields.Nested(DataRequestHistoryRAD, required=True)
    RAD_bytes_hash = fields.String(validate=is_valid_hash)
    data_request_parameters = fields.Nested(DataRequestHistoryParameters)
