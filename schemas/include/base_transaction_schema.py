from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.hash_schema import HashSchema
from schemas.include.validation_functions import is_valid_hash
from util.blockchain_functions import calculate_timestamp_from_epoch


class TimestampComponent(Schema):
    epoch = fields.Int(validate=validate.Range(min=0), required=True)
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)

    @validates_schema
    def validate_timestamp(self, args, **kwargs):
        expected_timestamp = calculate_timestamp_from_epoch(args["epoch"])
        if args["timestamp"] != expected_timestamp:
            raise ValidationError(
                f"Incorrect transaction timestamp: got {args['timestamp']}, excepted {expected_timestamp}."
            )


class BaseTransaction(HashSchema):
    epoch = fields.Int(validate=validate.Range(min=0), required=True)


class BaseApiTransaction(BaseTransaction, TimestampComponent):
    block = fields.Str(validate=is_valid_hash, required=True)
    confirmed = fields.Boolean(required=True)
    reverted = fields.Boolean(required=True)
