from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.hash_schema import HashSchema
from schemas.include.validation_functions import is_valid_hash


class TimestampComponent(Schema):
    epoch = fields.Int(validate=validate.Range(min=0), required=True)
    timestamp = fields.Int(validate=validate.Range(min=1_602_666_000), required=True)

    @validates_schema
    def validate_timestamp(self, args, **kwargs):
        if args["timestamp"] != 1_602_666_000 + (args["epoch"] + 1) * 45:
            raise ValidationError("Incorrect transaction timestamp.")


class BaseTransaction(HashSchema):
    epoch = fields.Int(validate=validate.Range(min=0), required=True)


class BaseApiTransaction(BaseTransaction, TimestampComponent):
    block = fields.Str(validate=is_valid_hash, required=True)
    confirmed = fields.Boolean(required=True)
    reverted = fields.Boolean(required=True)
