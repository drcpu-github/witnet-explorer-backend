from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.base_transaction_schema import BaseApiTransaction, BaseTransaction
from schemas.include.bytearray_field import BytearrayField
from schemas.include.validation_functions import is_valid_address, is_valid_hash


class TallyOutput(Schema):
    output_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    output_values = fields.List(
        fields.Integer(validate=validate.Range(min=0)), required=True
    )

    @validates_schema
    def validate_sizes(self, args, **kwargs):
        errors = {}
        if len(args["output_addresses"]) < 1:
            errors["output_addresses"] = "Need at least one output address."
        if len(args["output_addresses"]) != len(args["output_values"]):
            errors[
                "output_values"
            ] = "Size of output addresses and output values does not match."
        if len(errors):
            raise ValidationError(errors)


class TallyAddresses(Schema):
    error_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    liar_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)


class TallySummary(Schema):
    num_error_addresses = fields.Integer(validate=validate.Range(min=0), required=True)
    num_liar_addresses = fields.Integer(validate=validate.Range(min=0), required=True)
    tally = fields.String(required=True)
    success = fields.Boolean(required=True)


class TallyTransactionForApi(
    BaseApiTransaction, TallyOutput, TallyAddresses, TallySummary
):
    data_request = fields.Str(validate=is_valid_hash, required=True)


class TallyTransactionForBlock(BaseTransaction, TallyOutput, TallySummary):
    data_request = fields.Str(validate=is_valid_hash, required=True)


class TallyTransactionForDataRequest(BaseApiTransaction, TallyAddresses, TallySummary):
    pass


class TallyTransactionForExplorer(BaseTransaction, TallyOutput, TallyAddresses):
    data_request = fields.Str(validate=is_valid_hash, required=True)
    tally = BytearrayField(required=True)
    success = fields.Boolean(required=True)
