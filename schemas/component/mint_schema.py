from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.base_transaction_schema import BaseApiTransaction, BaseTransaction
from schemas.include.validation_functions import is_valid_address


class MintTransaction(Schema):
    miner = fields.Str(validate=is_valid_address, required=True)
    output_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    output_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )

    @validates_schema
    def validate_inputs(self, args, **kwargs):
        errors = {}
        if len(args["output_addresses"]) < 1:
            errors["output_addresses"] = "Need at least one output address."
        if len(args["output_addresses"]) != len(args["output_values"]):
            errors[
                "output_values"
            ] = "Number of output addresses and values is different."
        if len(errors):
            raise ValidationError(errors)


class MintTransactionForApi(BaseApiTransaction, MintTransaction):
    pass


class MintTransactionForBlock(BaseTransaction, MintTransaction):
    pass


class MintTransactionForExplorer(MintTransactionForBlock):
    pass
