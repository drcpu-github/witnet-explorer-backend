from marshmallow import ValidationError, fields, validate, validates_schema

from schemas.include.address_schema import AddressSchema
from schemas.include.base_transaction_schema import BaseApiTransaction, BaseTransaction
from schemas.include.input_utxo_schema import InputUtxoList, InputUtxoPointer
from schemas.include.validation_functions import is_valid_hash


class CommitTransactionForApi(BaseApiTransaction, AddressSchema):
    input_utxos = fields.List(fields.Nested(InputUtxoPointer), required=True)
    output_value = fields.Int(validate=validate.Range(min=0), required=True)


class CommitTransactionForBlock(BaseTransaction, AddressSchema):
    collateral = fields.Int(validate=validate.Range(min=1e9), required=True)
    data_request = fields.Str(validate=is_valid_hash, required=True)


class CommitTransactionForDataRequest(BaseApiTransaction, AddressSchema):
    pass


class CommitTransactionForExplorer(CommitTransactionForBlock, InputUtxoList):
    input_values = fields.List(
        fields.Integer(validate=validate.Range(min=0)), required=True
    )
    output_value = fields.Int(
        validate=validate.Range(min=1), allow_none=True, required=True
    )

    @validates_schema
    def validate_inputs(self, args, **kwargs):
        errors = {}
        if len(args["input_values"]) < 1:
            errors["input_values"] = "Need at least one input value."
        if len(args["input_utxos"]) != len(args["input_values"]):
            errors["input_utxos"] = (
                "Number of input UTXO's and input values is different."
            )
        if len(errors) > 0:
            raise ValidationError(errors)
