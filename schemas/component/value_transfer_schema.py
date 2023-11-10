from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.base_transaction_schema import (
    BaseApiTransaction,
    BaseTransaction,
    TimestampComponent,
)
from schemas.include.input_utxo_schema import InputUtxo, InputUtxoList, InputUtxoPointer
from schemas.include.validation_functions import is_valid_address


class AddressOutput(Schema):
    address = fields.Str(validate=is_valid_address, required=True)
    value = fields.Int(validate=validate.Range(min=1), required=True)
    timelock = fields.Int(validate=validate.Range(min=0), required=True)
    locked = fields.Boolean(required=True)


class ValueTransferTransactionForApi(BaseApiTransaction):
    input_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    input_utxos = fields.List(fields.Nested(InputUtxoPointer), required=True)
    inputs_merged = fields.List(fields.Nested(InputUtxo), required=True)
    output_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    output_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )
    timelocks = fields.List(fields.Int(validate=validate.Range(min=0)), required=True)
    utxos = fields.List(fields.Nested(AddressOutput), required=True)
    utxos_merged = fields.List(fields.Nested(AddressOutput), required=True)
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    value = fields.Int(validate=validate.Range(min=0), required=True)
    priority = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)
    true_output_addresses = fields.List(
        fields.Str(validate=is_valid_address), required=True
    )
    change_output_addresses = fields.List(
        fields.Str(validate=is_valid_address), required=True
    )
    true_value = fields.Int(validate=validate.Range(min=0), required=True)
    change_value = fields.Int(validate=validate.Range(min=0), required=True)

    @validates_schema
    def validate_changes(self, args, **kwargs):
        if len(args["input_addresses"]) != len(args["input_utxos"]):
            raise ValidationError("Number of input addresses and utxos is different.")
        if not (
            len(args["output_addresses"])
            == len(args["output_values"])
            == len(args["timelocks"])
        ):
            raise ValidationError(
                "Number of output addresses, values and timelocks is different."
            )
        if set(args["output_addresses"]) != set(
            args["true_output_addresses"] + args["change_output_addresses"]
        ):
            raise ValidationError(
                "Output addresses do not match, output_addresses != true_output_addresses + change_output_addresses."
            )
        if args["value"] != args["true_value"] + args["change_value"]:
            raise ValidationError(
                "Values do not match: value != true_value + change_value."
            )
        if sum(args["output_values"]) != args["value"]:
            raise ValidationError(
                "Sum of output values is different from the value field."
            )


class ValueTransferTransactionForBlock(BaseTransaction, TimestampComponent):
    unique_input_addresses = fields.List(
        fields.Str(validate=is_valid_address), required=True
    )
    true_output_addresses = fields.List(
        fields.Str(validate=is_valid_address), required=True
    )
    true_value = fields.Int(validate=validate.Range(min=0), required=True)
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)
    priority = fields.Int(validate=validate.Range(min=0), required=True)

    @validates_schema
    def validate(self, args, **kwargs):
        if len(args["unique_input_addresses"]) < 1:
            raise ValidationError("Need at least one input address.")


class ValueTransferTransactionForExplorer(BaseTransaction, InputUtxoList):
    input_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    input_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )
    output_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    output_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )
    timelocks = fields.List(fields.Int(validate=validate.Range(min=0)), required=True)
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)

    @validates_schema
    def validate(self, args, **kwargs):
        if not (
            len(args["input_addresses"])
            == len(args["input_values"])
            == len(args["input_utxos"])
        ):
            raise ValidationError(
                "Number of input addresses, values and UTXO's is different."
            )
        if not (
            len(args["output_addresses"])
            == len(args["output_values"])
            == len(args["timelocks"])
        ):
            raise ValidationError(
                "Number of output addresses, values and timelocks is different."
            )
