from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.base_transaction_schema import (
    BaseApiTransaction,
    BaseTransaction,
    TimestampComponent,
)
from schemas.include.bytearray_field import BytearrayField
from schemas.include.input_utxo_schema import InputUtxoList, InputUtxoPointer
from schemas.include.validation_functions import is_valid_address, is_valid_hash


class DataRequest(BaseTransaction):
    input_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    witnesses = fields.Int(validate=validate.Range(min=0, max=125), required=True)
    witness_reward = fields.Int(validate=validate.Range(min=0), required=True)
    commit_and_reveal_fee = fields.Int(validate=validate.Range(min=0), required=True)
    consensus_percentage = fields.Int(
        validate=validate.Range(min=50, max=100), required=True
    )
    dro_fee = fields.Int(validate=validate.Range(min=0), required=True)
    miner_fee = fields.Int(validate=validate.Range(min=0), required=True)
    collateral = fields.Int(validate=validate.Range(min=1000000000), required=True)
    RAD_bytes_hash = fields.Str(validate=is_valid_hash, required=True)
    DRO_bytes_hash = fields.Str(validate=is_valid_hash, required=True)
    weight = fields.Integer(validate=validate.Range(min=1), required=True)


class DataRequestRetrieval(Schema):
    kind = fields.Str(
        validate=validate.OneOf(["HTTP-GET", "HTTP-POST", "RNG"]), required=True
    )
    url = fields.URL(allow_none=True, required=True)
    headers = fields.List(fields.Str(), required=True)
    body = fields.String(required=True)
    script = fields.String(required=True)


class DataRequestTransactionForApi(BaseApiTransaction, DataRequest, TimestampComponent):
    input_utxos = fields.List(fields.Nested(InputUtxoPointer), required=True)
    priority = fields.Integer(validate=validate.Range(min=0), required=True)
    retrieve = fields.List(fields.Nested(DataRequestRetrieval), required=True)
    aggregate = fields.Str(required=True)
    tally = fields.Str(required=True)

    @validates_schema
    def validate_inputs(self, args, **kwargs):
        errors = {}
        if len(args["input_addresses"]) < 1:
            errors["input_addresses"] = "Need at least one input address."
        if len(args["input_utxos"]) < 1:
            errors["input_utxos"] = "Need at least one input utxo."
        if len(errors):
            raise ValidationError(errors)


class DataRequestTransactionForBlock(DataRequest):
    kinds = fields.List(
        fields.Str(validate=validate.OneOf(["HTTP-GET", "HTTP-POST", "RNG"])),
        required=True,
    )
    urls = fields.List(fields.URL(allow_none=True), required=True)
    headers = fields.List(fields.List(fields.Str()), required=True)
    bodies = fields.List(fields.Str(), required=True)
    scripts = fields.List(fields.Str(), required=True)
    aggregate_filters = fields.Str(required=True)
    aggregate_reducer = fields.Str(required=True)
    tally_filters = fields.Str(required=True)
    tally_reducer = fields.Str(required=True)


class DataRequestTransactionForExplorer(DataRequest, InputUtxoList):
    input_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )
    output_address = fields.Str(
        validate=is_valid_address, allow_none=True, required=True
    )
    output_value = fields.Int(
        validate=validate.Range(min=0), allow_none=True, required=True
    )
    kinds = fields.List(
        fields.Str(validate=validate.OneOf(["HTTP-GET", "HTTP-POST", "RNG"])),
        required=True,
    )
    urls = fields.List(fields.URL(allow_none=True), required=True)
    headers = fields.List(fields.List(fields.Str()), required=True)
    bodies = fields.List(BytearrayField(), required=True)
    scripts = fields.List(BytearrayField(), required=True)
    aggregate_filters = fields.List(
        fields.Tuple(
            (
                fields.Int(validate=validate.Range(min=0), required=True),
                BytearrayField(required=True),
            )
        ),
        required=True,
    )
    aggregate_reducer = fields.List(
        fields.Int(validate=validate.Range(min=0)), required=True
    )
    tally_filters = fields.List(
        fields.Tuple(
            (
                fields.Int(validate=validate.Range(min=0), required=True),
                BytearrayField(required=True),
            )
        ),
        required=True,
    )
    tally_reducer = fields.List(
        fields.Int(validate=validate.Range(min=0)), required=True
    )

    @validates_schema
    def validate_inputs(self, args, **kwargs):
        if len(args["input_utxos"]) < 1:
            raise ValidationError("Need at least one input UTXO.")
        if len(args["input_values"]) < 1:
            raise ValidationError("Need at least one input value.")
        if not (
            len(args["input_addresses"])
            == len(args["input_utxos"])
            == len(args["input_values"])
        ):
            raise ValidationError(
                "Number of input addresses, UTXO's and values is different."
            )
