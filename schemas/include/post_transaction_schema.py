from marshmallow import (
    Schema,
    ValidationError,
    fields,
    post_load,
    validate,
    validates_schema,
)

from schemas.include.output_pointer_schema import OutputPointer
from schemas.include.validation_functions import (
    is_hexadecimal,
    is_valid_address,
    is_valid_hash,
)
from util.data_transformer import hex2bytes


class TransactionOutput(Schema):
    pkh = fields.Str(validate=is_valid_address, required=True)
    time_lock = fields.Int(required=True, validate=validate.Range(min=0))
    value = fields.Int(
        required=True,
        validate=validate.Range(min=1, max=2500000000000000000),
    )


class TransactionBody(Schema):
    inputs = fields.List(fields.Nested(OutputPointer), required=True)
    outputs = fields.List(fields.Nested(TransactionOutput))


class TransactionPublicKey(Schema):
    key_bytes = fields.String(
        validate=is_valid_hash,
        data_key="bytes",
        attribute="bytes",
        required=True,
    )
    compressed = fields.Int(validate=validate.Range(min=0), required=True)

    @post_load
    def transform_bytes(self, args, **kwargs):
        args["bytes"] = hex2bytes(args["bytes"])
        return args


class TransactionSecp256k1(Schema):
    der = fields.String(validate=is_hexadecimal, required=True)

    @post_load
    def transform_der(self, args, **kwargs):
        args["der"] = hex2bytes(args["der"])
        return args


class TransactionSignatures(Schema):
    public_key = fields.Nested(TransactionPublicKey, required=True)
    signature = fields.Dict(
        keys=fields.Str(required=True, validate=validate.Equal("Secp256k1")),
        values=fields.Nested(TransactionSecp256k1, required=True),
        required=True,
    )


class PostValueTransfer(Schema):
    body = fields.Nested(TransactionBody, required=True)
    signatures = fields.List(fields.Nested(TransactionSignatures), required=True)

    @validates_schema
    def validate_input_signatures(self, args, **kwargs):
        if len(args["signatures"]) != len(args["body"]["inputs"]):
            raise ValidationError(
                f"Amount of signatures ({len(args['signatures'])}) != amount of inputs ({len(args['body']['inputs'])})."
            )


class PostStake(Schema):
    pass


class PostUnstake(Schema):
    pass


class PostTransaction(Schema):
    value_transfer = fields.Nested(
        PostValueTransfer, data_key="ValueTransfer", attribute="ValueTransfer"
    )
    stake = fields.Nested(PostStake, data_key="Stake", attribute="Stake")
    unstake = fields.Nested(PostUnstake, data_key="Unstake", attribute="Unstake")

    @validates_schema
    def validate_one_transaction(self, args, **kwargs):
        if len(args) != 1:
            raise ValidationError(
                "Transaction class requires exactly one of ValueTransfer, Stake or Unstake."
            )
