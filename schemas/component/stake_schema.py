from marshmallow import ValidationError, fields, validate, validates_schema

from blockchain.config import BlockchainConfig
from schemas.include.base_transaction_schema import (
    BaseApiTransaction,
    BaseTransaction,
    TimestampComponent,
)
from schemas.include.input_utxo_schema import InputUtxo, InputUtxoList
from schemas.include.validation_functions import is_valid_address


class StakeTransactionForApi(BaseApiTransaction):
    inputs = fields.List(fields.Nested(InputUtxo), required=True)
    change_address = fields.Str(
        validate=is_valid_address, allow_none=True, required=True
    )
    change_value = fields.Int(
        validate=validate.Range(min=0), allow_none=True, required=True
    )
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    stake_value = fields.Int(validate=validate.Range(min=0), required=True)
    priority = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)

    @validates_schema
    def validate(self, args, **kwargs):
        epoch = args["epoch"] if "epoch" in args else 0
        wip0028_epoch = BlockchainConfig.wip.get_activation_epoch("WIP0028") or 1e99

        if epoch < wip0028_epoch:
            raise ValidationError("Stake transactions are not allowed yet.", "epoch")

        input_value = sum(inp["value"] for inp in args["inputs"])
        change_value = args["change_value"] if args["change_value"] else 0
        total_output_value = change_value + args["stake_value"] + args["fee"]
        if input_value != total_output_value:
            raise ValidationError(
                f"Sum of inputs ({input_value}) does not match sum of stake value, output value and fee ({total_output_value})."
            )


class StakeTransactionForBlock(BaseTransaction, TimestampComponent):
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)
    priority = fields.Int(validate=validate.Range(min=0), required=True)
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    stake_value = fields.Int(validate=validate.Range(min=1), required=True)

    @validates_schema
    def validate(self, args, **kwargs):
        epoch = args["epoch"] if "epoch" in args else 0
        wip0028_epoch = BlockchainConfig.wip.get_activation_epoch("WIP0028") or 1e99

        if epoch < wip0028_epoch:
            raise ValidationError("Stake transactions are not allowed yet.", "epoch")


class StakeTransactionForExplorer(BaseTransaction, InputUtxoList):
    input_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    input_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )
    change_address = fields.Str(
        validate=is_valid_address, allow_none=True, required=True
    )
    change_value = fields.Int(
        validate=validate.Range(min=0), allow_none=True, required=True
    )
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    stake_value = fields.Int(validate=validate.Range(min=1), required=True)

    @validates_schema
    def validate(self, args, **kwargs):
        epoch = args["epoch"] if "epoch" in args else 0
        wip0028_epoch = BlockchainConfig.wip.get_activation_epoch("WIP0028") or 1e99

        if epoch < wip0028_epoch:
            raise ValidationError("Stake transactions are not allowed yet.", "epoch")

        if not (
            len(args["input_addresses"])
            == len(args["input_values"])
            == len(args["input_utxos"])
        ):
            raise ValidationError(
                "Number of input addresses, values and UTXO's is different."
            )
