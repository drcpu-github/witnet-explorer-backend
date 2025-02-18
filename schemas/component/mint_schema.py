from marshmallow import ValidationError, fields, validate, validates_schema

from blockchain.config import BlockchainConfig
from schemas.include.base_transaction_schema import BaseApiTransaction, BaseTransaction
from schemas.include.validation_functions import is_valid_address


class MintTransaction(BaseTransaction):
    miner = fields.Str(validate=is_valid_address, required=True)
    output_addresses = fields.List(fields.Str(validate=is_valid_address), required=True)
    output_values = fields.List(
        fields.Int(validate=validate.Range(min=1)), required=True
    )

    @validates_schema
    def validate_inputs(self, args, **kwargs):
        epoch = args["epoch"] if "epoch" in args else 0
        wit2_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99
        output_addresses = args["output_addresses"]
        output_values = args["output_values"]

        errors = {}
        if epoch > 0 and epoch < wit2_epoch and len(output_addresses) < 1:
            errors["output_addresses"] = "Need at least one output address."
        if len(output_addresses) != len(output_values):
            errors["output_values"] = (
                "Number of output addresses and values is different."
            )
        if len(errors):
            raise ValidationError(errors)


class MintTransactionForApi(BaseApiTransaction, MintTransaction):
    pass


class MintTransactionForBlock(MintTransaction):
    pass


class MintTransactionForExplorer(MintTransactionForBlock):
    pass
