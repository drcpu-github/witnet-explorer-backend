from marshmallow import ValidationError, fields, validate, validates_schema

from blockchain.config import BlockchainConfig
from schemas.include.base_transaction_schema import (
    BaseApiTransaction,
    BaseTransaction,
    TimestampComponent,
)
from schemas.include.validation_functions import is_valid_address


class UnstakeTransactionForExplorer(BaseTransaction):
    validator = fields.Str(validate=is_valid_address, required=True)
    withdrawer = fields.Str(validate=is_valid_address, required=True)
    unstake_value = fields.Int(validate=validate.Range(min=1), required=True)
    fee = fields.Int(validate=validate.Range(min=0), required=True)
    nonce = fields.Int(validate=validate.Range(min=1), required=True)
    weight = fields.Int(validate=validate.Range(min=1), required=True)

    @validates_schema
    def validate(self, args, **kwargs):
        epoch = args["epoch"] if "epoch" in args else 0
        wit2_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99

        if epoch < wit2_epoch:
            raise ValidationError("Unstake transactions are not allowed yet.", "epoch")


class UnstakeTransactionForApi(BaseApiTransaction, UnstakeTransactionForExplorer):
    priority = fields.Int(validate=validate.Range(min=0), required=True)


class UnstakeTransactionForBlock(UnstakeTransactionForExplorer, TimestampComponent):
    priority = fields.Int(validate=validate.Range(min=0), required=True)
