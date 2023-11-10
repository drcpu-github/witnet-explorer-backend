from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.component.commit_schema import (
    CommitTransactionForBlock,
    CommitTransactionForExplorer,
)
from schemas.component.data_request_schema import (
    DataRequestTransactionForBlock,
    DataRequestTransactionForExplorer,
)
from schemas.component.mint_schema import (
    MintTransactionForBlock,
    MintTransactionForExplorer,
)
from schemas.component.reveal_schema import (
    RevealTransactionForBlock,
    RevealTransactionForExplorer,
)
from schemas.component.tally_schema import (
    TallyTransactionForBlock,
    TallyTransactionForExplorer,
)
from schemas.component.value_transfer_schema import (
    ValueTransferTransactionForBlock,
    ValueTransferTransactionForExplorer,
)
from schemas.include.base_transaction_schema import TimestampComponent
from schemas.include.validation_functions import is_valid_hash


class BlockDetails(TimestampComponent):
    hash_value = fields.String(
        validate=is_valid_hash,
        data_key="hash",
        attribute="hash",
        required=True,
    )
    data_request_weight = fields.Integer(validate=validate.Range(min=0), required=True)
    value_transfer_weight = fields.Integer(
        validate=validate.Range(min=0),
        required=True,
    )
    weight = fields.Integer(validate=validate.Range(min=0), required=True)
    confirmed = fields.Boolean(required=True)
    reverted = fields.Boolean(required=True)


class BlockTransactionsForExplorer(Schema):
    mint = fields.Nested(MintTransactionForExplorer, required=True)
    value_transfer = fields.List(
        fields.Nested(ValueTransferTransactionForExplorer),
        required=True,
    )
    data_request = fields.List(
        fields.Nested(DataRequestTransactionForExplorer),
        required=True,
    )
    commit = fields.List(fields.Nested(CommitTransactionForExplorer), required=True)
    reveal = fields.List(fields.Nested(RevealTransactionForExplorer), required=True)
    tally = fields.List(fields.Nested(TallyTransactionForExplorer), required=True)


class BlockForExplorer(Schema):
    details = fields.Nested(BlockDetails, required=True)
    transactions = fields.Nested(BlockTransactionsForExplorer, required=True)
    tapi = fields.List(
        fields.Int(validate=validate.Range(min=0, max=1)),
        allow_none=True,
        required=True,
    )

    @validates_schema
    def validate_tapi(self, args, **kwargs):
        if args["tapi"] is not None and len(args["tapi"]) < 32:
            raise ValidationError("TAPI signal vector does not have a length of 32.")


class BlockTransactionsForApi(Schema):
    mint = fields.Nested(MintTransactionForBlock, required=True)
    value_transfer = fields.List(
        fields.Nested(ValueTransferTransactionForBlock),
        required=True,
    )
    data_request = fields.List(
        fields.Nested(DataRequestTransactionForBlock),
        required=True,
    )
    commit = fields.Dict(
        keys=fields.Str(validate=is_valid_hash),
        values=fields.List(fields.Nested(CommitTransactionForBlock)),
        required=True,
    )
    reveal = fields.Dict(
        keys=fields.Str(validate=is_valid_hash),
        values=fields.List(fields.Nested(RevealTransactionForBlock)),
        required=True,
    )
    tally = fields.List(fields.Nested(TallyTransactionForBlock), required=True)
    number_of_commits = fields.Int(validate=validate.Range(min=0), required=True)
    number_of_reveals = fields.Int(validate=validate.Range(min=0), required=True)


class BlockForApi(Schema):
    details = fields.Nested(BlockDetails, required=True)
    transactions = fields.Nested(BlockTransactionsForApi, required=True)
