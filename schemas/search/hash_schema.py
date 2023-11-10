from marshmallow import (
    Schema,
    ValidationError,
    fields,
    pre_load,
    validate,
    validates_schema,
)

from schemas.component.block_schema import BlockForApi
from schemas.component.commit_schema import CommitTransactionForApi
from schemas.component.data_request_schema import DataRequestTransactionForApi
from schemas.component.mint_schema import MintTransactionForApi
from schemas.component.reveal_schema import RevealTransactionForApi
from schemas.component.tally_schema import TallyTransactionForApi
from schemas.component.value_transfer_schema import ValueTransferTransactionForApi
from schemas.include.validation_functions import is_valid_hash
from schemas.search.data_request_history_schema import DataRequestHistory
from schemas.search.data_request_report_schema import DataRequestReport


class SearchHashArgs(Schema):
    value = fields.Str(validate=is_valid_hash, required=True)
    simple = fields.Boolean(load_default=False)

    @pre_load
    def remove_leading_0x(self, args, **kwargs):
        if "value" in args and args["value"].startswith("0x"):
            args["value"] = args["value"][2:]
        return args


class SearchHashResponse(Schema):
    response_type = fields.Str(
        validate=validate.OneOf(
            [
                "pending",
                "block",
                "mint",
                "value_transfer",
                "data_request",
                "commit",
                "reveal",
                "tally",
                "data_request_report",
                "data_request_history",
            ]
        ),
        required=True,
    )
    block = fields.Nested(BlockForApi)
    mint = fields.Nested(MintTransactionForApi)
    value_transfer = fields.Nested(ValueTransferTransactionForApi)
    data_request = fields.Nested(DataRequestTransactionForApi)
    commit = fields.Nested(CommitTransactionForApi)
    reveal = fields.Nested(RevealTransactionForApi)
    tally = fields.Nested(TallyTransactionForApi)
    data_request_report = fields.Nested(DataRequestReport)
    data_request_history = fields.Nested(DataRequestHistory)
    pending = fields.Str()

    @validates_schema
    def validate_type(self, args, **kwargs):
        if args["response_type"] not in args:
            raise ValidationError("Data for response type not found in response.")
