from marshmallow import fields, pre_load

from schemas.include.address_schema import AddressSchema
from schemas.include.base_transaction_schema import BaseApiTransaction, BaseTransaction
from schemas.include.bytearray_field import BytearrayField
from schemas.include.validation_functions import is_valid_hash


class RevealTransactionForApi(BaseApiTransaction, AddressSchema):
    reveal = fields.String(required=True)
    success = fields.Boolean(required=True)


class RevealTransactionForBlock(BaseTransaction, AddressSchema):
    data_request = fields.Str(validate=is_valid_hash, required=True)
    reveal = fields.String(required=True)
    success = fields.Boolean(required=True)


class RevealTransactionForDataRequest(BaseApiTransaction, AddressSchema):
    # Overwrite the hash_value and block fields from BaseApiTransaction to support missing reveals
    hash_value = fields.Str(
        allow_none=True,
        required=True,
        validate=is_valid_hash,
        data_key="hash",
        attribute="hash",
    )
    block = fields.Str(allow_none=True, validate=is_valid_hash, required=True)
    reveal = fields.String(required=True)
    success = fields.Boolean(required=True)
    error = fields.Boolean(required=True)
    liar = fields.Boolean(required=True)

    @pre_load
    def remove_leading_0x(self, args, **kwargs):
        if (
            "hash" in args
            and args["hash"] is not None
            and args["hash"].startswith("0x")
        ):
            args["hash"] = args["hash"][2:]
        return args


class RevealTransactionForExplorer(BaseTransaction, AddressSchema):
    data_request = fields.Str(validate=is_valid_hash, required=True)
    reveal = BytearrayField(required=True)
    success = fields.Boolean(required=True)
