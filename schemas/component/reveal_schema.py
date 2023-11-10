from marshmallow import fields

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
    reveal = fields.String(required=True)
    success = fields.Boolean(required=True)
    error = fields.Boolean(required=True)
    liar = fields.Boolean(required=True)


class RevealTransactionForExplorer(BaseTransaction, AddressSchema):
    data_request = fields.Str(validate=is_valid_hash, required=True)
    reveal = BytearrayField(required=True)
    success = fields.Boolean(required=True)
