from marshmallow import fields, validate

from schemas.address.info_schema import AddressInfoArgs
from schemas.include.address_schema import AddressSchema
from schemas.include.output_pointer_schema import OutputPointer


class AddressUtxosArgs(AddressInfoArgs):
    addresses = fields.Str(required=True)


class Utxo(OutputPointer):
    timelock = fields.Int(validate=validate.Range(min=0), required=True)
    utxo_mature = fields.Boolean(required=True)
    value = fields.Int(validate=validate.Range(min=1), required=True)


class AddressUtxosResponse(AddressSchema):
    utxos = fields.List(fields.Nested(Utxo))
