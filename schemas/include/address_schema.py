from marshmallow import Schema, fields

from schemas.include.validation_functions import is_valid_address


class AddressSchema(Schema):
    address = fields.Str(validate=is_valid_address, required=True)
