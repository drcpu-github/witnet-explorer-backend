from marshmallow import fields

from schemas.include.address_schema import AddressSchema


class AddressLabelResponse(AddressSchema):
    label = fields.Str(required=True)
