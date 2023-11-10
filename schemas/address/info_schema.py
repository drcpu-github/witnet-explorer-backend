from marshmallow import Schema, ValidationError, fields, post_load, validate, validates

from schemas.include.address_schema import AddressSchema


class AddressInfoArgs(Schema):
    """
    Addresses field cannot contain more than 10 addresses
    """

    addresses = fields.Str()

    @validates("addresses")
    def validate_addresses(self, data, **kwargs):
        addresses = data.split(",")
        if len(addresses) > 10:
            raise ValidationError(
                "Length of comma-separated address list cannot be more than 10."
            )
        all_err_info = ""
        for address in addresses:
            try:
                AddressSchema().load({"address": address})
            except ValidationError as err_info:
                all_err_info += f"{address}: {err_info.messages['address'][0][:-1]}, "
        if len(all_err_info):
            raise ValidationError(all_err_info[:-2] + ".")

    @post_load
    def split_addresses(self, data, **kwargs):
        if "addresses" in data:
            data["addresses"] = data["addresses"].split(",")
        return data


class AddressInfoResponse(AddressSchema):
    label = fields.Str(required=True)
    active = fields.Int(required=True, validate=validate.Range(min=0))
    block = fields.Int(required=True, validate=validate.Range(min=0))
    mint = fields.Int(required=True, validate=validate.Range(min=0))
    value_transfer = fields.Int(required=True, validate=validate.Range(min=0))
    data_request = fields.Int(required=True, validate=validate.Range(min=0))
    commit = fields.Int(required=True, validate=validate.Range(min=0))
    reveal = fields.Int(required=True, validate=validate.Range(min=0))
    tally = fields.Int(required=True, validate=validate.Range(min=0))
