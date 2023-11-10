from marshmallow import Schema, fields, pre_load

from schemas.include.validation_functions import is_valid_hash


class HashSchema(Schema):
    hash_value = fields.Str(
        required=True,
        validate=is_valid_hash,
        data_key="hash",
        attribute="hash",
    )

    @pre_load
    def remove_leading_0x(self, args, **kwargs):
        if "hash" in args and args["hash"].startswith("0x"):
            args["hash"] = args["hash"][2:]
        return args
