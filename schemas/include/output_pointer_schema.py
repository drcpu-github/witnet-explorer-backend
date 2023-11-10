from marshmallow import Schema, fields

from schemas.include.validation_functions import is_valid_output_pointer


class OutputPointer(Schema):
    output_pointer = fields.Str(validate=is_valid_output_pointer, required=True)
