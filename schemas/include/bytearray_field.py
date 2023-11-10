from marshmallow import ValidationError, fields


class BytearrayField(fields.Field):
    def _validate(self, value):
        if not isinstance(value, bytearray):
            raise ValidationError("Input type is not a bytearray.")
