from marshmallow import Schema, fields


class AbortSchema(Schema):
    message = fields.Str(required=True)
