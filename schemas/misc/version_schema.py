from marshmallow import Schema, fields


class VersionSchema(Schema):
    version = fields.Str(required=True)
