from marshmallow import Schema, fields


class SendArgs(Schema):
    test = fields.Boolean(load_default=False)


class SendResponse(Schema):
    result = fields.Str(required=True)
