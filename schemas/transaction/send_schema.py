from marshmallow import Schema, fields


class ValueTransferArgs(Schema):
    test = fields.Boolean(load_default=False)


class ValueTransferResponse(Schema):
    result = fields.Str(required=True)
