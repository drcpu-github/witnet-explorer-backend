from marshmallow import Schema, fields, validate


class PingResponse(Schema):
    response = fields.Str(validate=validate.Equal("pong"), required=True)
