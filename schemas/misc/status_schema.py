from marshmallow import Schema, fields, validate

from schemas.include.hash_schema import HashSchema


class NodePoolResponse(Schema):
    epoch = fields.Int(validate=validate.Range(min=0))
    status = fields.Str()
    message = fields.Str(required=True)


class DatabaseResponse(HashSchema):
    epoch = fields.Int(required=True, validate=validate.Range(min=0))


class StatusResponse(Schema):
    message = fields.Str(required=True)
    node_pool_message = fields.Nested(NodePoolResponse)
    database_confirmed = fields.Nested(DatabaseResponse)
    database_unconfirmed = fields.Nested(DatabaseResponse)
    database_message = fields.Str()
    expected_epoch = fields.Int(validate=validate.Range(min=0))
