from marshmallow import Schema, fields, validate

from schemas.component.block_schema import BlockForApi


class SearchEpochArgs(Schema):
    value = fields.Int(required=True, validate=validate.Range(min=0))


class SearchEpochResponse(Schema):
    response_type = fields.Str(
        validate=validate.Equal("block"),
        required=True,
    )
    block = fields.Nested(BlockForApi)
