from marshmallow import Schema, fields, validate

allowed_versions = [
    "V1_7",
    "V1_8",
    "V2_0",
]


class NetworkVersionArgs(Schema):
    key = fields.Str(
        validate=validate.OneOf(
            [
                "all",
                "current",
            ]
        ),
        required=True,
    )


class NetworkVersion(Schema):
    version = fields.Str(validate=validate.OneOf(allowed_versions), required=True)
    epoch = fields.Int(validate=validate.Range(min=0), required=True)
    period = fields.Int(validate=validate.Range(min=0), required=True)


class NetworkVersionResponse(Schema):
    current_version = fields.String(
        validate=validate.OneOf(allowed_versions),
        required=True,
    )
    current_epoch = fields.Int(
        validate=validate.Range(min=0),
        required=True,
    )
    versions = fields.List(fields.Nested(NetworkVersion))
