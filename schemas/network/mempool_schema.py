from marshmallow import Schema, ValidationError, fields, validate, validates_schema


class NetworkMempoolArgs(Schema):
    transaction_type = fields.Str(
        required=True,
        validate=validate.OneOf(
            [
                "data_requests",
                "value_transfers",
            ]
        ),
    )
    start_epoch = fields.Int(validate=validate.Range(min=0))
    stop_epoch = fields.Int(validate=validate.Range(min=0))

    @validates_schema
    def validate_fields(self, data, **kwargs):
        if (
            "start_epoch" in data
            and "stop_epoch" in data
            and data["stop_epoch"] < data["start_epoch"]
        ):
            raise ValidationError(
                "The stop_epoch parameter is smaller than the start_epoch parameter."
            )


class NetworkMempoolResponse(Schema):
    timestamp = fields.Int(required=True, validate=validate.Range(min=0))
    fee = fields.List(fields.Int(validate=validate.Range(min=0)), required=True)
    amount = fields.List(fields.Int(validate=validate.Range(min=0)), required=True)
