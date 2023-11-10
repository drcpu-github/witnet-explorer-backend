from marshmallow import Schema, fields, validate


class TransactionPriorityArgs(Schema):
    key = fields.Str(
        load_default="all",
        validate=validate.OneOf(
            [
                "all",
                "drt",
                "vtt",
            ]
        ),
    )


class TransactionPriority(Schema):
    priority = fields.Float(required=True, validate=validate.Range(min=0))
    time_to_block = fields.Int(required=True, validate=validate.Range(min=0))


class TransactionPriorityResponse(Schema):
    drt_stinky = fields.Nested(TransactionPriority)
    drt_low = fields.Nested(TransactionPriority)
    drt_medium = fields.Nested(TransactionPriority)
    drt_high = fields.Nested(TransactionPriority)
    drt_opulent = fields.Nested(TransactionPriority)
    vtt_stinky = fields.Nested(TransactionPriority)
    vtt_low = fields.Nested(TransactionPriority)
    vtt_medium = fields.Nested(TransactionPriority)
    vtt_high = fields.Nested(TransactionPriority)
    vtt_opulent = fields.Nested(TransactionPriority)
