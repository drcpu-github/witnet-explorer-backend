from marshmallow import Schema, fields, validate


class TransactionPriorityArgs(Schema):
    key = fields.Str(
        load_default="all",
        validate=validate.OneOf(
            [
                "all",
                "drt",
                "vtt",
                "st",
                "ut",
            ]
        ),
    )


class PriorityTime(Schema):
    priority = fields.Float(validate=validate.Range(min=0), required=True)
    time_to_block = fields.Int(validate=validate.Range(min=0), required=True)


class TransactionPriority(Schema):
    stinky = fields.Nested(PriorityTime, required=True)
    low = fields.Nested(PriorityTime, required=True)
    medium = fields.Nested(PriorityTime, required=True)
    high = fields.Nested(PriorityTime, required=True)
    opulent = fields.Nested(PriorityTime, required=True)


class TransactionPriorityResponse(Schema):
    drt = fields.Nested(TransactionPriority)
    vtt = fields.Nested(TransactionPriority)
    st = fields.Nested(TransactionPriority)
    ut = fields.Nested(TransactionPriority)
