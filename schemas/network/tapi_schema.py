from marshmallow import Schema, fields, validate


class NetworkTapiArgs(Schema):
    return_all = fields.Boolean(load_default=False)


class AcceptanceRates(Schema):
    periodic_rate = fields.Float(validate=validate.Range(min=0), required=True)
    relative_rate = fields.Float(validate=validate.Range(min=0), required=True)
    global_rate = fields.Float(validate=validate.Range(min=0), required=True)


class NetworkTapiResponse(Schema):
    tapi_id = fields.Int(validate=validate.Range(min=0), required=True)
    title = fields.Str(required=True)
    description = fields.Str(required=True)
    urls = fields.List(fields.Url(schemes=set(["https"])), required=True)
    start_epoch = fields.Int(validate=validate.Range(min=0), required=True)
    start_time = fields.Int(validate=validate.Range(min=0), required=True)
    stop_epoch = fields.Int(validate=validate.Range(min=0), required=True)
    stop_time = fields.Int(validate=validate.Range(min=0), required=True)
    bit = fields.Int(validate=validate.Range(min=0), required=True)
    rates = fields.List(fields.Nested(AcceptanceRates), required=True)
    relative_acceptance_rate = fields.Float(
        validate=validate.Range(min=0),
        required=True,
    )
    global_acceptance_rate = fields.Float(validate=validate.Range(min=0), required=True)
    active = fields.Boolean(required=True)
    finished = fields.Boolean(required=True)
    activated = fields.Boolean(required=True)
    plot = fields.Str()
    current_epoch = fields.Int(validate=validate.Range(min=0), required=True)
    last_updated = fields.Int(validate=validate.Range(min=0), required=True)
