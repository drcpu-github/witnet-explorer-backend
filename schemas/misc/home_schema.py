from marshmallow import Schema, fields, validate

from schemas.include.hash_schema import HashSchema
from schemas.network.supply_schema import NetworkSupply


class HomeArgs(Schema):
    key = fields.Str(
        load_default="full",
        validate=validate.OneOf(
            [
                "full",
                "network_stats",
                "supply_info",
                "blocks",
                "data_requests",
                "value_transfers",
            ]
        ),
    )


class HomeNetworkStats(Schema):
    epochs = fields.Int(validate=validate.Range(min=0), required=True)
    num_blocks = fields.Int(validate=validate.Range(min=0), required=True)
    num_data_requests = fields.Int(validate=validate.Range(min=0), required=True)
    num_value_transfers = fields.Int(validate=validate.Range(min=0), required=True)
    num_active_nodes = fields.Int(validate=validate.Range(min=0), required=True)
    num_reputed_nodes = fields.Int(validate=validate.Range(min=0), required=True)
    num_pending_requests = fields.Int(validate=validate.Range(min=0), required=True)


class HomeBlock(HashSchema):
    data_request = fields.Int(validate=validate.Range(min=0), required=True)
    value_transfer = fields.Int(validate=validate.Range(min=0), required=True)
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)
    confirmed = fields.Boolean(required=True)


class HomeTransaction(HashSchema):
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)
    confirmed = fields.Boolean(required=True)


class HomeResponse(Schema):
    network_stats = fields.Nested(HomeNetworkStats)
    supply_info = fields.Nested(NetworkSupply)
    latest_blocks = fields.List(fields.Nested(HomeBlock))
    latest_data_requests = fields.List(fields.Nested(HomeTransaction))
    latest_value_transfers = fields.List(fields.Nested(HomeTransaction))
    last_updated = fields.Int(validate=validate.Range(min=0))
