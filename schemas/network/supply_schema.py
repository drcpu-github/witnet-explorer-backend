from marshmallow import Schema, fields, validate


class NetworkSupplyArgs(Schema):
    key = fields.Str(
        required=True,
        validate=validate.OneOf(
            [
                "blocks_minted",
                "blocks_minted_reward",
                "blocks_missing",
                "blocks_missing_reward",
                "current_locked_supply",
                "current_time",
                "current_unlocked_supply",
                "epoch",
                "in_flight_requests",
                "locked_wits_by_requests",
                "maximum_supply",
                "current_supply",
                "total_supply",
                "supply_burned_lies",
            ]
        ),
    )


class NetworkSupply(Schema):
    blocks_minted = fields.Int(
        required=True,
        validate=validate.Range(min=0),
    )
    blocks_minted_reward = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=1750000000000000000),
    )
    blocks_missing = fields.Int(
        required=True,
        validate=validate.Range(min=0),
    )
    blocks_missing_reward = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=1750000000000000000),
    )
    current_locked_supply = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
    current_time = fields.Int(
        required=True,
        validate=validate.Range(min=0),
    )
    current_unlocked_supply = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
    epoch = fields.Int(
        required=True,
        validate=validate.Range(min=0),
    )
    in_flight_requests = fields.Int(
        required=True,
        validate=validate.Range(min=0),
    )
    locked_wits_by_requests = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
    maximum_supply = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
    current_supply = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
    total_supply = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
    supply_burned_lies = fields.Int(
        required=True,
        validate=validate.Range(min=0, max=2500000000000000000),
    )
