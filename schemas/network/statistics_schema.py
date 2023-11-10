from marshmallow import Schema, ValidationError, fields, validate, validates_schema

from schemas.include.address_schema import AddressSchema


class NetworkStatisticsArgs(Schema):
    key = fields.Str(
        validate=validate.OneOf(
            [
                "list-rollbacks",
                "num-unique-miners",
                "num-unique-data-request-solvers",
                "top-100-miners",
                "top-100-data-request-solvers",
                "percentile-staking-balances",
                "histogram-data-requests",
                "histogram-data-request-composition",
                "histogram-data-request-witness",
                "histogram-data-request-lie-rate",
                "histogram-burn-rate",
                "histogram-data-request-collateral",
                "histogram-data-request-reward",
                "histogram-value-transfers",
            ]
        ),
        required=True,
    )
    start_epoch = fields.Int(validate=validate.Range(min=0))
    stop_epoch = fields.Int(validate=validate.Range(min=0))

    @validates_schema
    def validate_fields(self, data, **kwargs):
        if (
            "stop_epoch" in data
            and "start_epoch" in data
            and data["stop_epoch"] < data["start_epoch"]
        ):
            raise ValidationError(
                "The stop_epoch parameter is smaller than the start_epoch parameter."
            )


class NetworkStaking(Schema):
    ars = fields.List(
        fields.Int(validate=validate.Range(min=0, max=2500000000000000000)),
        required=True,
    )
    trs = fields.List(
        fields.Int(validate=validate.Range(min=0, max=2500000000000000000)),
        required=True,
    )
    percentiles = fields.List(
        fields.Int(validate=validate.Range(min=1, max=100)), required=True
    )


class NetworkRollback(Schema):
    timestamp = fields.Int(validate=validate.Range(min=0), required=True)
    epoch_from = fields.Int(validate=validate.Range(min=0), required=True)
    epoch_to = fields.Int(validate=validate.Range(min=1), required=True)
    length = fields.Int(validate=validate.Range(min=1), required=True)

    @validates_schema
    def validate_length(self, args, **kwargs):
        if args["epoch_to"] - args["epoch_from"] + 1 != args["length"]:
            raise ValidationError(
                f"Incorrect rollback length: {args['epoch_to']} - {args['epoch_from']} + 1 != {args['length']}."
            )


class Top100(AddressSchema):
    amount = fields.Int(validate=validate.Range(min=0), required=True)


class DataRequests(Schema):
    total = fields.Int(validate=validate.Range(min=0), required=True)
    failure = fields.Int(validate=validate.Range(min=0), required=True)


class DataRequestsComposition(Schema):
    total = fields.Int(validate=validate.Range(min=0), required=True)
    http_get = fields.Int(validate=validate.Range(min=0), required=True)
    http_post = fields.Int(validate=validate.Range(min=0), required=True)
    rng = fields.Int(validate=validate.Range(min=0), required=True)


class DataRequestsLieRate(Schema):
    witnessing_acts = fields.Int(validate=validate.Range(min=0), required=True)
    errors = fields.Int(validate=validate.Range(min=0), required=True)
    no_reveal_lies = fields.Int(validate=validate.Range(min=0), required=True)
    out_of_consensus_lies = fields.Int(validate=validate.Range(min=0), required=True)


class SupplyBurnRate(Schema):
    reverted = fields.Int(validate=validate.Range(min=0), required=True)
    lies = fields.Int(validate=validate.Range(min=0), required=True)


class ValueTransfers(Schema):
    value_transfers = fields.Int(validate=validate.Range(min=0), required=True)


class NetworkStatisticsResponse(Schema):
    start_epoch = fields.Int(validate=validate.Range(min=0))
    stop_epoch = fields.Int(validate=validate.Range(min=0))
    histogram_keys = fields.List(fields.String())
    staking = fields.Nested(NetworkStaking)
    list_rollbacks = fields.List(fields.Nested(NetworkRollback))
    top_100_miners = fields.List(fields.Nested(Top100))
    top_100_data_request_solvers = fields.List(fields.Nested(Top100))
    num_unique_miners = fields.Int(validate=validate.Range(min=0))
    num_unique_data_request_solvers = fields.Int(validate=validate.Range(min=0))
    histogram_data_requests = fields.List(fields.Nested(DataRequests))
    histogram_data_request_composition = fields.List(
        fields.Nested(DataRequestsComposition)
    )
    histogram_data_request_witness = fields.List(
        fields.Dict(
            keys=fields.String(),
            values=fields.Int(validate=validate.Range(min=0)),
        )
    )
    histogram_data_request_reward = fields.List(
        fields.Dict(
            keys=fields.String(),
            values=fields.Int(validate=validate.Range(min=0)),
        )
    )
    histogram_data_request_collateral = fields.List(
        fields.Dict(
            keys=fields.String(),
            values=fields.Int(validate=validate.Range(min=0)),
        )
    )
    histogram_data_request_lie_rate = fields.List(fields.Nested(DataRequestsLieRate))
    histogram_burn_rate = fields.List(fields.Nested(SupplyBurnRate))
    histogram_value_transfers = fields.List(fields.Nested(ValueTransfers))

    @validates_schema
    def validate_histogram_keys(self, args, **kwargs):
        if "histogram_data_request_witness" in args:
            if "histogram_keys" not in args:
                raise ValidationError(
                    "The histogram_keys field is required when a histogram_data_request_witness field is supplied."
                )
        if "histogram_data_request_collateral" in args:
            if "histogram_keys" not in args:
                raise ValidationError(
                    "The histogram_keys field is required when a histogram_data_request_collateral field is supplied."
                )
        if "histogram_data_request_reward" in args:
            if "histogram_keys" not in args:
                raise ValidationError(
                    "The histogram_keys field is required when a histogram_data_request_reward field is supplied."
                )
