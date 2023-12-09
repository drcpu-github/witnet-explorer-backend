from marshmallow import (
    Schema,
    ValidationError,
    fields,
    post_load,
    pre_load,
    validate,
    validates_schema,
)

from schemas.include.validation_functions import (
    is_valid_address,
    is_valid_hash,
    is_valid_output_pointer,
)


class InputUtxo(Schema):
    address = fields.Str(validate=is_valid_address, required=True)
    value = fields.Int(validate=validate.Range(min=0), required=True)


class InputUtxoPointer(InputUtxo):
    input_utxo = fields.Str(validate=is_valid_output_pointer, required=True)


class InputUtxoList(Schema):
    input_utxos = fields.List(
        fields.Tuple(
            (
                fields.Str(validate=is_valid_hash),
                fields.Int(validate=validate.Range(min=0)),
            ),
        ),
        required=True,
    )

    @pre_load
    def transform_hashes_before_load(self, args, **kwargs):
        if "input_utxos" not in args:
            return args
        input_utxos = args["input_utxos"]
        invalid_lengths = []
        for i, utxo in enumerate(input_utxos):
            length_check_failed = False
            if len(utxo) != 2:
                length_check_failed = True
                invalid_lengths.append(
                    f"Output pointer tuple {i} has an invalid length."
                )
            if not length_check_failed and type(utxo[0]) is bytearray:
                args["input_utxos"][i] = (utxo[0].hex(), utxo[1])
        if len(invalid_lengths) > 0:
            raise ValidationError(invalid_lengths)
        return args

    @post_load
    def transform_hashes_after_load(self, args, **kwargs):
        if "input_utxos" not in args:
            return args
        input_utxos = args["input_utxos"]
        for i, utxo in enumerate(input_utxos):
            args["input_utxos"][i] = (bytearray.fromhex(utxo[0]), utxo[1])
        return args

    @validates_schema
    def validate_inputs(self, args, **kwargs):
        if len(args["input_utxos"]) < 1:
            raise ValidationError({"input_utxos": "Need at least one input UTXO."})
