from marshmallow import ValidationError


def is_hexadecimal(input_value):
    try:
        int(input_value, 16)
    except ValueError:
        raise ValidationError("Not a hexadecimal value.")


def is_valid_address(input_value):
    errors = []
    if len(input_value) != 42:
        errors.append("Address does not contain 42 characters.")
    if not input_value.startswith("wit1"):
        errors.append("Address does not start with wit1 string.")
    if len(errors) > 0:
        raise ValidationError(errors)


def is_valid_hash(hash_value):
    errors = []
    if len(hash_value) != 64:
        errors.append("Hash does not contain 64 characters.")
    try:
        is_hexadecimal(hash_value)
    except ValidationError:
        errors.append("Hash is not a hexadecimal value.")
    if len(errors) > 0:
        raise ValidationError(errors)


def is_valid_output_pointer(pointer):
    try:
        transaction_hash, output_index = pointer.split(":")
    except ValueError:
        raise ValidationError(
            "Cannot split output pointer into transaction hash and output index."
        )

    is_valid_hash(transaction_hash)

    try:
        int(output_index)
    except ValueError:
        raise ValidationError("Cannot convert output index to an integer.")

    if int(output_index) < 0:
        raise ValidationError("Output index must be larger than 0.")
