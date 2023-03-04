def calculate_block_reward(epoch, consensus_constants):
    halvings = int(epoch // consensus_constants.halving_period)
    if halvings < 64:
        return consensus_constants.initial_block_reward >> halvings
    else:
        return 0

# Round the priority to the nearest integer value but handle the actual zero-fee transactions and round-to-zero fee transactions differently
def calculate_priority(fee, weight, round_priority=False):
    if round_priority:
        if fee == 0:
            return 0
        elif round(fee / weight) == 0:
            return 1
        else:
            return round(fee / weight)
    else:
        return fee / weight

# Check input type
def sanitize_input(input_value, required_type):
    if required_type == "bool":
        return input_value in (True, False)
    elif required_type == "hexadecimal":
        try:
            int(input_value, 16)
            return True
        except ValueError:
            return False
    elif required_type == "alpha":
        return input_value.isalpha()
    elif required_type == "alphanumeric":
        return input_value.isalnum()
    elif required_type == "numeric":
        return input_value.isnumeric()
    elif required_type == "positive_integer":
        try:
            return int(input_value) >= 0
        except ValueError:
            return False
    return False

def sanitize_address(address):
    if not address.startswith("wit1"):
        return False
    if len(address) != 42:
        return False
    if not sanitize_input(address, "alphanumeric"):
        return False
    return True
