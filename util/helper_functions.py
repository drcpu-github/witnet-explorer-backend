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
