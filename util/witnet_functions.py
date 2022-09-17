def calculate_block_reward(epoch, consensus_constants):
    halvings = int(epoch // consensus_constants.halving_period)
    if halvings < 64:
        return consensus_constants.initial_block_reward >> halvings
    else:
        return 0
