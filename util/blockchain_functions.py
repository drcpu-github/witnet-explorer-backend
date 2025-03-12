import time

from blockchain.config import BlockchainConfig
from util.common_sql import sql_epoch_times

def calculate_block_reward(epoch):
    consensus_constants = BlockchainConfig.consensus_constants
    wit2_activation_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99
    if epoch >= wit2_activation_epoch:
        return consensus_constants.wit2_block_reward
    else:
        halvings = int(epoch / consensus_constants.halving_period)
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

def get_activatation_epoch_wit2():
    return BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99

def calculate_start_timestamp_wit2():
    consensus_constants = BlockchainConfig.consensus_constants
    checkpoint_zero_timestamp = consensus_constants.checkpoint_zero_timestamp
    checkpoints_period = consensus_constants.checkpoints_period
    wit2_activation_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99
    return checkpoint_zero_timestamp + checkpoints_period * wit2_activation_epoch

def calculate_current_epoch():
    consensus_constants = BlockchainConfig.consensus_constants
    checkpoint_zero_timestamp = consensus_constants.checkpoint_zero_timestamp
    checkpoints_period = consensus_constants.checkpoints_period
    wit2_checkpoints_period = consensus_constants.wit2_checkpoints_period
    wit2_activation_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99
    wit2_timestamp = calculate_start_timestamp_wit2()
    current_time = time.time()
    if current_time > wit2_timestamp:
        return wit2_activation_epoch + int((current_time - wit2_timestamp) / wit2_checkpoints_period)
    else:
        return int((current_time - checkpoint_zero_timestamp) / checkpoints_period)

def calculate_timestamp_from_epoch(epoch):
    consensus_constants = BlockchainConfig.consensus_constants
    checkpoint_zero_timestamp = consensus_constants.checkpoint_zero_timestamp
    checkpoints_period = consensus_constants.checkpoints_period
    wit2_checkpoints_period = consensus_constants.wit2_checkpoints_period
    wit2_activation_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99
    if epoch > wit2_activation_epoch:
        return calculate_start_timestamp_wit2() + (epoch - wit2_activation_epoch) * wit2_checkpoints_period
    else:
        return checkpoint_zero_timestamp + checkpoints_period * epoch

def calculate_epoch_from_timestamp(timestamp):
    consensus_constants = BlockchainConfig.consensus_constants
    checkpoint_zero_timestamp = consensus_constants.checkpoint_zero_timestamp
    checkpoints_period = consensus_constants.checkpoints_period
    wit2_checkpoints_period = consensus_constants.wit2_checkpoints_period
    wit2_activation_epoch = BlockchainConfig.wip.get_activation_epoch("wit/2") or 1e99
    wit2_timestamp = calculate_start_timestamp_wit2()
    if timestamp > wit2_timestamp:
        return wit2_activation_epoch + int((timestamp - wit2_timestamp) / wit2_checkpoints_period)
    else:
        return int((timestamp - checkpoint_zero_timestamp) / checkpoints_period)
