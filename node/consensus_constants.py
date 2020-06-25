import time

from node.witnet_node import WitnetNode

class ConsensusConstants(object):
    def __init__(self, socket_host, socket_port, error_retry, logging_queue, log_label):
        witnet_node = WitnetNode(socket_host, socket_port, 15, logging_queue, log_label)

        response = witnet_node.get_consensus_constants()
        while type(response) is dict and "error" in response:
            time.sleep(error_retry)
            response = witnet_node.get_consensus_constants()
        witnet_node.close_connection()

        response = response["result"]

        self.activity_period = response["activity_period"]
        self.bootstrap_hash = response["bootstrap_hash"]
        self.bootstrapping_committee = response["bootstrapping_committee"]
        self.checkpoint_zero_timestamp = response["checkpoint_zero_timestamp"]
        self.checkpoints_period = response["checkpoints_period"]
        self.collateral_age = response["collateral_age"]
        self.collateral_minimum = response["collateral_minimum"]
        self.epochs_with_minimum_difficulty = response["epochs_with_minimum_difficulty"]
        self.extra_rounds = response["extra_rounds"]
        self.genesis_hash = response["genesis_hash"]
        self.halving_period = response["halving_period"]
        self.initial_block_reward = response["initial_block_reward"]
        self.minimum_difficulty = response["minimum_difficulty"]
        self.max_dr_weight = response["max_dr_weight"]
        self.max_vt_weight = response["max_vt_weight"]
        self.mining_backup_factor = response["mining_backup_factor"]
        self.mining_replication_factor = response["mining_replication_factor"]
        self.reputation_expire_alpha_diff = response["reputation_expire_alpha_diff"]
        self.reputation_issuance = response["reputation_issuance"]
        self.reputation_issuance_stop = response["reputation_issuance_stop"]
        self.reputation_penalization_factor = response["reputation_penalization_factor"]
        self.superblock_committee_decreasing_period = response["superblock_committee_decreasing_period"]
        self.superblock_committee_decreasing_step = response["superblock_committee_decreasing_step"]
        self.superblock_period = response["superblock_period"]
        self.superblock_signing_committee_size = response["superblock_signing_committee_size"]
