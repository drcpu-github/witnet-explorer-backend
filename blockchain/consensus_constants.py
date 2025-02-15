import time

from mockups.database import MockDatabase
from node.witnet_node import WitnetNode
from util.database_manager import DatabaseManager


class ConsensusConstants(object):
    def __init__(
        self,
        database=None,
        witnet_node=None,
        error_retry=0,
        mockup=False,
    ):
        # First try to fetch the consensus constants from the database
        database_created = False
        if database is None:
            if mockup:
                database = MockDatabase()
            else:
                database = DatabaseManager()
                database_created = True

        sql = "SELECT * FROM consensus_constants"
        if hasattr(database, "named_cursor") and database.named_cursor:
            database.reset_cursor()
        fetched_consensus_constants = database.sql_return_all(sql)
        if database_created:
            database.terminate()

        # If that did not work, fetch them from a node
        if not fetched_consensus_constants:
            witnet_node_created = False
            if not witnet_node:
                witnet_node = WitnetNode()
                witnet_node_created = True

            consensus_constants = witnet_node.get_consensus_constants()
            while "error" in consensus_constants:
                time.sleep(error_retry)
                consensus_constants = witnet_node.get_consensus_constants()
            if witnet_node_created:
                witnet_node.close_connection()

            consensus_constants = consensus_constants["result"]
        # Transform the list of fetched rows to a dictionary
        else:
            consensus_constants = {}
            for key, int_val, str_val in fetched_consensus_constants:
                if int_val is not None:
                    if key == "reputation_penalization_factor":
                        int_val = int_val / 100
                    consensus_constants[key] = int_val
                if str_val is not None:
                    if key == "bootstrap_hash" or key == "genesis_hash":
                        str_val = str_val[0]
                    consensus_constants[key] = str_val

        self.activity_period = consensus_constants["activity_period"]
        self.bootstrap_hash = consensus_constants["bootstrap_hash"]
        self.bootstrapping_committee = consensus_constants["bootstrapping_committee"]
        self.checkpoint_zero_timestamp = consensus_constants[
            "checkpoint_zero_timestamp"
        ]
        self.checkpoints_period = consensus_constants["checkpoints_period"]
        self.collateral_age = consensus_constants["collateral_age"]
        self.collateral_minimum = consensus_constants["collateral_minimum"]
        self.epochs_with_minimum_difficulty = consensus_constants[
            "epochs_with_minimum_difficulty"
        ]
        self.extra_rounds = consensus_constants["extra_rounds"]
        self.genesis_hash = consensus_constants["genesis_hash"]
        self.halving_period = consensus_constants["halving_period"]
        self.initial_block_reward = consensus_constants["initial_block_reward"]
        self.minimum_difficulty = consensus_constants["minimum_difficulty"]
        self.max_dr_weight = consensus_constants["max_dr_weight"]
        self.max_vt_weight = consensus_constants["max_vt_weight"]
        self.mining_backup_factor = consensus_constants["mining_backup_factor"]
        self.mining_replication_factor = consensus_constants[
            "mining_replication_factor"
        ]
        self.reputation_expire_alpha_diff = consensus_constants[
            "reputation_expire_alpha_diff"
        ]
        self.reputation_issuance = consensus_constants["reputation_issuance"]
        self.reputation_issuance_stop = consensus_constants["reputation_issuance_stop"]
        self.reputation_penalization_factor = consensus_constants[
            "reputation_penalization_factor"
        ]
        self.superblock_committee_decreasing_period = consensus_constants[
            "superblock_committee_decreasing_period"
        ]
        self.superblock_committee_decreasing_step = consensus_constants[
            "superblock_committee_decreasing_step"
        ]
        self.superblock_period = consensus_constants["superblock_period"]
        self.superblock_signing_committee_size = consensus_constants[
            "superblock_signing_committee_size"
        ]
        self.wit2_checkpoints_period = consensus_constants["wit2_checkpoints_period"]
        self.wit2_minimum_total_stake_nanowits = consensus_constants[
            "wit2_minimum_total_stake_nanowits"
        ]
        self.wit2_activation_delay_epochs = consensus_constants[
            "wit2_activation_delay_epochs"
        ]
        self.wit2_maximum_stake_block_weight = consensus_constants[
            "wit2_maximum_stake_block_weight"
        ]
        self.wit2_maximum_unstake_block_weight = consensus_constants[
            "wit2_maximum_unstake_block_weight"
        ]
        self.wit2_unstaking_delay_seconds = consensus_constants[
            "wit2_unstaking_delay_seconds"
        ]
        self.wit2_min_stake_nanowits = consensus_constants["wit2_min_stake_nanowits"]
        self.wit2_max_stake_nanowits = consensus_constants["wit2_max_stake_nanowits"]
        self.wit2_block_reward = consensus_constants["wit2_block_reward"]
