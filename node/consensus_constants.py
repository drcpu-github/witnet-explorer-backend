import time

from node.witnet_node import WitnetNode
from util.database_manager import DatabaseManager


class ConsensusConstants(object):
    def __init__(
        self,
        config={},
        error_retry=0,
        logger=None,
        log_queue=None,
        log_label=None,
        mock=False,
        mock_parameters={},
    ):
        if not mock:
            # First try to fetch the consensus constants from the database
            db_mngr = DatabaseManager(config["database"], logger=logger)

            sql = """
                SELECT
                    *
                FROM
                    consensus_constants
            """
            fetched_consensus_constants = database.sql_return_all(sql)

            # If that did not work, fetch them from a node
            if not fetched_consensus_constants:
                witnet_node = WitnetNode(
                    config["node-pool"],
                    logger=logger,
                    log_queue=log_queue,
                    log_label=log_label,
                )

                consensus_constants = witnet_node.get_consensus_constants()
                while (
                    type(consensus_constants) is dict and "error" in consensus_constants
                ):
                    time.sleep(error_retry)
                    consensus_constants = witnet_node.get_consensus_constants()
                witnet_node.close_connection()

                consensus_constants = consensus_constants["result"]
            # Transform the list of fetched rows to a dictionary
            else:
                consensus_constants = {}
                for key, int_val, str_val in fetched_consensus_constants:
                    if int_val:
                        if key == "reputation_penalization_factor":
                            int_val = int_val / 100
                        consensus_constants[key] = int_val
                    if str_val:
                        if key == "bootstrap_hash" or key == "genesis_hash":
                            str_val = str_val[0]
                        consensus_constants[key] = str_val

            self.activity_period = consensus_constants["activity_period"]
            self.bootstrap_hash = consensus_constants["bootstrap_hash"]
            self.bootstrapping_committee = consensus_constants[
                "bootstrapping_committee"
            ]
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
            self.reputation_issuance_stop = consensus_constants[
                "reputation_issuance_stop"
            ]
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
        else:
            if "activity_period" in mock_parameters:
                self.activity_period = mock_parameters["activity_period"]
            if "bootstrap_hash" in mock_parameters:
                self.bootstrap_hash = mock_parameters["bootstrap_hash"]
            if "bootstrapping_committee" in mock_parameters:
                self.bootstrapping_committee = mock_parameters[
                    "bootstrapping_committee"
                ]
            if "checkpoint_zero_timestamp" in mock_parameters:
                self.checkpoint_zero_timestamp = mock_parameters[
                    "checkpoint_zero_timestamp"
                ]
            if "checkpoints_period" in mock_parameters:
                self.checkpoints_period = mock_parameters["checkpoints_period"]
            if "collateral_age" in mock_parameters:
                self.collateral_age = mock_parameters["collateral_age"]
            if "collateral_minimum" in mock_parameters:
                self.collateral_minimum = mock_parameters["collateral_minimum"]
            if "epochs_with_minimum_difficulty" in mock_parameters:
                self.epochs_with_minimum_difficulty = mock_parameters[
                    "epochs_with_minimum_difficulty"
                ]
            if "extra_rounds" in mock_parameters:
                self.extra_rounds = mock_parameters["extra_rounds"]
            if "genesis_hash" in mock_parameters:
                self.genesis_hash = mock_parameters["genesis_hash"]
            if "halving_period" in mock_parameters:
                self.halving_period = mock_parameters["halving_period"]
            if "initial_block_reward" in mock_parameters:
                self.initial_block_reward = mock_parameters["initial_block_reward"]
            if "minimum_difficulty" in mock_parameters:
                self.minimum_difficulty = mock_parameters["minimum_difficulty"]
            if "max_dr_weight" in mock_parameters:
                self.max_dr_weight = mock_parameters["max_dr_weight"]
            if "max_vt_weight" in mock_parameters:
                self.max_vt_weight = mock_parameters["max_vt_weight"]
            if "mining_backup_factor" in mock_parameters:
                self.mining_backup_factor = mock_parameters["mining_backup_factor"]
            if "mining_replication_factor" in mock_parameters:
                self.mining_replication_factor = mock_parameters[
                    "mining_replication_factor"
                ]
            if "reputation_expire_alpha_diff" in mock_parameters:
                self.reputation_expire_alpha_diff = mock_parameters[
                    "reputation_expire_alpha_diff"
                ]
            if "reputation_issuance" in mock_parameters:
                self.reputation_issuance = mock_parameters["reputation_issuance"]
            if "reputation_issuance_stop" in mock_parameters:
                self.reputation_issuance_stop = mock_parameters[
                    "reputation_issuance_stop"
                ]
            if "reputation_penalization_factor" in mock_parameters:
                self.reputation_penalization_factor = mock_parameters[
                    "reputation_penalization_factor"
                ]
            if "superblock_committee_decreasing_period" in mock_parameters:
                self.superblock_committee_decreasing_period = mock_parameters[
                    "superblock_committee_decreasing_period"
                ]
            if "superblock_committee_decreasing_step" in mock_parameters:
                self.superblock_committee_decreasing_step = mock_parameters[
                    "superblock_committee_decreasing_step"
                ]
            if "superblock_period" in mock_parameters:
                self.superblock_period = mock_parameters["superblock_period"]
            if "superblock_signing_committee_size" in mock_parameters:
                self.superblock_signing_committee_size = mock_parameters[
                    "superblock_signing_committee_size"
                ]
