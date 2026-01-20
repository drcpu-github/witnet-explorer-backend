import optparse
import pylibmc
import sys
import time
import toml

from marshmallow import ValidationError
from psycopg.sql import SQL, Literal

from blockchain.config import BlockchainConfig
from blockchain.consensus_constants import ConsensusConstants
from blockchain.objects.wip import WIP
from blockchain.objects.block import Block
from caching.client import Client
from util.data_transformer import re_sql
from util.logger import configure_logger
from util.memcached import calculate_timeout
from util.common_sql import sql_last_block
from util.blockchain_functions import (
    calculate_block_reward,
    calculate_current_epoch,
    calculate_timestamp_from_epoch,
    get_activatation_epoch_wit2,
)


class Stakes(Client):
    def __init__(self):
        s_cfg = BlockchainConfig.config["api"]["caching"]["scripts"]["stakes"]

        network = BlockchainConfig.config["environment"]["network"]
        if network == "mainnet":
            self.address_length = Literal(42)
        elif network == "testnet":
            self.address_length = Literal(43)
        else:
            print(
                f"Unsupported network environment {network}! Supported networks are mainnet and testnet."
            )
            sys.exit(1)

        # Setup logger
        self.logger = configure_logger("stakes", s_cfg["log_file"], s_cfg["level_file"])

        super().__init__(BlockchainConfig.config)

        # Fetch configured timeout for stakes cache expiry
        self.memcached_timeout = s_cfg["timeout"]

    def process(self):
        start = time.perf_counter()

        self.logger.info(f"Fetching current stakes")

        stakes = self.witnet_node.get_stakes(None, None)
        if "error" in stakes:
            self.logger.error(f"Could not fetch stakes: {stakes['error']}")
            return False
        stakes = stakes["result"]

        self.logger.info(
            f"Fetched current stakes for {len(stakes)} validators in {time.perf_counter() - start:.2f}s"
        )

        self.logger.info(f"Fetching data for all validators")

        wit2_activation_epoch = get_activatation_epoch_wit2()

        # Count the number of blocks created per validator and sum the associated transactions fees
        sql = """
            SELECT
                miner,
                COUNT(*),
                SUM(txns_fees),
                MAX(mint_txns.epoch)
            FROM
                mint_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch=mint_txns.epoch
            WHERE
                mint_txns.epoch>=%s
            GROUP BY
                miner
        """
        blocks = self.database.sql_return_all(
            re_sql(sql),
            parameters=(wit2_activation_epoch,),
        )
        blocks_mined = {miner: num for miner, num, _, _ in blocks}
        blocks_fees = {miner: int(fees) for miner, _, fees, _ in blocks}
        last_block_mined = {
            miner: calculate_timestamp_from_epoch(epoch)
            for miner, _, _, epoch in blocks
        }

        # Count the number of data requests this validator committed to resolve
        sql = """
            SELECT
                txn_address,
                COUNT(*)
            FROM
                commit_txns
            WHERE
                epoch>=%s
            GROUP BY
                txn_address
        """
        data_requests_solved = self.database.sql_return_all(
            re_sql(sql),
            parameters=(wit2_activation_epoch,),
        )
        data_requests_solved = {miner: num for miner, num in data_requests_solved}

        # Check how many time a validator lied and how much collateral is lost due to it
        sql = """
            SELECT
                liar_addresses,
                collateral
            FROM
                tally_txns
            LEFT JOIN
                data_request_txns
            ON
                tally_txns.data_request=data_request_txns.txn_hash
            WHERE
                tally_txns.epoch>=%s
            AND
                CARDINALITY(liar_addresses)>0
        """
        tally_liars = self.database.sql_return_all(
            re_sql(sql),
            parameters=(wit2_activation_epoch,),
        )
        liar_addresses, collateral_lost = {}, {}
        for addresses, collateral in tally_liars:
            for address in addresses:
                if address not in liar_addresses:
                    liar_addresses[address] = 0
                liar_addresses[address] += 1
                if address not in collateral_lost:
                    collateral_lost[address] = 0
                collateral_lost[address] += collateral

        # Get the data request rewards for a validator (excluding all data requests in which an error or lie was committed)
        sql = """
            SELECT
                commit_txns.txn_address,
                SUM(data_request_txns.witness_reward)
            FROM
                commit_txns
            LEFT JOIN
                data_request_txns
            ON
                commit_txns.data_request=data_request_txns.txn_hash
            LEFT JOIN
                tally_txns
            ON
                commit_txns.data_request=tally_txns.data_request
            WHERE
                commit_txns.epoch>=%s
            AND
                tally_txns.epoch>=%s
            AND
                NOT (tally_txns.liar_addresses @> ARRAY[commit_txns.txn_address]::CHAR({length})[])
            AND
                NOT (tally_txns.error_addresses @> ARRAY[commit_txns.txn_address]::CHAR({length})[])
            GROUP BY
                commit_txns.txn_address
        """
        data_request_rewards = self.database.sql_return_all(
            SQL(re_sql(sql)).format(length=self.address_length),
            parameters=(wit2_activation_epoch, wit2_activation_epoch),
        )
        data_request_rewards = {
            miner: int(reward) for miner, reward in data_request_rewards
        }

        # Get the genesis epoch per validator
        sql = """
            SELECT
                validator,
                MIN(epoch)
            FROM
                stake_txns
            GROUP BY
                validator
        """
        stake_epochs = self.database.sql_return_all(re_sql(sql))
        stake_timestamps = {
            validator: calculate_timestamp_from_epoch(epoch)
            for validator, epoch in stake_epochs
        }

        # Get the last commit epoch per validator
        sql = """
            SELECT
                txn_address,
                MAX(epoch)
            FROM
                commit_txns
            GROUP BY
                txn_address
        """
        last_commit = self.database.sql_return_all(re_sql(sql))
        last_commit = {
            txn_address: calculate_timestamp_from_epoch(epoch)
            for txn_address, epoch in last_commit
        }
        all_active_validators = list(
            set(last_commit.keys()).union(set(last_block_mined.keys()))
        )
        last_active = {
            validator: max(
                last_block_mined.get(validator, 0), last_commit.get(validator, 0)
            )
            for validator in all_active_validators
        }

        # Get the average stake of the validator over time
        current_epoch = calculate_current_epoch()
        sql = """
            SELECT
                epoch,
                validator,
                stake_value
            FROM
                stake_txns
        """
        transactions = self.database.sql_return_all(re_sql(sql))

        sql = """
            SELECT
                epoch,
                validator,
                -unstake_value
            FROM
                unstake_txns
        """
        unstake_txns = self.database.sql_return_all(re_sql(sql))
        transactions.extend(unstake_txns)
        transactions = sorted(transactions, key=lambda l: l[0])

        time_weighted_stake, first_stake_epoch = {}, {}
        # Add average stakes
        for epoch, validator, value in transactions:
            if validator not in time_weighted_stake:
                time_weighted_stake[validator] = value
                first_stake_epoch[validator] = epoch
            else:
                nominator = current_epoch - epoch
                denominator = current_epoch - first_stake_epoch[validator]
                time_weighted_stake[validator] += value * nominator / denominator

        self.all_stakes = []
        for stake in stakes:
            validator = stake["key"]["validator"]

            # A fresh validator might not have mined blocks
            num_blocks = 0
            if validator in blocks_mined:
                num_blocks = blocks_mined[validator]

            # Some validators might not feel like solving data requests
            num_data_requests = 0
            if validator in data_requests_solved:
                num_data_requests = data_requests_solved[validator]

            # A validator might be super honest and never lied
            num_lies = 0
            if validator in liar_addresses:
                num_lies = liar_addresses[validator]

            total_rewards = num_blocks * calculate_block_reward(wit2_activation_epoch)
            if validator in blocks_fees:
                total_rewards += blocks_fees[validator]
            if validator in data_request_rewards:
                total_rewards += data_request_rewards[validator]
            if validator in collateral_lost:
                total_rewards -= collateral_lost[validator]

            # It is possible a validator has neither mined a block nor committed to a data request yet
            active_timestamp = 0
            if validator in last_active:
                active_timestamp = last_active[validator]

            self.all_stakes.append(
                {
                    "validator": validator,
                    "withdrawer": stake["key"]["withdrawer"],
                    "current_stake": stake["value"]["coins"],
                    "time_weighted_stake": time_weighted_stake[validator],
                    "nonce": stake["value"]["nonce"],
                    "blocks": num_blocks,
                    "rewards": total_rewards,
                    "data_requests": num_data_requests,
                    "lies": num_lies,
                    "genesis": stake_timestamps[validator],
                    "last_active": active_timestamp,
                }
            )

        self.logger.info(
            f"Cached current stakes data in {time.perf_counter() - start:.2f}s"
        )

        return True

    def cache_stakes(self):
        try:
            # Cache all stakers data
            self.memcached_client.set(
                "network_stakes_all",
                {
                    "stakes": self.all_stakes,
                    "last_updated": int(time.time()),
                },
                time=self.memcached_timeout,
            )
        except pylibmc.TooBig as e:
            raise


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "--config-file",
        type="string",
        default="explorer.toml",
        dest="config_file",
        help="Specify a configuration file",
    )
    options, args = parser.parse_args()

    if options.config_file == None:
        sys.stderr.write("Need to specify a configuration file!\n")
        sys.exit(1)

    # Load config file
    BlockchainConfig.config = toml.load(options.config_file)
    BlockchainConfig.consensus_constants = ConsensusConstants()
    BlockchainConfig.wip = WIP()

    # Create stakes cache
    stakes = Stakes()
    if stakes.process():
        stakes.cache_stakes()


if __name__ == "__main__":
    main()
