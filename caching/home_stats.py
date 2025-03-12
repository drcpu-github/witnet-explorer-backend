import marshmallow
import optparse
import pylibmc
import sys
import time
import toml

from blockchain.config import BlockchainConfig
from blockchain.consensus_constants import ConsensusConstants
from blockchain.objects.wip import WIP
from caching.client import Client
from schemas.misc.home_schema import (
    HomeBlock,
    HomeNetworkStats,
    HomeTransaction,
    HomeResponse,
)
from schemas.network.supply_schema import NetworkSupply
from util.blockchain_functions import (
    calculate_current_epoch,
    calculate_epoch_from_timestamp,
    calculate_timestamp_from_epoch,
    get_activatation_epoch_wit2,
)
from util.data_transformer import re_sql
from util.logger import configure_logger


class HomeStats(Client):
    def __init__(self):
        h_cfg = BlockchainConfig.config["api"]["caching"]["scripts"]["home_stats"]

        # Setup logger
        self.logger = configure_logger("home", h_cfg["log_file"], h_cfg["level_file"])

        super().__init__(BlockchainConfig.config)

        # Initialize previous variables
        self.current_epoch = calculate_current_epoch()

        self.default_supply_info = None
        self.last_saved_validators = 0
        self.last_saved_stakes = 0
        self.last_saved_unstakes = 0
        self.last_saved_pending_requests = 0
        self.last_saved_total_staked = 0
        last_saved_home = self.memcached_client.get("home")
        if last_saved_home:
            self.default_supply_info = last_saved_home["supply_info"]
            network_stats = last_saved_home["network_stats"]
            if "validators" in network_stats:
                self.last_saved_validators = network_stats["validators"]
            if "stakes" in network_stats:
                self.last_saved_stakes = network_stats["stakes"]
            if "unstakes" in last_saved_home["network_stats"]:
                self.last_saved_unstakes = network_stats["unstakes"]
            if "pending_requests" in network_stats:
                self.last_saved_pending_requests = network_stats["pending_requests"]
            if "total_staked" in last_saved_home:
                self.last_saved_total_staked = last_saved_home["total_staked"]

    def collect_home_stats(self):
        start = time.perf_counter()

        self.logger.info(f"Collecting home statistics")

        self.home_stats = {}

        start_inner = time.perf_counter()
        self.logger.info("Collecting network statistics")
        self.home_stats["network_stats"] = self.get_network_stats()
        self.logger.info(
            f"Collected network statistics in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting supply info")
        self.home_stats["supply_info"] = self.get_supply_info()
        self.logger.info(
            f"Collected supply info in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest blocks")
        self.home_stats["latest_blocks"] = self.get_latest_blocks()
        self.logger.info(
            f"Collected latest blocks in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest data requests")
        self.home_stats["latest_data_requests"] = self.get_latest_data_requests()
        self.logger.info(
            f"Collected latest data requests in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest value transfers")
        self.home_stats["latest_value_transfers"] = self.get_latest_value_transfers()
        self.logger.info(
            f"Collected latest value transfers in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest stakes")
        self.home_stats["latest_stakes"] = self.get_latest_stakes()
        self.logger.info(
            f"Collected latest stakes in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest unstakes")
        self.home_stats["latest_unstakes"] = self.get_latest_unstakes()
        self.logger.info(
            f"Collected latest unstakes in {time.perf_counter() - start_inner:.2f}s"
        )

        start_inner = time.perf_counter()
        self.logger.info("Collecting total staked")
        self.home_stats["total_staked"] = self.get_total_staked()
        self.logger.info(
            f"Collected total staked in {time.perf_counter() - start_inner:.2f}s"
        )

        self.home_stats["last_updated"] = int(time.time())

        HomeResponse().load(self.home_stats)

        self.logger.info(
            f"Collected home statistics in {time.perf_counter() - start:.2f}s"
        )

    def get_network_stats(self):
        stakes = self.witnet_node.get_stakes(None, None)
        if "error" in stakes:
            validators = self.last_saved_validators
        else:
            validators = len(stakes["result"])

        # Count the number of confirmed blocks
        sql = """
            SELECT
                COUNT(*)
            FROM blocks
            WHERE
                confirmed=true
        """
        blocks = self.database.sql_return_one(re_sql(sql))
        if blocks:
            blocks = blocks[0]
        else:
            blocks = 0

        # Count the total number of data requests included in all confirmed blocks
        sql = """
            SELECT
                SUM(data_request)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        data_requests = self.database.sql_return_one(re_sql(sql))
        if data_requests:
            data_requests = data_requests[0]
        else:
            data_requests = 0

        # Count the total number of value transfers included in all confirmed blocks
        sql = """
            SELECT
                SUM(value_transfer)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        value_transfers = self.database.sql_return_one(re_sql(sql))
        if value_transfers:
            value_transfers = value_transfers[0]
        else:
            value_transfers = 0

        # Count the total number of stakes included in all confirmed blocks
        sql = """
            SELECT
                SUM(stake)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        stakes = self.database.sql_return_one(re_sql(sql))
        if stakes:
            stakes = stakes[0]
        else:
            stakes = 0

        # Count the total number of unstakes included in all confirmed blocks
        sql = """
            SELECT
                SUM(unstake)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        unstakes = self.database.sql_return_one(re_sql(sql))
        if unstakes:
            unstakes = unstakes[0]
        else:
            unstakes = 0

        # Fetch the mempool from a witnet node
        # On error: use the previous pending requests
        # On success:
        #   1) calculate the sum of all pending data requests and value transfers
        #   2) update the previous pending requests
        pending_requests = self.witnet_node.get_mempool()
        if "error" in pending_requests:
            pending_requests = self.last_saved_pending_requests
        else:
            pending_requests = pending_requests["result"]
            pending_requests = (
                len(pending_requests["data_request"])
                + len(pending_requests["value_transfer"])
                + len(pending_requests["stake"])
                + len(pending_requests["unstake"])
            )

        return HomeNetworkStats().load(
            {
                "validators": validators,
                "epochs": self.current_epoch,
                "blocks": blocks,
                "data_requests": data_requests,
                "value_transfers": value_transfers,
                "stakes": stakes,
                "unstakes": unstakes,
                "pending_requests": pending_requests,
            }
        )

    def get_supply_info(self):
        # Fetch the supply info from a witnet node
        # On error: use the previous supply info
        # On success:
        #   1) extract the current supply info
        #   2) update the previous supply info
        supply_info = self.witnet_node.get_supply_info()
        supply_info_2 = self.witnet_node.get_supply_info_2()
        if "error" in supply_info or "error" in supply_info_2:
            return self.default_supply_info
        else:
            supply_info = supply_info["result"]
            supply_info_2 = supply_info_2["result"]

            del supply_info["maximum_supply"]

            supply_info["current_staked_supply"] = supply_info_2[
                "current_staked_supply"
            ]

            supply_info["current_supply"] = (
                supply_info_2["initial_supply"] + supply_info_2["blocks_minted_reward"]
            )
            supply_info["supply_burned_lies"] = supply_info_2["burnt_supply"]

            return NetworkSupply().load(supply_info)

    def get_latest_blocks(self):
        # Fetch the last 32 blocks + metadata from the database
        sql = """
            SELECT
                block_hash,
                data_request,
                value_transfer,
                stake,
                unstake,
                epoch,
                confirmed
            FROM
                blocks
            ORDER BY
                epoch
            DESC
            LIMIT 32
        """
        result = self.database.sql_return_all(re_sql(sql))

        # Add the number of data requests and value transfers and calculate the block timestamp
        blocks = []
        for (
            block_hash,
            data_request,
            value_transfer,
            stake,
            unstake,
            epoch,
            confirmed,
        ) in result:
            timestamp = calculate_timestamp_from_epoch(epoch)
            blocks.append(
                HomeBlock().load(
                    {
                        "hash": block_hash.hex(),
                        "data_request": data_request,
                        "value_transfer": value_transfer,
                        "stake": stake,
                        "unstake": unstake,
                        "timestamp": timestamp,
                        "confirmed": confirmed,
                    }
                )
            )

        return blocks

    def get_latest_data_requests(self):
        # Fetch the latest 32 data request transactions
        sql = """
            SELECT
                data_request_txns.txn_hash,
                data_request_txns.epoch,
                blocks.confirmed
            FROM
                data_request_txns
            LEFT JOIN
                blocks
            ON
                data_request_txns.epoch=blocks.epoch
            ORDER BY
                epoch
            DESC
            LIMIT 32
        """
        result = self.database.sql_return_all(re_sql(sql))

        # Calculate the data requests timestamp
        data_requests = []
        if result:
            for txn_hash, epoch, block_confirmed in result:
                timestamp = calculate_timestamp_from_epoch(epoch)
                data_requests.append(
                    HomeTransaction().load(
                        {
                            "hash": txn_hash.hex(),
                            "timestamp": timestamp,
                            "confirmed": block_confirmed,
                        }
                    )
                )

        return data_requests

    def get_latest_value_transfers(self):
        # Fetch the latest 32 value transfers transactions
        sql = """
            SELECT
                value_transfer_txns.txn_hash,
                value_transfer_txns.epoch,
                blocks.confirmed
            FROM
                value_transfer_txns
            LEFT JOIN
                blocks
            ON
                value_transfer_txns.epoch=blocks.epoch
            ORDER BY
                epoch
            DESC
            LIMIT 32
        """
        result = self.database.sql_return_all(re_sql(sql))

        # Calculate the value transfer timestamp
        value_transfers = []
        if result:
            for txn_hash, epoch, block_confirmed in result:
                timestamp = calculate_timestamp_from_epoch(epoch)
                value_transfers.append(
                    HomeTransaction().load(
                        {
                            "hash": txn_hash.hex(),
                            "timestamp": timestamp,
                            "confirmed": block_confirmed,
                        }
                    )
                )

        return value_transfers

    def get_latest_stakes(self):
        # Fetch the latest 32 stake transactions
        sql = """
            SELECT
                stake_txns.txn_hash,
                stake_txns.epoch,
                blocks.confirmed
            FROM
                stake_txns
            LEFT JOIN
                blocks
            ON
                stake_txns.epoch=blocks.epoch
            ORDER BY
                epoch
            DESC
            LIMIT 32
        """
        result = self.database.sql_return_all(re_sql(sql))

        # Calculate the value transfer timestamp
        stakes = []
        if result:
            for txn_hash, epoch, block_confirmed in result:
                timestamp = calculate_timestamp_from_epoch(epoch)
                stakes.append(
                    HomeTransaction().load(
                        {
                            "hash": txn_hash.hex(),
                            "timestamp": timestamp,
                            "confirmed": block_confirmed,
                        }
                    )
                )

        return stakes

    def get_latest_unstakes(self):
        # Fetch the latest 32 value transfers transactions
        sql = """
            SELECT
                unstake_txns.txn_hash,
                unstake_txns.epoch,
                blocks.confirmed
            FROM
                unstake_txns
            LEFT JOIN
                blocks
            ON
                unstake_txns.epoch=blocks.epoch
            ORDER BY
                epoch
            DESC
            LIMIT 32
        """
        result = self.database.sql_return_all(re_sql(sql))

        # Calculate the value transfer timestamp
        unstakes = []
        if result:
            for txn_hash, epoch, block_confirmed in result:
                timestamp = calculate_timestamp_from_epoch(epoch)
                unstakes.append(
                    HomeTransaction().load(
                        {
                            "hash": txn_hash.hex(),
                            "timestamp": timestamp,
                            "confirmed": block_confirmed,
                        }
                    )
                )

        return unstakes

    def get_total_staked(self):
        # Use the previously calculated last total staked if it exists
        previous_processed_epoch = -1
        if self.last_saved_total_staked:
            previous_processed_epoch = calculate_epoch_from_timestamp(
                self.last_saved_total_staked[-1]["timestamp"]
            )

        total_staked = {
            epoch: {"timestamp": calculate_timestamp_from_epoch(epoch), "staked": 0}
            for epoch in range(previous_processed_epoch + 1, self.current_epoch)
        }

        sql = """
            SELECT
                stake_txns.stake_value,
                stake_txns.epoch
            FROM
                stake_txns
            WHERE
                epoch > %s
            ORDER BY
                epoch
        """
        stakes = self.database.sql_return_all(
            re_sql(sql),
            parameters=(previous_processed_epoch,),
        )

        sql = """
            SELECT
                -unstake_txns.unstake_value,
                unstake_txns.epoch
            FROM
                unstake_txns
            WHERE
                epoch > %s
            ORDER BY
                epoch
        """
        unstakes = self.database.sql_return_all(
            re_sql(sql),
            parameters=(previous_processed_epoch,),
        )

        sql = """
            SELECT
                epoch
            FROM
                blocks
            WHERE
                epoch > %s
            ORDER BY
                epoch
        """
        blocks = self.database.sql_return_all(
            re_sql(sql),
            parameters=(max(previous_processed_epoch, get_activatation_epoch_wit2()),),
        )

        # Set stakes
        transactions = sorted(stakes + unstakes, key=lambda l: l[1])
        for value, epoch in transactions:
            total_staked[epoch]["staked"] += value

        block_reward = BlockchainConfig.consensus_constants.wit2_block_reward
        for (epoch,) in blocks:
            total_staked[epoch]["staked"] += block_reward

        # Accumulate stakes
        for epoch, staked in sorted(total_staked.items()):
            if epoch == previous_processed_epoch + 1:
                # Use the last staked amount from the previous iteration
                if self.last_saved_total_staked:
                    previous_stake = self.last_saved_total_staked[-1]["staked"]
                # Or use 0 assuming this is the first epoch
                else:
                    previous_stake = 0
            else:
                previous_stake = total_staked[epoch - 1]["staked"]
            total_staked[epoch]["staked"] = previous_stake + staked["staked"]

        if self.last_saved_total_staked:
            # Append every 100th epoch to the previous collected total staked if it exists
            truncated_total_staked = self.last_saved_total_staked
            time_diff = (
                self.last_saved_total_staked[1]["timestamp"]
                - self.last_saved_total_staked[0]["timestamp"]
            )
            for epoch, stake in total_staked.items():
                if (
                    calculate_timestamp_from_epoch(epoch)
                    - truncated_total_staked[-1]["timestamp"]
                    >= time_diff
                ):
                    truncated_total_staked.append(stake)
            truncated_total_staked = truncated_total_staked[
                len(truncated_total_staked) - 1000 :
            ]
        else:
            # From the last 100k epochs, keep every 100th element
            truncated_total_staked = [
                stake
                for epoch, stake in total_staked.items()
                if epoch >= self.current_epoch - 100000 and epoch % 100 == 0
            ]

        return truncated_total_staked

    def save_home_stats(self):
        self.logger.info("Saving all data in the memcached instance")

        # Save the a JSON object summarizing all statistics for the home page in the memcached client
        try:
            self.memcached_client.set("home", self.home_stats)
        except pylibmc.TooBig as e:
            self.logger.warning(
                "Could not save items in cache because the item size exceeded 1MB"
            )


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

    # Create home cache
    home_cache = HomeStats()
    home_cache.collect_home_stats()
    home_cache.save_home_stats()


if __name__ == "__main__":
    main()
