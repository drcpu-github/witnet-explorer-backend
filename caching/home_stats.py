import marshmallow
import optparse
import pylibmc
import sys
import time
import toml

from caching.client import Client
from objects.wip import WIP
from schemas.misc.home_schema import HomeBlock, HomeNetworkStats, HomeTransaction, HomeResponse
from schemas.network.supply_schema import NetworkSupply
from util.data_transformer import re_sql
from util.logger import configure_logger

class HomeStats(Client):
    def __init__(self, config):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["home_stats"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["home_stats"]["level_file"]
        self.logger = configure_logger("home", log_filename, log_level)

        super().__init__(config)

        # Assign some of the consensus constants
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period

        wips = WIP(database_config=config["database"], node_config=config["node-pool"])
        self.wip0027_activation_epoch = wips.get_activation_epoch("WIP0027")

        # Initialize previous variables
        self.current_epoch = int((time.time() - self.start_time) / self.epoch_period)

        last_saved_home = self.memcached_client.get("home")
        self.default_supply_info = last_saved_home["supply_info"]
        self.last_saved_num_active_nodes = last_saved_home["network_stats"]["num_active_nodes"]
        self.last_saved_num_reputed_nodes = last_saved_home["network_stats"]["num_reputed_nodes"]
        self.last_saved_num_pending_requests = last_saved_home["network_stats"]["num_pending_requests"]

    def collect_home_stats(self):
        start = time.perf_counter()

        self.logger.info(f"Collecting home statistics")

        self.home_stats = {}

        start_inner = time.perf_counter()
        self.logger.info("Collecting network statistics")
        self.home_stats["network_stats"] = self.get_network_stats()
        self.logger.info(f"Collected network statistics in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting supply info")
        self.home_stats["supply_info"] = self.get_supply_info()
        self.logger.info(f"Collected supply info in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest blocks")
        self.home_stats["latest_blocks"] = self.get_latest_blocks()
        self.logger.info(f"Collected latest blocks in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest data requests")
        self.home_stats["latest_data_requests"] = self.get_latest_data_requests()
        self.logger.info(f"Collected latest data requests in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting latest value transfers")
        self.home_stats["latest_value_transfers"] = self.get_latest_value_transfers()
        self.logger.info(f"Collected latest value transfers in {time.perf_counter() - start_inner:.2f}s")

        self.home_stats["last_updated"] = int(time.time())

        HomeResponse().load(self.home_stats)

        self.logger.info(f"Collected home statistics in {time.perf_counter() - start:.2f}s")

    def get_network_stats(self):
        # Count the number of confirmed blocks
        sql = """
            SELECT
                COUNT(*)
            FROM blocks
            WHERE
                confirmed=true
        """
        num_blocks = self.database.sql_return_one(re_sql(sql))
        if num_blocks:
            num_blocks = num_blocks[0]
        else:
            num_blocks = 0

        # Count the total number of data requests included in all confirmed blocks
        sql = """
            SELECT
                SUM(data_request)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        num_data_requests = self.database.sql_return_one(re_sql(sql))
        if num_data_requests:
            num_data_requests = num_data_requests[0]
        else:
            num_data_requests = 0

        # Count the total number of value transfers included in all confirmed blocks
        sql = """
            SELECT
                SUM(value_transfer)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        num_value_transfers = self.database.sql_return_one(re_sql(sql))
        if num_value_transfers:
            num_value_transfers = num_value_transfers[0]
        else:
            num_value_transfers = 0

        # Fetch all reputation statistics from a witnet node
        # On error: use the previous active and reputed nodes
        # On success:
        #   1) sum active and reputed nodes separately
        #   2) update the previous active and reputed nodes
        active_nodes = self.witnet_node.get_reputation_all()
        if "error" in active_nodes:
            num_active_nodes = self.last_saved_num_active_nodes
            num_reputed_nodes = self.last_saved_num_reputed_nodes
        else:
            active_nodes = active_nodes["result"]
            num_active_nodes = sum([1 for key in active_nodes["stats"].keys() if active_nodes["stats"][key]["is_active"]])
            num_reputed_nodes = sum([1 for key in active_nodes["stats"].keys() if active_nodes["stats"][key]["reputation"] > 0])

        # Fetch the mempool from a witnet node
        # On error: use the previous pending requests
        # On success: 
        #   1) calculate the sum of all pending data requests and value transfers
        #   2) update the previous pending requests
        pending_requests = self.witnet_node.get_mempool()
        if "error" in pending_requests:
            num_pending_requests = self.last_saved_num_pending_requests
        else:
            pending_requests = pending_requests["result"]
            num_pending_requests = len(pending_requests["data_request"]) + len(pending_requests["value_transfer"])

        return HomeNetworkStats().load(
            {
                "epochs": self.current_epoch,
                "num_blocks": num_blocks,
                "num_data_requests": num_data_requests,
                "num_value_transfers": num_value_transfers,
                "num_active_nodes": num_active_nodes,
                "num_reputed_nodes": num_reputed_nodes,
                "num_pending_requests": num_pending_requests,
            }
        )

    def get_supply_info(self):
        # Fetch the supply info from a witnet node
        # On error: use the previous supply info
        # On success:
        #   1) extract the current supply info
        #   2) update the previous supply info
        supply_info = self.witnet_node.get_supply_info()
        if "error" in supply_info:
            return self.default_supply_info
        else:
            supply_info = supply_info["result"]

            supply_info["current_supply"] = supply_info["current_unlocked_supply"] + supply_info["current_locked_supply"]

            sql = """
                SELECT
                    data_request_txns.collateral,
                    tally_txns.liar_addresses
                FROM
                    data_request_txns
                LEFT JOIN
                    blocks
                ON
                    blocks.epoch = data_request_txns.epoch
                LEFT JOIN
                    tally_txns
                ON
                    data_request_txns.txn_hash = tally_txns.data_request
                WHERE
                    blocks.confirmed = true
                AND
                    blocks.epoch >= %s
            """ % self.wip0027_activation_epoch
            self.database.reset_cursor()
            burn_rate_data = self.database.sql_return_all(re_sql(sql))

            supply_info["supply_burned_lies"] = sum(collateral * len(liar_addresses) for collateral, liar_addresses in burn_rate_data)

            supply_info["total_supply"] = supply_info["maximum_supply"] - supply_info["blocks_missing_reward"] - supply_info["supply_burned_lies"]

            return NetworkSupply().load(supply_info)

    def get_latest_blocks(self):
        # Fetch the last 32 blocks + metadata from the database
        sql = """
            SELECT
                block_hash,
                data_request,
                value_transfer,
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
        for block_hash, data_request, value_transfer, epoch, confirmed in result:
            timestamp = self.start_time + (epoch + 1) * self.epoch_period
            blocks.append(
                HomeBlock().load(
                    {
                        "hash": block_hash.hex(),
                        "data_request": data_request,
                        "value_transfer": value_transfer,
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
                timestamp = self.start_time + (epoch + 1) * self.epoch_period
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
                timestamp = self.start_time + (epoch + 1) * self.epoch_period
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

    def save_home_stats(self):
        self.logger.info("Saving all data in the memcached instance")

        # Save the a JSON object summarizing all statistics for the home page in the memcached client
        try:
            self.memcached_client.set("home", self.home_stats)
        except pylibmc.TooBig as e:
            self.logger.warning("Could not save items in cache because the item size exceeded 1MB")

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file", help="Specify a configuration file")
    options, args = parser.parse_args()

    if options.config_file == None:
        sys.stderr.write("Need to specify a configuration file!\n")
        sys.exit(1)

    # Load config file
    config = toml.load(options.config_file)

    # Create home cache
    home_cache = HomeStats(config)
    home_cache.collect_home_stats()
    home_cache.save_home_stats()

if __name__ == "__main__":
    main()