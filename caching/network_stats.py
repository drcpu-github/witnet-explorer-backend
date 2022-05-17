import optparse
import pylibmc
import sys
import time
import toml

from caching.client import Client

from util.logger import configure_logger

class NetworkStats(Client):
    def __init__(self, config):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["network_stats"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["network_stats"]["level_file"]
        self.logger = configure_logger("network", log_filename, log_level)

        # Create database client, memcached client and a consensus constants object
        super().__init__(config, database=True, memcached_client=True, consensus_constants=True)

        # Assign some of the consensus constants
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period

    def build_network_stats(self):
        start = time.perf_counter()

        self.logger.info(f"Building network statistics")

        inner_start = time.perf_counter()
        self.logger.info("Collecting rollbacks")
        self.get_rollbacks()
        self.logger.info(f"Found {len(self.rollbacks)} rollbacks in {time.perf_counter() - inner_start:.2f}s")

        inner_start = time.perf_counter()
        self.logger.info("Collecting miners")
        self.get_unique_miners()
        self.logger.info(f"Found {len(self.unique_miners)} miners in {time.perf_counter() - inner_start:.2f}s")

        inner_start = time.perf_counter()
        self.logger.info("Collecting data request solvers")
        self.get_unique_data_request_solvers()
        self.logger.info(f"Found {len(self.unique_data_request_solvers)} data request solvers in {time.perf_counter() - inner_start:.2f}s")

        self.logger.info(f"Built network stats in {time.perf_counter() - start:.2f}s")

        self.network = {
            "rollbacks": self.latest_rollbacks,
            "miners": len(self.unique_miners),
            "data_request_solvers": len(self.unique_data_request_solvers),
            "miner_top_100": self.miner_top_100,
            "data_request_solver_top_100": self.data_request_solver_top_100,
            "last_updated": int(time.time()),
        }

    def get_rollbacks(self):
        # Fetch all confirmed blocks
        sql = """
            SELECT
                epoch
            FROM blocks
            WHERE
                blocks.confirmed=true
            ORDER BY epoch ASC
        """
        epoch_data = self.witnet_database.sql_return_all(sql)

        previous_epoch = 0
        self.rollbacks = []
        for epoch in epoch_data:
            # If there is a gap of more than 1 epoch between two consecutive blocks, we mark it as a rollback
            if epoch[0] > previous_epoch + 1:
                # Calculate the timestamp of the rollback and its boundaries
                timestamp = self.start_time + (previous_epoch + 1) * self.epoch_period
                self.rollbacks.append((timestamp, previous_epoch + 1, epoch[0] - 1, epoch[0] - previous_epoch - 1))
            previous_epoch = epoch[0]
        # List the 100 latest rollbacks
        self.latest_rollbacks = sorted(self.rollbacks, reverse=True)[:100]

    def get_unique_miners(self):
        # Fetch all confirmed mint transactions (as a proxy for blocks) and count unique miners (block producers)
        sql = """
            SELECT
                mint_txns.miner,
                COUNT(mint_txns.miner)
            FROM mint_txns
            LEFT JOIN blocks ON
                blocks.epoch=mint_txns.epoch
            WHERE
                blocks.confirmed=true
            GROUP BY
                mint_txns.miner
        """
        self.unique_miners = self.witnet_database.sql_return_all(sql)
        # Reverse sort and extract the top 100 miners
        self.miner_top_100 = sorted(self.unique_miners, key=lambda l: l[1], reverse=True)[:100]

    def get_unique_data_request_solvers(self):
        # Fetch all confirmed commit transactions and count unique addresses
        sql = """
            SELECT
                commit_txns.txn_address,
                COUNT(commit_txns.txn_address)
            FROM commit_txns
            LEFT JOIN blocks ON
                blocks.epoch=commit_txns.epoch
            WHERE
                blocks.confirmed=true
            GROUP BY
                commit_txns.txn_address
        """
        self.unique_data_request_solvers = self.witnet_database.sql_return_all(sql)
        # Reverse sort and extract the top 100 data request solvers
        self.data_request_solver_top_100 = sorted(self.unique_data_request_solvers, key=lambda l: l[1], reverse=True)[:100]

    def save_network(self):
        self.logger.info("Saving all data in our memcached instance")
        try:
            self.memcached_client.set("network", self.network)
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

    # Create network cache
    network_cache = NetworkStats(config)
    network_cache.build_network_stats()
    network_cache.save_network()

if __name__ == "__main__":
    main()
