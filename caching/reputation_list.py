import optparse
import pylibmc
import sys
import time
import toml

from caching.client import Client
from schemas.network.reputation_schema import NetworkReputationResponse
from util.logger import configure_logger

class ReputationList(Client):
    def __init__(self, config):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["reputation_list"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["reputation_list"]["level_file"]
        self.logger = configure_logger("reputation", log_filename, log_level)

        # Read some Witnet node parameters
        self.node_retries = config["api"]["caching"]["node_retries"]

        super().__init__(config)

    def get_reputation(self):
        start = time.perf_counter()

        self.logger.info("Fetching reputation for all addresses")

        # Fetch reputation for all addresses from node
        result = self.witnet_node.get_reputation_all()

        # On fail: retry for a configurable amount of times (adding a sleep timeout)
        attempts = 0
        while type(result) is dict and "error" in result:
            self.logger.error(f"Failed to fetch reputation for all addresses: {result}")

            result = self.witnet_node.get_reputation_all()
            if "result" in result:
                break

            attempts += 1
            if attempts == self.node_retries:
                self.logger.error(f"Maximum retries ({self.node_retries}) to fetch reputation for all addresses exceeded")
                return False

            # Sleep for an increasing timeout
            time.sleep(attempts)

        # Parse reputation statistics
        stats = result["result"]["stats"]
        total_reputation = result["result"]["total_reputation"]
        # Only keep identities with a non-zero reputation
        reputation = [
            {
                "address": key,
                "reputation": stats[key]["reputation"],
                "eligibility": stats[key]["eligibility"] / total_reputation * 100
            } for key in stats.keys() if stats[key]["reputation"] > 0
        ]
        reputation = sorted(reputation, key=lambda l: l["reputation"], reverse=True)

        self.reputation = NetworkReputationResponse().load({
            "reputation": reputation,
            "total_reputation": result["result"]["total_reputation"],
            "last_updated": int(time.time())
        })

        self.logger.info(f"Fetched reputation data for the ARS in {time.perf_counter() - start:.2f}s")

        return True

    def save_reputation(self):
        self.logger.info("Saving all data in our memcached instance")
        try:
            self.memcached_client.set("reputation", self.reputation)
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

    # Create reputation cache
    reputation_cache = ReputationList(config)
    # Only save reputation to the memcached instance on fetch and process success
    if reputation_cache.get_reputation():
        reputation_cache.save_reputation()

if __name__ == "__main__":
    main()
