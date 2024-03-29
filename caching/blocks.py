import optparse
import pylibmc
import sys
import time
import toml

from marshmallow import ValidationError

from caching.client import Client
from blockchain.objects.block import Block
from util.data_transformer import re_sql
from util.logger import configure_logger
from util.memcached import calculate_timeout
from util.common_sql import sql_last_block

class Blocks(Client):
    def __init__(self, config):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["blocks"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["blocks"]["level_file"]
        self.logger = configure_logger("block", log_filename, log_level)

        super().__init__(config)

        self.node_config = config["node-pool"]

        # Fetch configured timeout for block cache expiry
        self.memcached_timeout = config["api"]["caching"]["scripts"]["blocks"]["timeout"]
        # Calculate how many epochs in the past this script has to cache blocks
        self.lookback_epochs = int(config["api"]["caching"]["scripts"]["blocks"]["timeout"] / self.consensus_constants.checkpoints_period)

        self.superblock_period = self.consensus_constants.superblock_period

    def process(self, force_update):
        start = time.perf_counter()

        # Fetch the most recently added epoch
        last = self.database.sql_return_one(sql_last_block)
        if last:
            last_epoch = last[1]
        else:
            last_epoch = 1E31

        # check until which epoch we succesfully added blocks to the cache
        # if no epoch is found, start at the current epoch minus the TOML-defined timeout
        # if force_update is enabled, update all blocks up to self.lookback_epochs ago
        blocks_epoch = self.get_start_epoch("blocks_epoch")
        if not blocks_epoch or force_update:
            blocks_epoch = last_epoch - self.lookback_epochs

        self.logger.info(f"Fetching blocks starting at epoch {blocks_epoch} to {last_epoch}")

        sql = """
            SELECT
                block_hash,
                epoch
            FROM
                blocks
            WHERE
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (blocks_epoch, last_epoch)
        blocks = self.database.sql_return_all(re_sql(sql))

        self.logger.info(f"Collected {len(blocks)} blocks in {time.perf_counter() - start:.2f}s")
        self.logger.info(f"Building blocks starting at epoch {blocks_epoch}")

        new_blocks, updated_blocks = 0, 0
        for block_hash, epoch in blocks:
            inner_start = time.perf_counter()

            # Try to fetch this block
            block_hash = block_hash.hex()
            json_block = self.memcached_client.get(block_hash)

            # Check if the block is already present in the cache or if a forced update is required
            if not json_block or force_update:
                try:
                    json_block = self.build_block(block_hash, epoch)
                except ValidationError:
                    self.logger.error(f"Block {block_hash} does not pass the Marshmallow validation")
                    continue

                if json_block is None:
                    continue

                # Try to insert the block in the cache
                try:
                    self.cache_block(last_epoch, epoch, block_hash, json_block)

                    new_blocks += 1

                    confirmed = json_block["details"]["confirmed"]
                    # track the last epoch for which we successfully added a confirmed block to the cache
                    # on the next execution of this script, it will start processing blocks from that epoch
                    if confirmed:
                        blocks_epoch = epoch

                    self.logger.info(f"Built {'confirmed' if confirmed else 'unconfirmed'} block {block_hash} for epoch {epoch} and added it to the memcached cache in {time.perf_counter() - inner_start:.2f}s")
                except pylibmc.TooBig as e:
                    self.logger.warning(f"Built block {block_hash} for epoch {epoch} in {time.perf_counter() - inner_start:.2f}s, but could not save it in the memcached instance because its size exceeded 1MB")
            else:
                json_block = json_block["block"]
                superblock_epoch = int(json_block["details"]["epoch"] / self.superblock_period) * self.superblock_period
                if not json_block["details"]["confirmed"] and last_epoch >= superblock_epoch + self.superblock_period * 2:
                    json_block = self.build_block(block_hash, epoch)

                    if json_block is None:
                        continue

                    if json_block["details"]["confirmed"]:
                        self.cache_block(last_epoch, epoch, block_hash, json_block)

                        # track the last epoch for which we successfully added a confirmed block to the cache
                        # on the next execution of this script, it will start processing blocks from that epoch
                        blocks_epoch = epoch

                        updated_blocks += 1

                        self.logger.info(f"Updated block {block_hash} for epoch {epoch} in the memcached cache in {time.perf_counter() - inner_start:.2f}s")
                else:
                    self.logger.debug(f"Found block {block_hash} for epoch {epoch} in memcached cache in {time.perf_counter() - inner_start:.2f}s")

        # Save the most recent epoch for which we sucessfully cached a block
        self.set_start_epoch("blocks_epoch", blocks_epoch)

        self.logger.info(f"Cached {new_blocks} and updated {updated_blocks} recent blocks in {time.perf_counter() - start:.2f}s")

    def build_block(self, block_hash, epoch):
        # Build block
        block = Block(self.consensus_constants, block_hash=block_hash, logger=self.logger, database=self.database, database_config=self.config["database"], node_config=self.node_config)
        json_block = block.process_block("api")
        if "error" in json_block:
            self.logger.warning(f"Could not fetch block {block_hash} for epoch {epoch}")
            return None
        return json_block

    def cache_block(self, last_epoch, epoch, block_hash, json_block):
        try:
            # Cache older blocks for a shorter amount of time proportional to mimic the normal expiry time
            timeout = calculate_timeout(int(self.memcached_timeout * (self.lookback_epochs - last_epoch + epoch) / self.lookback_epochs))
            # First, cache the full block based on the hash
            self.memcached_client.set(
                block_hash,
                {
                    "response_type": "block",
                    "block": json_block,
                },
                time=timeout
            )
            # Second, cache the block epoch to block hash mapping
            self.memcached_client.set(str(epoch), block_hash, time=timeout)
        except pylibmc.TooBig as e:
            raise

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file", help="Specify a configuration file")
    parser.add_option("--force-update", action="store_true", dest="force_update", help="Use this flag to force an update of all blocks which should be cached")
    options, args = parser.parse_args()

    if options.config_file == None:
        sys.stderr.write("Need to specify a configuration file!\n")
        sys.exit(1)

    # Load config file
    config = toml.load(options.config_file)

    # Create block cache
    blocks = Blocks(config)
    blocks.process(options.force_update)

if __name__ == "__main__":
    main()
