import numpy
import optparse
import pylibmc
import sys
import time
import toml

from caching.client import Client

from objects.wip import WIP

from util.data_transformer import re_sql
from util.helper_functions import calculate_block_reward
from util.logger import configure_logger

class NetworkStats(Client):
    def __init__(self, config, reset):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["network_stats"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["network_stats"]["level_file"]
        self.logger = configure_logger("network", log_filename, log_level)

        # Create node client, database client, memcached client and a consensus constants object
        timeout = config["api"]["caching"]["scripts"]["network_stats"]["node_timeout"]
        super().__init__(config, node=True, timeout=timeout, database=True, named_cursor=True, memcached_client=True, consensus_constants=True)

        # Assign some of the consensus constants
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period

        # Granularity at which network statistics are aggregated
        self.aggregation_epochs = config["api"]["caching"]["scripts"]["network_stats"]["aggregation_epochs"]

        self.wips = WIP(config["database"])

        self.last_update_time = int(time.time())

        if reset:
            self.last_processed_epoch = 0
            self.last_processed_epoch_update = 0
        else:
            # Last processed epoch
            self.last_processed_epoch = self.memcached_client.get("network_epoch")
            if self.last_processed_epoch == None:
                self.last_processed_epoch = 0
            self.last_processed_epoch_update = 0

        self.logger.info(f"Last processed epoch was {self.last_processed_epoch}")

        self.last_confirmed_epoch = self.get_last_confirmed_epoch()
        if self.last_confirmed_epoch == -1:
            self.logger.warning("Could not fetch last confirmed epoch")
        self.last_confirmed_epoch_ceiled = int(self.last_confirmed_epoch / self.aggregation_epochs + 1) * self.aggregation_epochs

    def get_last_confirmed_epoch(self):
        sql = """
            SELECT
                epoch,
                confirmed
            FROM
                blocks
            WHERE
                confirmed=true
            ORDER BY
                epoch
            DESC
            LIMIT
                1
        """
        self.witnet_database.db_mngr.reset_cursor()
        result = self.witnet_database.sql_return_one(re_sql(sql))

        if result:
            return int(result[0])
        else:
            return -1

    def build_network_stats(self, reset):
        start_outer = time.perf_counter()

        self.logger.info(f"Building network statistics")

        start_inner = time.perf_counter()
        self.logger.info("Collecting rollbacks")
        self.get_rollbacks(reset)
        self.logger.info(f"Found {len(self.rollbacks)} rollbacks in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting miners per period")
        self.get_miners_per_period(reset)
        self.logger.info(f"Found {self.num_unique_miners} miners in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting data request solvers per period")
        self.get_data_request_solvers_per_period(reset)
        self.logger.info(f"Found {self.num_unique_data_request_solvers} data request solvers in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting data requests per period")
        self.get_data_requests_per_period(reset)
        self.logger.info(f"Calculated data request for {len(self.data_requests_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting lie rate data per period")
        self.get_lie_rates_per_period(reset)
        self.logger.info(f"Calculated lie rate data for {len(self.lie_rates_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting burn rate data per period")
        self.get_burn_rate_per_period(reset)
        self.logger.info(f"Calculated burn rate data for {len(self.burn_rate_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting TRS data per period")
        self.get_trs_data_per_period(reset)
        self.logger.info(f"Calculated TRS data for {len(self.trs_data_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting value transfers per period")
        self.get_value_transfers_per_period(reset)
        self.logger.info(f"Collected value transfers {len(self.value_transfers_period)} for periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Collecting staking statistics")
        self.get_staking_stats()
        self.logger.info(f"Collected staking statistics in {time.perf_counter() - start_inner:.2f}s")

        self.logger.info(f"Built network stats in {time.perf_counter() - start_outer:.2f}s")

    def construct_keys(self, key, epoch):
        keys = []
        for period in range(0, epoch, self.aggregation_epochs):
            keys.append(f"{key}_{period}_{period + self.aggregation_epochs}")
        return keys

    def read_data_from_cache(self, key):
        # Start at the epoch marking the start of an aggregation epoch
        epoch = int(self.last_processed_epoch / self.aggregation_epochs) * self.aggregation_epochs

        # Get previously saved periods
        keys = self.construct_keys(key, epoch)
        cached_data = self.memcached_client.get_multi(keys)

        # Check if previous periods were deleted
        keys_not_found = set(keys) - set(cached_data.keys())
        if keys_not_found != set():
            # Start at the lower bound of the earliest period which was deleted
            min_epoch = min([int(key.split("_")[2]) for key in keys_not_found])
            epoch = min(epoch, min_epoch)

        return epoch, cached_data

    def get_rollbacks(self, reset):
        master_key = "network_rollbacks"

        # Temporary copy
        epoch = self.last_processed_epoch

        # Fetch previous rollbacks from the cache (unless reset was set)
        self.rollbacks = None
        if not reset:
            self.rollbacks = self.memcached_client.get(master_key)

        # Rollbacks is not present in the cache anymore, reset last epoch
        if not self.rollbacks:
            epoch = 0
            self.rollbacks = []

        self.logger.info(f"Creating {master_key} statistic from epoch {int(epoch / self.aggregation_epochs) * self.aggregation_epochs} to {self.last_confirmed_epoch}")

        # Fetch all confirmed blocks
        sql = """
            SELECT
                epoch
            FROM
                blocks
            WHERE
                confirmed = true
            AND
                epoch BETWEEN %s AND %s
            ORDER BY
                epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        epoch_data = self.witnet_database.sql_return_all(re_sql(sql))

        previous_epoch = epoch
        for epoch in epoch_data:
            epoch = epoch[0]

            # If there is a gap of more than 1 epoch between two consecutive blocks, we mark it as a rollback
            if epoch > previous_epoch + 1:
                # Calculate the timestamp of the rollback and its boundaries
                timestamp = self.start_time + (previous_epoch + 1) * self.epoch_period
                self.rollbacks.append((timestamp, previous_epoch + 1, epoch - 1, epoch - previous_epoch - 1))
            previous_epoch = epoch

            # Save the last seen epoch
            if epoch > self.last_processed_epoch_update:
                self.last_processed_epoch_update = epoch

        # List rollbacks in reverse order
        self.rollbacks = sorted(self.rollbacks, reverse=True)

    def get_miners_per_period(self, reset):
        master_key = "network_miners"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.unique_miners_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.unique_miners_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.unique_miners_period:
                self.unique_miners_period[key] = {}

        # Fetch all confirmed mint transactions (as a proxy for blocks)
        sql = """
            SELECT
                blocks.epoch,
                mint_txns.miner,
                addresses.id
            FROM
                mint_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch = mint_txns.epoch
            LEFT JOIN
                addresses
            ON
                addresses.address = mint_txns.miner
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        miners = self.witnet_database.sql_return_all(re_sql(sql))

        self.top_100_miners = []

        if miners == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        for epoch, miner, miner_id in miners:
            if miner_id == None:
                self.logger.warning(f"No id for address {miner}")
                miner_id = miner

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

            # Create histogram
            if miner_id in self.unique_miners_period[per_period_key]:
                self.unique_miners_period[per_period_key][miner_id] += 1
            else:
                self.unique_miners_period[per_period_key][miner_id] = 1

        self.num_unique_miners, self.top_100_miners = aggregate_nodes(self.unique_miners_period.values())

    def get_data_request_solvers_per_period(self, reset):
        master_key = "network_data-request-solvers"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.unique_data_request_solvers_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.unique_data_request_solvers_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.unique_data_request_solvers_period:
                self.unique_data_request_solvers_period[key] = {}

        # Fetch all confirmed commit transactions
        sql = """
            SELECT
                blocks.epoch,
                commit_txns.txn_address,
                addresses.id
            FROM
                commit_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch = commit_txns.epoch
            LEFT JOIN
                addresses
            ON
                addresses.address = commit_txns.txn_address
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        data_request_solvers = self.witnet_database.sql_return_all(re_sql(sql))

        self.top_100_data_request_solvers = []

        if data_request_solvers == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        for epoch, data_request_solver, data_request_solver_id in data_request_solvers:
            if data_request_solver_id == None:
                self.logger.warning(f"No id for address {data_request_solver}")
                data_request_solver_id = data_request_solver

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

            # Create histogram
            if data_request_solver_id in self.unique_data_request_solvers_period[per_period_key]:
                self.unique_data_request_solvers_period[per_period_key][data_request_solver_id] += 1
            else:
                self.unique_data_request_solvers_period[per_period_key][data_request_solver_id] = 1

        self.num_unique_data_request_solvers, self.top_100_data_request_solvers = aggregate_nodes(self.unique_data_request_solvers_period.values())

    def get_data_requests_per_period(self, reset):
        master_key = "network_data-requests"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.data_requests_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.data_requests_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.data_requests_period:
                self.data_requests_period[key] = [
                    0,  # Amount of requests
                    0,  # Success
                    0,  # HTTP-GET
                    0,  # HTTP-POST
                    0,  # RNG
                    {}, # Witness histogram
                    {}, # Witness reward histogram
                    {}, # Collateral histogram
                ]

        # Fetch all confirmed data requests and (part of) their metadata
        sql = """
            SELECT
                blocks.epoch,
                data_request_txns.witnesses,
                data_request_txns.witness_reward,
                data_request_txns.collateral,
                data_request_txns.kinds,
                tally_txns.success
            FROM
                data_request_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch = data_request_txns.epoch
            LEFT JOIN
                tally_txns
            ON
                data_request_txns.txn_hash = tally_txns.data_request_txn_hash
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        data_requests = self.witnet_database.sql_return_all(re_sql(sql))

        if data_requests == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        for epoch, witnesses, witness_reward, collateral, kinds, success in data_requests:
            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

            # Create data structure
            self.data_requests_period[per_period_key][0] += 1

            if success:
                self.data_requests_period[per_period_key][1] += 1

            kinds = kinds[1:-1].split(",")
            self.data_requests_period[per_period_key][2] += sum(1 for kind in kinds if kind == "HTTP-GET")
            self.data_requests_period[per_period_key][3] += sum(1 for kind in kinds if kind == "HTTP-POST")
            self.data_requests_period[per_period_key][4] += sum(1 for kind in kinds if kind == "RNG")

            if witnesses in self.data_requests_period[per_period_key][5]:
                self.data_requests_period[per_period_key][5][witnesses] += 1
            else:
                self.data_requests_period[per_period_key][5][witnesses] = 1

            if witness_reward in self.data_requests_period[per_period_key][6]:
                self.data_requests_period[per_period_key][6][witness_reward] += 1
            else:
                self.data_requests_period[per_period_key][6][witness_reward] = 1

            if collateral in self.data_requests_period[per_period_key][7]:
                self.data_requests_period[per_period_key][7][collateral] += 1
            else:
                self.data_requests_period[per_period_key][7][collateral] = 1

    def get_lie_rates_per_period(self, reset):
        master_key = "network_lie-rates"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.lie_rates_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.lie_rates_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.lie_rates_period:
                self.lie_rates_period[key] = [
                    0,  # Amount of requests
                    0,  # Amount of errors
                    0,  # Amount of lies (no reveals)
                    0,  # Amount of lies (out-of-consensus values)
                ]

        sql = """
            SELECT
                reveal_txns.data_request_txn_hash,
                COUNT(reveal_txns.data_request_txn_hash) as reveal_count
            FROM
                reveal_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch = reveal_txns.epoch
            WHERE
                blocks.epoch BETWEEN %s AND %s
            GROUP BY
                reveal_txns.data_request_txn_hash
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        reveal_data = self.witnet_database.sql_return_all(re_sql(sql))

        number_of_reveals = {}
        for txn_hash, reveals in reveal_data:
            number_of_reveals[txn_hash.hex()] = reveals

        # Fetch all confirmed data requests and (part of) their metadata
        sql = """
            SELECT
                blocks.epoch,
                data_request_txns.txn_hash,
                data_request_txns.witnesses,
                tally_txns.error_addresses,
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
                data_request_txns.txn_hash = tally_txns.data_request_txn_hash
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        lie_rate_data = self.witnet_database.sql_return_all(re_sql(sql))

        if lie_rate_data == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        for epoch, txn_hash, witnesses, error_addresses, liar_addresses in lie_rate_data:
            txn_hash = txn_hash.hex()

            if txn_hash not in number_of_reveals:
                reveals = 0
                self.logger.warning(f"Could not find data request reveals for {txn_hash} at epoch {epoch}")
            else:
                reveals = number_of_reveals[txn_hash]

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

            # Create data structure
            self.lie_rates_period[per_period_key][0] += witnesses
            if error_addresses:
                self.lie_rates_period[per_period_key][1] += len(error_addresses)
            if liar_addresses and len(liar_addresses) > 0:
                self.lie_rates_period[per_period_key][2] += witnesses - reveals
                num_liar_addresses = len(liar_addresses)
            else:
                num_liar_addresses = 0
            self.lie_rates_period[per_period_key][3] += max(0, num_liar_addresses - (witnesses - reveals))

    def get_burn_rate_per_period(self, reset):
        master_key = "network_burn-rate"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.burn_rate_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.burn_rate_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.burn_rate_period:
                self.burn_rate_period[key] = [
                    0,  # Burn rate reverted blocks
                    0,  # Burn rate data request lies
                ]

        # Fetch all blocks and data requests
        sql = """
            SELECT
                blocks.epoch,
                data_request_txns.txn_hash,
                data_request_txns.collateral,
                tally_txns.liar_addresses
            FROM
                blocks
            LEFT JOIN
                data_request_txns
            ON
                blocks.epoch = data_request_txns.epoch
            LEFT JOIN
                tally_txns
            ON
                data_request_txns.txn_hash = tally_txns.data_request_txn_hash
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        burn_rate_data = self.witnet_database.sql_return_all(re_sql(sql))

        if burn_rate_data == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        previous_epoch = epoch
        for epoch, txn_hash, collateral, liar_addresses in burn_rate_data:
            # First calculate burn because of the reverted blocks
            if epoch > previous_epoch + 1:
                for e in range(previous_epoch + 1, epoch):
                    # Check if the next aggregation period was reached
                    if e >= next_aggregation_period:
                        next_aggregation_period = int(e / self.aggregation_epochs + 1) * self.aggregation_epochs
                        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

                    block_reward = calculate_block_reward(epoch, self.consensus_constants)
                    self.burn_rate_period[per_period_key][0] += block_reward

            previous_epoch = epoch

            if not self.wips.is_wip0027_active(epoch):
                continue

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

            # Add liar burn rate
            if liar_addresses and len(liar_addresses) > 0:
                num_liar_addresses = len(liar_addresses)
            else:
                collateral = 0
                num_liar_addresses = 0
            self.burn_rate_period[per_period_key][1] += num_liar_addresses * collateral

    def get_trs_data_per_period(self, reset):
        master_key = "network_trs-data"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.trs_data_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.trs_data_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.trs_data_period:
                self.trs_data_period[key] = [
                    0,  # Amount of TRS nodes
                    0,  # Average reputation of TRS nodes
                    0,  # Median reputation of TRS nodes
                    0,  # Highest reputation of TRS nodes
                ]

        # Fetch all confirmed data requests and (part of) their metadata
        sql = """
            SELECT
                blocks.epoch,
                trs.reputations
            FROM trs
            LEFT JOIN
                blocks
            ON
                blocks.epoch = trs.epoch
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        trs_data = self.witnet_database.sql_return_all(re_sql(sql))

        if trs_data == None:
            return

        previous_epoch = epoch
        previous_reputation = self.memcached_client.get("network_previous-reputation")
        if previous_reputation == None:
            previous_reputation = []

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        epoch_counter = 0
        self.previous_reputation = None
        for epoch, reputation in trs_data:
            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                # Add values of the last epoch in an aggregation period
                if len(previous_reputation) > 0:
                    self.trs_data_period[per_period_key][0] += len(previous_reputation) * (next_aggregation_period - previous_epoch)
                    self.trs_data_period[per_period_key][1] += numpy.average(previous_reputation) * (next_aggregation_period - previous_epoch)
                    self.trs_data_period[per_period_key][2] += numpy.median(previous_reputation) * (next_aggregation_period - previous_epoch)

                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs

                # Divide summed statistics to get a time-weighted average
                self.trs_data_period[per_period_key][0] /= self.aggregation_epochs
                self.trs_data_period[per_period_key][1] /= self.aggregation_epochs
                self.trs_data_period[per_period_key][2] /= self.aggregation_epochs

                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

                self.previous_reputation = previous_reputation

                epoch_counter = 0

            # Create data structure

            # Time-weighted average statistics taking into account the amount of epochs they lasted
            aggregation_lower_bound = int(epoch / self.aggregation_epochs) * self.aggregation_epochs
            if len(previous_reputation) > 0:
                self.trs_data_period[per_period_key][0] += len(previous_reputation) * (epoch - max(aggregation_lower_bound, previous_epoch))
                self.trs_data_period[per_period_key][1] += numpy.average(previous_reputation) * (epoch - max(aggregation_lower_bound, previous_epoch))
                self.trs_data_period[per_period_key][2] += numpy.median(previous_reputation) * (epoch - max(aggregation_lower_bound, previous_epoch))

            # Track the maximum reputation
            if len(previous_reputation) > 0 and max(previous_reputation) > self.trs_data_period[per_period_key][3]:
                self.trs_data_period[per_period_key][3] = max(previous_reputation)

            previous_epoch = epoch
            previous_reputation = reputation

            epoch_counter += 1

        if not self.previous_reputation:
            self.previous_reputation = self.memcached_client.get("network_previous-reputation")

        self.trs_data_period[per_period_key][0] /= epoch_counter
        self.trs_data_period[per_period_key][1] /= epoch_counter
        self.trs_data_period[per_period_key][2] /= epoch_counter

    def get_value_transfers_per_period(self, reset):
        master_key = "network_value-transfers"

        # Read data from cache (unless reset was set)
        if not reset:
            epoch, self.value_transfers_period = self.read_data_from_cache(master_key)
        else:
            epoch, self.value_transfers_period = 0, {}

        self.logger.info(f"Creating {master_key} statistic from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(master_key, self.last_confirmed_epoch_ceiled):
            if key not in self.value_transfers_period:
                self.value_transfers_period[key] = 0

        # Fetch all confirmed data requests and (part of) their metadata
        sql = """
            SELECT
                blocks.epoch,
                value_transfer_txns.txn_hash
            FROM
                value_transfer_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch = value_transfer_txns.epoch
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.witnet_database.db_mngr.reset_cursor()
        value_transfers = self.witnet_database.sql_return_all(re_sql(sql))

        if value_transfers == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

        for epoch, value_transfer in value_transfers:
            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = f"{master_key}_{next_aggregation_period - self.aggregation_epochs}_{next_aggregation_period}"

            # Create data structure
            self.value_transfers_period[per_period_key] += 1

    def get_staking_stats(self):
        # Find active and reputed identities
        reputation_list = self.witnet_node.get_reputation_all()
        if "result" in reputation_list:
            reputation_list = reputation_list["result"]
        else:
            self.logger.warning("Could not fetch ARS from node")
            return

        ars_addresses = [address for address in reputation_list["stats"].keys()]
        reputed_addresses = [address for address in reputation_list["stats"].keys() if reputation_list["stats"][address]["reputation"] > 0]

        # Get their balance
        balance_list = self.witnet_node.get_balance_all()
        if "result" in balance_list:
            balance_list = balance_list["result"]
        else:
            self.logger.warning("Could not fetch all balances from node")
            return

        ars_balances, trs_balances = [], []
        for ars_address in ars_addresses:
            if ars_address in balance_list.keys():
                ars_balances.append(balance_list[ars_address]["total"])
            else:
                ars_balances.append(0)
            if ars_address in reputed_addresses:
                if ars_address in balance_list.keys():
                    trs_balances.append(balance_list[ars_address]["total"])
                else:
                    trs_balances.append(0)

        # Calculate the percentiles
        percentiles = list(range(1, 100, 1))
        self.percentile_staking_balances = {
            "percentiles": percentiles[::-1],
            "ars": [int(value) for value in numpy.percentile(ars_balances, percentiles)],
            "trs": [int(value) for value in numpy.percentile(trs_balances, percentiles)],
        }

    def save_network(self):
        start = time.perf_counter()

        self.logger.info("Saving all data in our memcached instance")

        # Can potentially exceed 1MB of storage, surround with a try-except
        try:
            self.memcached_client.set("network_list-rollbacks", self.rollbacks)
        except pylibmc.TooBig:
            self.logger.error("Could not save network_list-rollbacks in cache because the item size exceeded 1MB")

        try:
            self.memcached_client.set("network_previous-reputation", self.previous_reputation)
        except pylibmc.TooBig:
            self.logger.error("Could not save previous_reputation in cache because the item size exceeded 1MB")

        # These objects cannot exceed 1MB by design
        self.memcached_client.set("network_last-updated", self.last_update_time)

        self.last_processed_epoch = self.last_processed_epoch_update
        self.memcached_client.set("network_epoch", self.last_processed_epoch)

        self.memcached_client.set_multi(self.unique_miners_period)
        self.memcached_client.set("network_top-100-miners", self.top_100_miners)
        self.memcached_client.set("network_num-unique-miners", self.num_unique_miners)

        self.memcached_client.set_multi(self.unique_data_request_solvers_period)
        self.memcached_client.set("network_top-100-data-request-solvers", self.top_100_data_request_solvers)
        self.memcached_client.set("network_num-unique-data-request-solvers", self.num_unique_data_request_solvers)

        self.memcached_client.set_multi(self.data_requests_period)

        self.memcached_client.set_multi(self.lie_rates_period)

        self.memcached_client.set_multi(self.burn_rate_period)

        self.memcached_client.set_multi(self.trs_data_period)

        self.memcached_client.set_multi(self.value_transfers_period)

        self.memcached_client.set("network_percentile-staking-balances", self.percentile_staking_balances)

        self.logger.info(f"Saved all data in our memcached instance in {time.perf_counter() - start:.2f}s")

def aggregate_nodes(data):
    # Aggregate data of multiple periods
    aggregated_nodes = {}
    for nodes in data:
        for identity, amount in nodes.items():
            if identity not in aggregated_nodes:
                aggregated_nodes[identity] = amount
            else:
                aggregated_nodes[identity] += amount

    # Reverse sort and extract the top 100
    top_100_aggregated_nodes = sorted(aggregated_nodes.items(), key=lambda l: l[1], reverse=True)[:100]

    return len(aggregated_nodes), top_100_aggregated_nodes

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file", help="Specify a configuration file")
    parser.add_option("--reset", action="store_true", dest="reset", help="Start at epoch 0 and recalculate all statistics")
    options, args = parser.parse_args()

    if options.config_file == None:
        sys.stderr.write("Need to specify a configuration file!\n")
        sys.exit(1)

    # Load config file
    config = toml.load(options.config_file)

    # Create network cache
    network_cache = NetworkStats(config, options.reset)
    network_cache.build_network_stats(options.reset)
    network_cache.save_network()

if __name__ == "__main__":
    main()
