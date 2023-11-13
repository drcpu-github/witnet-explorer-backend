import json
import numpy
import optparse
import sys
import time
import toml

from caching.client import Client
from caching.network_stats_functions import aggregate_nodes, read_from_database
from blockchain.objects.wip import WIP

from util.data_transformer import re_sql
from util.common_functions import calculate_block_reward
from util.logger import configure_logger

class NetworkStats(Client):
    def __init__(self, config, reset):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["network_stats"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["network_stats"]["level_file"]
        self.logger = configure_logger("network", log_filename, log_level)

        timeout = config["api"]["caching"]["scripts"]["network_stats"]["node_timeout"]
        super().__init__(config, node_timeout=timeout, named_cursor=True)

        # Assign some of the consensus constants
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period
        self.halving_period = self.consensus_constants.halving_period
        self.initial_block_reward = self.consensus_constants.initial_block_reward

        # Granularity at which network statistics are aggregated
        self.aggregation_epochs = config["api"]["caching"]["scripts"]["network_stats"]["aggregation_epochs"]

        self.wips = WIP(database_config=config["database"], node_config=config["node-pool"])

        self.last_update_time = int(time.time())

        self.last_confirmed_epoch = self.get_last_confirmed_epoch()
        if self.last_confirmed_epoch == -1:
            self.logger.warning("Could not fetch last confirmed epoch")

    def get_last_confirmed_epoch(self):
        sql = """
            SELECT
                epoch
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
        self.database.reset_cursor()
        result = self.database.sql_return_one(re_sql(sql))

        if result:
            return result[0]
        else:
            return -1

    def build_network_stats(self, reset):
        start_outer = time.perf_counter()

        self.logger.info(f"Building network statistics")

        start_inner = time.perf_counter()
        self.logger.info("Calculating rollbacks")
        self.get_rollbacks(reset)
        self.logger.info(f"Calculated {len(self.rollbacks)} rollbacks in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating miner statistics")
        self.get_miners_per_period(reset)
        self.logger.info(f"Calculated {self.unique_miners['amount']} miners in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating data request solver statistics")
        self.get_data_request_solvers_per_period(reset)
        self.logger.info(f"Calculated {self.unique_data_request_solvers['amount']} data request solvers in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating data request statistics")
        self.get_data_requests_per_period(reset)
        self.logger.info(f"Calculated data request statistics for {len(self.data_requests_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating lie rate statistics")
        self.get_lie_rates_per_period(reset)
        self.logger.info(f"Calculated lie rate statistics for {len(self.lie_rates_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating burn rate statistics")
        self.get_burn_rate_per_period(reset)
        self.logger.info(f"Calculated burn rate statistics for {len(self.burn_rate_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating value transfer statistics")
        self.get_value_transfers_per_period(reset)
        self.logger.info(f"Calculated value transfer statistics for {len(self.value_transfers_period)} periods in {time.perf_counter() - start_inner:.2f}s")

        start_inner = time.perf_counter()
        self.logger.info("Calculating staking statistics")
        self.get_staking_stats()
        self.logger.info(f"Calculated staking statistics in {time.perf_counter() - start_inner:.2f}s")

        self.logger.info(f"Built network stats in {time.perf_counter() - start_outer:.2f}s")

    def construct_keys(self, from_epoch, to_epoch):
        keys = []
        from_epoch = int(from_epoch / self.aggregation_epochs) * self.aggregation_epochs
        to_epoch = int(to_epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        for period in range(from_epoch, to_epoch, self.aggregation_epochs):
            keys.append((period, period + self.aggregation_epochs))
        return keys

    def get_rollbacks(self, reset):
        # Fetch previous rollbacks (unless reset was set)
        self.rollbacks = []
        self.last_processed_epoch = 0
        if not reset:
            self.last_processed_epoch, rollbacks = read_from_database("rollbacks", self.aggregation_epochs, self.database_client, all_periods=True)
            if rollbacks:
                self.rollbacks = rollbacks[0][2]

        self.logger.info(f"Calculating rollbacks statistics from epoch {self.last_processed_epoch} to {self.last_confirmed_epoch}")

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
        """ % (self.last_processed_epoch, self.last_confirmed_epoch)
        self.database.reset_cursor()
        epoch_data = self.database.sql_return_all(re_sql(sql))

        previous_epoch = self.last_processed_epoch
        for epoch in epoch_data:
            epoch = epoch[0]

            # If there is a gap of more than 1 epoch between two consecutive blocks, we mark it as a rollback
            if epoch > previous_epoch + 1:
                # Calculate the timestamp of the rollback and its boundaries
                timestamp = self.start_time + (previous_epoch + 1) * self.epoch_period
                self.rollbacks.append([timestamp, previous_epoch + 1, epoch - 1, epoch - previous_epoch - 1])
            previous_epoch = epoch

        # Save the last seen epoch
        if epoch_data and epoch > self.last_processed_epoch:
            self.last_processed_epoch = epoch

        # List rollbacks in reverse order
        self.rollbacks = sorted(self.rollbacks, reverse=True)

    def get_miners_per_period(self, reset):
        # Read data from database (unless reset was set)
        epoch = 0
        self.unique_miners = {"amount": 0, "top-100": {}, "per-period": {}}
        if not reset:
            epoch, miner_data = read_from_database("miners", self.aggregation_epochs, self.database_client, all_periods=True)
            for md in miner_data:
                if md[0] != None and md[1] != None:
                    self.unique_miners["per-period"][(md[0], md[1])] = md[2]
                else:
                    self.unique_miners["amount"] = md[2]["amount"]
                    self.unique_miners["top-100"] = md[2]["top-100"]

        self.logger.info(f"Calculating miners statistics from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(0, self.last_confirmed_epoch):
            if key not in self.unique_miners["per-period"]:
                self.unique_miners["per-period"][key] = {}

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
        self.database.reset_cursor()
        miners = self.database.sql_return_all(re_sql(sql))

        if miners == None:
            return

        updated_keys = []

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)
        updated_keys.append(per_period_key)

        for epoch, miner, miner_id in miners:
            if miner_id == None:
                self.logger.warning(f"No id for address {miner}")
                miner_id = miner

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)
                updated_keys.append(per_period_key)

            # Create histogram
            if miner_id in self.unique_miners["per-period"][per_period_key]:
                self.unique_miners["per-period"][per_period_key][miner_id] += 1
            else:
                self.unique_miners["per-period"][per_period_key][miner_id] = 1

        self.unique_miners["amount"], self.unique_miners["top-100"] = aggregate_nodes(self.unique_miners["per-period"].values())

        self.unique_miners["per-period"] = {per_period_key: self.unique_miners["per-period"][per_period_key] for per_period_key in self.unique_miners["per-period"].keys() if per_period_key in updated_keys}

    def get_data_request_solvers_per_period(self, reset):
        # Read data from database (unless reset was set)
        epoch = 0
        self.unique_data_request_solvers = {"amount": 0, "top-100": {}, "per-period": {}}
        if not reset:
            epoch, solver_data = read_from_database("data_request_solvers", self.aggregation_epochs, self.database_client, all_periods=True)
            for sd in solver_data:
                if sd[0] != None and sd[1] != None:
                    self.unique_data_request_solvers["per-period"][(sd[0], sd[1])] = sd[2]
                else:
                    self.unique_data_request_solvers["amount"] = sd[2]["amount"]
                    self.unique_data_request_solvers["top-100"] = sd[2]["top-100"]

        self.logger.info(f"Calculating data request solver statistics from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(0, self.last_confirmed_epoch):
            if key not in self.unique_data_request_solvers["per-period"]:
                self.unique_data_request_solvers["per-period"][key] = {}

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
        self.database.reset_cursor()
        data_request_solvers = self.database.sql_return_all(re_sql(sql))

        if data_request_solvers == None:
            return

        updated_keys = []

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)
        updated_keys.append(per_period_key)

        for epoch, data_request_solver, data_request_solver_id in data_request_solvers:
            if data_request_solver_id == None:
                self.logger.warning(f"No id for address {data_request_solver}")
                data_request_solver_id = data_request_solver

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)
                updated_keys.append(per_period_key)

            # Create histogram
            if data_request_solver_id in self.unique_data_request_solvers["per-period"][per_period_key]:
                self.unique_data_request_solvers["per-period"][per_period_key][data_request_solver_id] += 1
            else:
                self.unique_data_request_solvers["per-period"][per_period_key][data_request_solver_id] = 1

        self.unique_data_request_solvers["amount"], self.unique_data_request_solvers["top-100"] = aggregate_nodes(self.unique_data_request_solvers["per-period"].values())

        self.unique_data_request_solvers["per-period"] = {per_period_key: self.unique_data_request_solvers["per-period"][per_period_key] for per_period_key in self.unique_data_request_solvers["per-period"].keys() if per_period_key in updated_keys}

    def get_data_requests_per_period(self, reset):
        # Read data from database (unless reset was set)
        epoch, self.data_requests_period = 0, {}
        if not reset:
            epoch, data_requests_period = read_from_database("data_requests", self.aggregation_epochs, self.database_client)
            self.data_requests_period = {(drp[0], drp[1]): drp[2] for drp in data_requests_period}

        self.logger.info(f"Calculating data request statistics from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(epoch, self.last_confirmed_epoch):
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
                data_request_txns.txn_hash = tally_txns.data_request
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.database.reset_cursor()
        data_requests = self.database.sql_return_all(re_sql(sql))

        if data_requests == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

        for epoch, witnesses, witness_reward, collateral, kinds, success in data_requests:
            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

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
        # Read data from database (unless reset was set)
        epoch, self.lie_rates_period = 0, {}
        if not reset:
            epoch, lie_rates_period = read_from_database("data_requests", self.aggregation_epochs, self.database_client)
            self.lie_rates_period = {(lrp[0], lrp[1]): lrp[2] for lrp in lie_rates_period}

        self.logger.info(f"Calculating lie rate statistics from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(epoch, self.last_confirmed_epoch):
            if key not in self.lie_rates_period:
                self.lie_rates_period[key] = [
                    0,  # Amount of witnessing acts
                    0,  # Amount of errors
                    0,  # Amount of lies (no reveals)
                    0,  # Amount of lies (out-of-consensus values)
                ]

        # Fetch reveal counts per data request
        sql = """
            SELECT
                reveal_txns.data_request,
                COUNT(reveal_txns.data_request) as reveal_count
            FROM
                reveal_txns
            LEFT JOIN
                blocks
            ON
                blocks.epoch = reveal_txns.epoch
            WHERE
                blocks.epoch BETWEEN %s AND %s
            GROUP BY
                reveal_txns.data_request
        """ % (epoch, self.last_confirmed_epoch)
        self.database.reset_cursor()
        reveal_data = self.database.sql_return_all(re_sql(sql))

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
                data_request_txns.txn_hash = tally_txns.data_request
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.database.reset_cursor()
        lie_rate_data = self.database.sql_return_all(re_sql(sql))

        if lie_rate_data == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

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
                per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

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
        # Read data from database (unless reset was set)
        epoch, self.burn_rate_period = 0, {}
        if not reset:
            epoch, burn_rate_period = read_from_database("data_requests", self.aggregation_epochs, self.database_client)
            self.burn_rate_period = {(brp[0], brp[1]): brp[2] for brp in burn_rate_period}

        self.logger.info(f"Calculating burn rate statistics from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(epoch, self.last_confirmed_epoch):
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
                data_request_txns.txn_hash = tally_txns.data_request
            WHERE
                blocks.confirmed = true
            AND
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
            ASC
        """ % (epoch, self.last_confirmed_epoch)
        self.database.reset_cursor()
        burn_rate_data = self.database.sql_return_all(re_sql(sql))

        if burn_rate_data == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

        previous_epoch = epoch
        for epoch, txn_hash, collateral, liar_addresses in burn_rate_data:
            # First calculate burn because of the reverted blocks
            if epoch > previous_epoch + 1:
                for e in range(previous_epoch + 1, epoch):
                    # Check if the next aggregation period was reached
                    if e >= next_aggregation_period:
                        next_aggregation_period = int(e / self.aggregation_epochs + 1) * self.aggregation_epochs
                        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

                    block_reward = calculate_block_reward(epoch, self.halving_period, self.initial_block_reward)
                    self.burn_rate_period[per_period_key][0] += block_reward

            previous_epoch = epoch

            if not self.wips.is_wip0027_active(epoch):
                continue

            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

            # Add liar burn rate
            if liar_addresses and len(liar_addresses) > 0:
                num_liar_addresses = len(liar_addresses)
            else:
                collateral = 0
                num_liar_addresses = 0
            self.burn_rate_period[per_period_key][1] += num_liar_addresses * collateral

    def get_value_transfers_per_period(self, reset):
        # Read data from database (unless reset was set)
        epoch, self.value_transfers_period = 0, {}
        if not reset:
            epoch, value_transfers_period = read_from_database("value_transfers", self.aggregation_epochs, self.database_client)
            self.value_transfers_period = {(vtp[0], vtp[1]): vtp[2] for vtp in value_transfers_period}

        self.logger.info(f"Calculating value transfer statistics from epoch {epoch} to {self.last_confirmed_epoch}")

        # Add all keys upfront to make sure periods without data are initialized as empty
        for key in self.construct_keys(epoch, self.last_confirmed_epoch):
            if key not in self.value_transfers_period:
                self.value_transfers_period[key] = [0]

        # Fetch value transfer data
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
        self.database.reset_cursor()
        value_transfers = self.database.sql_return_all(re_sql(sql))

        if value_transfers == None:
            return

        next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
        per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

        for epoch, value_transfer in value_transfers:
            # Check if the next aggregation period was reached
            if epoch >= next_aggregation_period:
                next_aggregation_period = int(epoch / self.aggregation_epochs + 1) * self.aggregation_epochs
                per_period_key = (next_aggregation_period - self.aggregation_epochs, next_aggregation_period)

            # Create data structure
            self.value_transfers_period[per_period_key][0] += 1

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

        self.logger.info("Saving all data in our database instance")

        sql = """
            INSERT INTO network_stats(
                stat,
                from_epoch,
                to_epoch,
                data
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT
                network_stats_stat_from_epoch_to_epoch_key
            DO UPDATE SET
                data = EXCLUDED.data
        """

        # Save single statistics since network inception
        stats = [
            ["epoch", self.last_processed_epoch],
            ["rollbacks", self.rollbacks],
            [
                "miners",
                {"amount": self.unique_miners["amount"], "top-100": self.unique_miners["top-100"]}
            ],
            [
                "data_request_solvers",
                {"amount": self.unique_data_request_solvers["amount"], "top-100": self.unique_data_request_solvers["top-100"]}
            ],
            ["staking", self.percentile_staking_balances],
        ]
        for lbl, stat in stats:
            self.database_client.sql_insert_one(
                re_sql(sql),
                (lbl, None, None, json.dumps(stat))
            )

        sql = """
            INSERT INTO network_stats(
                stat,
                from_epoch,
                to_epoch,
                data
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT
                network_stats_stat_from_epoch_to_epoch_key
            DO UPDATE SET
                data = EXCLUDED.data
        """

        # Save per-period statistics
        per_period_stats = [
            ["miners", self.unique_miners["per-period"]],
            ["data_request_solvers", self.unique_data_request_solvers["per-period"]],
            ["data_requests", self.data_requests_period],
            ["lie_rate", self.lie_rates_period],
            ["burn_rate", self.burn_rate_period],
            ["value_transfers", self.value_transfers_period],
        ]
        for sn, pps in per_period_stats:
            lst = [(sn, from_epoch, to_epoch, json.dumps(value)) for (from_epoch, to_epoch), value in pps.items()]
            self.database_client.sql_execute_many(
                re_sql(sql),
                lst
            )

        self.logger.info(f"Saved all data in our database instance in {time.perf_counter() - start:.2f}s")

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
