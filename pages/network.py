import logging
import logging.handlers
import math
import signal
import time

from multiprocessing import Process
from multiprocessing import Queue

from queue import Empty

from blockchain.witnet_database import WitnetDatabase

from util.address_generator import AddressGenerator

class Network(object):
    def __init__(self, database_config, consensus_constants, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "network")
        self.logger = logging.getLogger("network")

        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        # Rollbacks
        self.rollbacks = []
        # Blocks minted and data requests solved by unique nodes and
        self.unique_miners, self.unique_dr_solvers = 0, 0
        # Top 10 of block miners and data request solvers
        self.top_100_miners, self.top_100_dr_solvers = [], []

        # Rebuild network statistics every hour
        self.refresh_timeout = 3600
        self.last_updated = int(time.time() / self.refresh_timeout) * self.refresh_timeout

        self.network_queue = Queue()
        self.process = Process(target=self.collect_network_stats, args=(database_config, self.network_queue, logging_queue))
        self.process.daemon = True
        self.process.start()

        self.logger.info(f"Started network process ({self.process.pid})")

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def collect_network_stats(self, database_config, network_queue, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "network-process")
        logger = logging.getLogger("network-process")

        # Create database
        db_user = database_config["user"]
        db_name = database_config["name"]
        db_pass = database_config["password"]
        witnet_database = WitnetDatabase(db_user, db_name, db_pass, logging_queue, "db-network")

        address_generator = AddressGenerator("wit")

        rollbacks = []
        mint_start_epoch, commit_start_epoch = 0, 0
        unique_miners, unique_dr_solvers = {}, {}

        batch_size = 100000
        while True:
            start = time.time()
            next_interval = math.ceil(start / self.refresh_timeout) * self.refresh_timeout

            # Query all blocks batch-wise to keep memory usage under control
            while True:
                logger.info(f"Collecting miners and rollbacks for epochs {mint_start_epoch} to {mint_start_epoch + batch_size}")

                sql = """
                    SELECT
                        mint_txns.miner,
                        mint_txns.epoch
                    FROM mint_txns
                    LEFT JOIN blocks ON
                        blocks.epoch=mint_txns.epoch
                    WHERE
                        blocks.confirmed=true
                        AND
                        mint_txns.epoch BETWEEN %s AND %s
                    ORDER BY mint_txns.epoch ASC
                """ % (mint_start_epoch, mint_start_epoch + batch_size)
                miner_data = witnet_database.sql_return_all(sql)

                previous_epoch = mint_start_epoch
                if miner_data:
                    for miner, epoch in miner_data:
                        # generate address and check for uniqueness
                        if miner in unique_miners:
                            unique_miners[miner] += 1
                        else:
                            unique_miners[miner] = 1

                        # check for rollbacks
                        if epoch > previous_epoch + 1:
                            timestamp = self.start_time + (previous_epoch + 1) * self.epoch_period
                            rollbacks.append((timestamp, previous_epoch + 1, epoch - 1, epoch - previous_epoch - 1))

                        # update mint_start_epoch
                        if epoch > mint_start_epoch:
                            mint_start_epoch = epoch
                        previous_epoch = epoch
                    mint_start_epoch += 1
                else:
                    break

            logger.info(f"Found {len(unique_miners)} unique miners")
            top_100_miners = sorted(unique_miners.items(), key=lambda l: l[1], reverse=True)[:100]

            # Query all commit transactions batch-wise to keep memory usage under control
            while True:
                logger.info(f"Collecting data request solvers for epochs {commit_start_epoch} to {commit_start_epoch + batch_size}")

                sql = """
                    SELECT
                        commit_txns.txn_address,
                        commit_txns.epoch
                    FROM commit_txns
                    LEFT JOIN blocks ON
                        blocks.epoch=commit_txns.epoch
                    WHERE
                        blocks.confirmed=true AND
                        commit_txns.epoch BETWEEN %s AND %s
                    ORDER BY commit_txns.epoch ASC
                """ % (commit_start_epoch, commit_start_epoch + batch_size)
                commit_data = witnet_database.sql_return_all(sql)

                if commit_data:
                    for txn_address, epoch in commit_data:
                        # generate address and check for uniqueness
                        if txn_address in unique_dr_solvers:
                            unique_dr_solvers[txn_address] += 1
                        else:
                            unique_dr_solvers[txn_address] = 1

                        # update start_epoch
                        if epoch > commit_start_epoch:
                            commit_start_epoch = epoch
                    commit_start_epoch += 1
                else:
                    break

            logger.info(f"Found {len(unique_dr_solvers)} unique data request solvers")
            top_100_dr_solvers = sorted(unique_dr_solvers.items(), key=lambda l: l[1], reverse=True)[:100]

            # Empty queue so only the most recent value dictionary is left
            while not network_queue.empty():
                try:
                    # Fetch data in a non-blocking way so we can catch possible concurrency problems
                    network_queue.get(False)
                except Empty:
                    break

            network_queue.put({
                "rollbacks": rollbacks[-100:][::-1],
                "unique_miners": len(unique_miners),
                "unique_dr_solvers": len(unique_dr_solvers),
                "top_100_miners": top_100_miners,
                "top_100_dr_solvers": top_100_dr_solvers,
                "last_updated": int(time.time() / self.refresh_timeout) * self.refresh_timeout,
            })

            # Rebuild network statistics every hour
            elapsed = math.ceil(time.time() - start)
            sleep_time = math.ceil(max(0, next_interval - time.time()))
            logger.info(f"Collected network statistics which took {elapsed} seconds, sleeping {sleep_time} seconds")
            time.sleep(sleep_time)

    def get_network_stats(self):
        self.logger.info("get_network_stats()")

        if not self.network_queue.empty():
            try:
                self.logger.info("Updating network statistics")
                # Fetch data in a non-blocking way so we can catch possible concurrency problems
                data = self.network_queue.get(False)
                self.rollbacks = data["rollbacks"]
                self.unique_miners, self.unique_dr_solvers = data["unique_miners"], data["unique_dr_solvers"]
                self.top_100_miners, self.top_100_dr_solvers = data["top_100_miners"], data["top_100_dr_solvers"]
                self.last_updated = data["last_updated"]
            except Empty:
                self.logger.info("Failed to update network statistics")

        return {
            "rollbacks": self.rollbacks,
            "unique_miners": self.unique_miners,
            "unique_dr_solvers": self.unique_dr_solvers,
            "top_100_miners": self.top_100_miners,
            "top_100_dr_solvers": self.top_100_dr_solvers,
            "last_updated": self.last_updated
        }