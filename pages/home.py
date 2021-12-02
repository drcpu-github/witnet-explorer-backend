import logging
import logging.handlers
import math
import time

from multiprocessing import Process
from multiprocessing import Queue

from queue import Empty

from blockchain.witnet_database import WitnetDatabase

from node.witnet_node import WitnetNode

class Home(object):
    def __init__(self, database_config, node_config, consensus_constants, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "home")
        self.logger = logging.getLogger("home")

        # Rebuild home statistics every 30 seconds
        self.refresh_timeout = 30

        # Set default start values
        self.network_stats = {
            "epochs": 0,
            "num_blocks": 0,
            "num_data_requests": 0,
            "num_value_transfers": 0,
            "num_active_nodes": 0,
            "num_reputed_nodes": 0,
            "num_pending_requests": 0,
        }
        self.supply_info = {
            "blocks_minted": 0,
            "blocks_minted_reward": 0,
            "blocks_missing": 0,
            "blocks_missing_reward": 0,
            "current_locked_supply": 0,
            "current_time": 0,
            "current_unlocked_supply": 0,
            "epoch": 0,
            "in_flight_requests": 0,
            "locked_wits_by_requests": 0,
            "maximum_supply": 0,
        }
        self.previous_supply_info = self.supply_info
        self.latest_blocks = []
        self.latest_data_requests = []
        self.latest_value_transfers = []
        self.last_updated = int(time.time() / self.refresh_timeout) * self.refresh_timeout

        self.home_queue = Queue()
        self.process = Process(target=self.collect_home_stats, args=(database_config, node_config, consensus_constants, self.home_queue, logging_queue))
        self.process.daemon = True
        self.process.start()

        self.logger.info(f"Started home process ({self.process.pid})")

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def collect_home_stats(self, database_config, node_config, consensus_constants, home_queue, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "home-process")
        logger = logging.getLogger("home-process")

        # Connect to the database
        db_user = database_config["user"]
        db_name = database_config["name"]
        db_pass = database_config["password"]
        self.witnet_database = WitnetDatabase(db_user, db_name, db_pass, logging_queue, "db-home")

        # Connect to the node pool
        socket_host = node_config["host"]
        socket_port = node_config["port"]
        self.witnet_node = WitnetNode(socket_host, socket_port, 15, logging_queue, "node-home")

        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        self.previous_num_active_nodes = 0
        self.previous_num_reputed_nodes = 0

        while True:
            start = time.time()

            home_stats = {}
            home_stats["network_stats"] = self.get_network_stats()
            home_stats["supply_info"] = self.get_supply_info()
            home_stats["latest_blocks"] = self.get_latest_blocks()
            home_stats["latest_data_requests"] = self.get_latest_data_requests()
            home_stats["latest_value_transfers"] = self.get_latest_value_transfers()
            home_stats["last_updated"] = int(time.time() / self.refresh_timeout) * self.refresh_timeout

            # Empty queue
            while not home_queue.empty():
                try:
                    # Fetch data in a non-blocking way so we can catch possible concurrency problems
                    home_queue.get(False)
                except Queue.Empty:
                    break

            home_queue.put(home_stats)

            # Rebuild home statistics every 30 seconds
            elapsed = math.ceil(time.time() - start)
            if elapsed < self.refresh_timeout:
                sleep_time = self.refresh_timeout - elapsed
            else:
                sleep_time = self.refresh_timeout - (elapsed % self.refresh_timeout)
            logger.info(f"Collected home statistics which took {elapsed} seconds, sleeping {sleep_time} seconds")
            time.sleep(sleep_time)

        return home_stats

    def get_network_stats(self):
        epochs = int((time.time() - self.start_time) / self.epoch_period)

        sql = """
            SELECT
                COUNT(1)
            FROM blocks
            WHERE
                confirmed=true
        """
        num_blocks = self.witnet_database.sql_return_one(sql)
        if num_blocks:
            num_blocks = num_blocks[0]
        else:
            num_blocks = 0

        sql = """
            SELECT
                SUM(data_request)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        num_data_requests = self.witnet_database.sql_return_one(sql)
        if num_data_requests:
            num_data_requests = num_data_requests[0]
        else:
            num_data_requests = 0

        sql = """
            SELECT
                SUM(value_transfer)
            FROM blocks
            WHERE
                blocks.confirmed=true
        """
        num_value_transfers = self.witnet_database.sql_return_one(sql)
        if num_value_transfers:
            num_value_transfers = num_value_transfers[0]
        else:
            num_value_transfers = 0

        active_nodes = self.witnet_node.get_reputation_all()
        if type(active_nodes) is dict and "error" in active_nodes:
            num_active_nodes = self.previous_num_active_nodes
            num_reputed_nodes = self.previous_num_reputed_nodes
        else:
            active_nodes = active_nodes["result"]
            num_active_nodes = sum([1 for key in active_nodes["stats"].keys() if active_nodes["stats"][key]["is_active"]])
            num_reputed_nodes = sum([1 for key in active_nodes["stats"].keys() if active_nodes["stats"][key]["reputation"] > 0])
            self.previous_num_active_nodes = num_active_nodes
            self.previous_num_reputed_nodes = num_reputed_nodes

        pending_requests = self.witnet_node.get_mempool()
        if type(pending_requests) is dict and "error" in pending_requests:
            num_pending_requests = 0
        else:
            pending_requests = pending_requests["result"]
            num_pending_requests = len(pending_requests["data_request"]) + len(pending_requests["value_transfer"])

        return {
            "epochs": epochs,
            "num_blocks": num_blocks,
            "num_data_requests": num_data_requests,
            "num_value_transfers": num_value_transfers,
            "num_active_nodes": num_active_nodes,
            "num_reputed_nodes": num_reputed_nodes,
            "num_pending_requests": num_pending_requests
        }

    def get_supply_info(self):
        supply_info = self.witnet_node.get_supply_info()
        if type(supply_info) is dict and "error" in supply_info:
            return self.previous_supply_info
        else:
            supply_info = supply_info["result"]
            self.previous_supply_info = supply_info
            return supply_info

    def get_latest_blocks(self):
        sql = """
            SELECT
                block_hash,
                data_request,
                value_transfer,
                epoch,
                confirmed
            FROM blocks
            ORDER BY epoch
            DESC LIMIT 32
        """
        result = self.witnet_database.sql_return_all(sql)

        blocks = []
        for block_hash, data_request, value_transfer, epoch, confirmed in result:
            timestamp = self.start_time + (epoch + 1) * self.epoch_period
            blocks.append([block_hash.hex(), data_request, value_transfer, timestamp, confirmed])

        return blocks

    def get_latest_data_requests(self):
        sql = """
            SELECT
                data_request_txns.txn_hash,
                data_request_txns.epoch,
                blocks.confirmed
            FROM data_request_txns
            LEFT JOIN blocks ON
                data_request_txns.epoch=blocks.epoch
            ORDER BY epoch
            DESC LIMIT 32
        """
        result = self.witnet_database.sql_return_all(sql)

        data_requests = []
        if result:
            for txn_hash, epoch, block_confirmed in result:
                timestamp = self.start_time + (epoch + 1) * self.epoch_period
                data_requests.append((txn_hash.hex(), timestamp, block_confirmed))

        return data_requests

    def get_latest_value_transfers(self):
        sql = """
            SELECT
                value_transfer_txns.txn_hash,
                value_transfer_txns.epoch,
                blocks.confirmed
            FROM value_transfer_txns
            LEFT JOIN blocks ON
                value_transfer_txns.epoch=blocks.epoch
            ORDER BY epoch
            DESC LIMIT 32
        """
        result = self.witnet_database.sql_return_all(sql)

        value_transfers = []
        if result:
            for txn_hash, epoch, block_confirmed in result:
                timestamp = self.start_time + (epoch + 1) * self.epoch_period
                value_transfers.append((txn_hash.hex(), timestamp, block_confirmed))

        return value_transfers

    def get_home(self, key):
        self.logger.info(f"get_home({key})")

        if not self.home_queue.empty():
            try:
                self.logger.info("Updating home statistics")
                # Fetch data in a non-blocking way so we can catch possible concurrency problems
                data = self.home_queue.get(False)
                # Update data
                self.network_stats = data["network_stats"]
                self.supply_info = data["supply_info"]
                self.latest_blocks = data["latest_blocks"]
                self.latest_data_requests = data["latest_data_requests"]
                self.latest_value_transfers = data["latest_value_transfers"]
                self.last_updated = data["last_updated"]
            except Empty:
                self.logger.warning("Failed to update home statistics")

        return {
            "network_stats": self.network_stats,
            "supply_info": self.supply_info,
            "latest_blocks": self.latest_blocks,
            "latest_data_requests": self.latest_data_requests,
            "latest_value_transfers": self.latest_value_transfers,
            "last_updated": self.last_updated,
        }
