import logging
import logging.handlers
import math
import signal
import time

from multiprocessing import Process
from multiprocessing import Queue

from queue import Empty

from blockchain.witnet_database import WitnetDatabase

from node.witnet_node import WitnetNode

class RichList(object):
    def __init__(self, node_config, database_config, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "richlist")
        self.logger = logging.getLogger("richlist")

        # Rebuild richlist every hour
        self.refresh_timeout = 3600

        self.balances = []
        self.balances_sum = 0
        self.last_updated = int(time.time() / self.refresh_timeout) * self.refresh_timeout

        self.richlist_queue = Queue()
        self.process = Process(target=self.build_richlist, args=(node_config, database_config, self.richlist_queue, logging_queue))
        self.process.daemon = True
        self.process.start()

        self.logger.info(f"Started richlist process ({self.process.pid})")

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def build_richlist(self, node_config, database_config, richlist_queue, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "richlist-process")
        logger = logging.getLogger("richlist-process")

        # Connect to node pool
        socket_host = node_config["host"]
        socket_port = node_config["port"]
        witnet_node = WitnetNode(socket_host, socket_port, 300, logging_queue, "node-richlist")

        # Connect to database
        db_user = database_config["user"]
        db_name = database_config["name"]
        db_pass = database_config["password"]
        witnet_database = WitnetDatabase(db_user, db_name, db_pass, logging_queue, "db-richlist")

        while True:
            start = time.time()
            next_interval = math.ceil(start / self.refresh_timeout) * self.refresh_timeout

            sql = """
                SELECT
                    address,
                    label
                FROM addresses
            """
            address_labels = witnet_database.sql_return_all(sql)
            if address_labels:
                address_labels = {address: label for address,label in address_labels}

            address_balances = witnet_node.get_balance_all()

            balances, balances_sum = [], 0
            if type(address_balances) is dict and "error" in address_balances:
                logger.warning(f"Could not fetch all address balances: {address_balances}")
            else:
                address_balances = address_balances["result"]
                for address, balance in address_balances.items():
                    # Sum all balances
                    balances_sum += balance["total"] / 1E9
                    # Only save addresses with a balance above 1 WIT
                    if balance["total"] // 1E9 < 1:
                        continue
                    balances.append([address, round(balance["total"] // 1E9), address_labels[address] if address in address_labels else ""])
                balances = sorted(balances, key=lambda l: l[1], reverse=True)

                # Empty richlist
                while not richlist_queue.empty():
                    try:
                        # Fetch data in a non-blocking way so we can catch possible concurrency problems
                        richlist_queue.get(False)
                    except Queue.Empty:
                        break

                richlist_queue.put({
                    "balances": balances,
                    "balances_sum": balances_sum,
                    "last_updated": int(time.time() / self.refresh_timeout) * self.refresh_timeout
                })

            # Rebuild richlist every hour
            elapsed = math.ceil(time.time() - start)
            sleep_time = math.ceil(max(0, next_interval - time.time()))
            logger.info(f"Rebuilt richlist containing {len(balances)} addresses which took {elapsed} seconds, sleeping {sleep_time} seconds")
            time.sleep(sleep_time)

    def get_rich_list(self, start, stop):
        self.logger.info(f"get_rich_list({start}, {stop})")

        if not self.richlist_queue.empty():
            try:
                self.logger.info("Updating richlist balances")
                # Fetch data in a non-blocking way so we can catch possible concurrency problems
                data = self.richlist_queue.get(False)
                self.balances = data["balances"]
                self.balances_sum = data["balances_sum"]
                self.last_updated = data["last_updated"]
            except Empty:
                self.logger.warning("Failed to update richlist")

        return {
            "richlist": self.balances[start:stop],
            "balances_sum": self.balances_sum,
            "total_addresses": len(self.balances),
            "last_updated": self.last_updated
        }
