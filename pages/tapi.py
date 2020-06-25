import itertools
import logging
import logging.handlers
import math
import time

from multiprocessing import Process
from multiprocessing import Queue

from queue import Empty

from blockchain.witnet_database import WitnetDatabase

from node.witnet_node import WitnetNode

class Tapi(object):
    def __init__(self, database_config, consensus_constants, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "tapi")
        self.logger = logging.getLogger("tapi")

        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        # Rebuild TAPI data every 5 minutes
        self.refresh_timeout = 300

        # Set default start values
        self.tapi_data = {}

        self.tapi_queue = Queue()
        self.process = Process(target=self.collect_tapi_data, args=(database_config, self.tapi_queue, logging_queue))
        self.process.daemon = True
        self.process.start()

        self.logger.info(f"Started TAPI process ({self.process.pid})")

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def collect_acceptance_data(self, witnet_database, start_epoch, stop_epoch, blocks):
        _, last_epoch = witnet_database.get_last_block(confirmed=False)

        acceptance_data = []

        # TAPI has not started yet
        if last_epoch < start_epoch:
            return last_epoch, acceptance_data

        # If the first blocks since the TAPI start epoch were rolled back, append 0 to indicate reject
        acceptance_data.extend([0] * max(0, blocks[0][0] - start_epoch))

        # Loop over the available blocks and process those
        previous_epoch = start_epoch
        for block in blocks:
            epoch, tapi_accept, confirmed, reverted = block

            # If the previous block was more than 1 epoch ago, extrapolate TAPI acceptance to 0 (reject) for rollbacks
            if epoch > previous_epoch + 1:
                acceptance_data.extend([0 for i in range(epoch - previous_epoch - 1)])

            # If the block was confirmed, check TAPI acceptance
            if confirmed and not reverted:
                acceptance_data.append(1 if tapi_accept else 0)

            # If the block was reverted, hardcode TAPI as not accepted
            if reverted:
                acceptance_data.append(0)

            previous_epoch = epoch

        # If the last blocks before the TAPI stop epoch were rolled back, append 0 indicating reject
        acceptance_data.extend([0] * (min(last_epoch, stop_epoch - 1) - blocks[-1][0]))

        return previous_epoch, acceptance_data

    def create_summary(self, tapi_period_length, acceptance):
        # Periodic acceptance rates per 1000 epochs
        rates = []
        epochs = 1000
        for i in range(0, len(acceptance), epochs):
            rates.append(
                {
                    "periodic_rate": acceptance[i : i + epochs].count(1) / len(acceptance[i : i + epochs]) * 100,
                    "relative_rate": acceptance[0 : i + epochs].count(1) / len(acceptance[0 : i + epochs]) * 100,
                    "global_rate": acceptance[0 : i + epochs].count(1) / tapi_period_length * 100,
                }
            )

        # Overall acceptance rates
        if len(acceptance) > 0:
            relative_acceptance_rate = acceptance.count(1) / len(acceptance) * 100
        else:
            relative_acceptance_rate = 0
        global_acceptance_rate = acceptance.count(1) / tapi_period_length * 100

        return rates, relative_acceptance_rate, global_acceptance_rate

    def convert_binary_acceptance_to_ints(self, acceptance_data):
        accept_int = []
        # Transform all binary data to 32-bit ints except the last (incomplete) number
        for i in range(0, len(acceptance_data) - 32, 32):
            accept_int.append(int("".join(str(n) for n in acceptance_data[i : i + 32]), 2))
        # Keep the last number in binary
        if len(acceptance_data) % 32 == 0:
            accept_int.append("".join(str(n) for n in acceptance_data[-32:]))
        else:
            accept_int.append("".join(str(n) for n in acceptance_data[-(len(acceptance_data) % 32):]))
        return accept_int

    def collect_tapi_data(self, database_config, tapi_queue, logging_queue):
        # Set up logger
        self.configure_logging_process(logging_queue, "tapi-process")
        logger = logging.getLogger("tapi-process")

        # Connect to the database
        db_user = database_config["user"]
        db_name = database_config["name"]
        db_pass = database_config["password"]
        witnet_database = WitnetDatabase(db_user, db_name, db_pass, logging_queue, "db-tapi")

        # Periodically update TAPI data
        tapi_data = {}
        tapi_data_updated = False
        last_tapi_id, last_tapi_start_epoch, last_tapi_stop_epoch = 0, 0, 0
        while True:
            start = time.time()

            # Update current TAPI starting at the last epoch processed
            _, last_epoch = witnet_database.get_last_block(confirmed=False)

            # Setup of all TAPI data, query this periodically to find newly added TAPI periods
            sql = """
                SELECT
                    id,
                    title,
                    description,
                    start_epoch,
                    stop_epoch,
                    bit,
                    urls
                FROM tapi
                ORDER BY id ASC
            """
            tapis = witnet_database.sql_return_all(sql)
            for tapi in tapis:
                # Save TAPI metadata
                tapi_id, title, description, start_epoch, stop_epoch, bit, urls = tapi

                if tapi_id in tapi_data:
                    continue

                logger.info(f"Collecting initial TAPI data for TAPI {tapi_id}, running from epoch {start_epoch} to epoch {stop_epoch - 1}")

                tapi_data_updated = True

                tapi_data[tapi_id] = {}
                tapi_data[tapi_id]["tapi_id"] = tapi_id
                tapi_data[tapi_id]["title"] = title
                tapi_data[tapi_id]["description"] = description
                tapi_data[tapi_id]["start_epoch"] = start_epoch
                tapi_data[tapi_id]["start_time"] = self.start_time + (start_epoch + 1) * self.epoch_period
                tapi_data[tapi_id]["stop_epoch"] = stop_epoch
                tapi_data[tapi_id]["stop_time"] = self.start_time + (stop_epoch + 1) * self.epoch_period
                tapi_data[tapi_id]["bit"] = bit
                tapi_data[tapi_id]["urls"] = urls

                # Save data for the latest TAPI period
                if start_epoch > last_tapi_start_epoch:
                    last_tapi_id = tapi_id
                    last_tapi_start_epoch = start_epoch
                    last_tapi_stop_epoch = stop_epoch

                # Check TAPI acceptance for each epoch
                sql = """
                    SELECT
                        epoch,
                        tapi_accept,
                        confirmed,
                        reverted
                    FROM blocks
                    WHERE
                        epoch BETWEEN %s and %s
                    ORDER BY epoch ASC
                """ % (start_epoch, stop_epoch - 1)
                block_data = witnet_database.sql_return_all(sql)
                tapi_data[tapi_id]["previous_epoch"], tapi_data[tapi_id]["accept"] = self.collect_acceptance_data(witnet_database, start_epoch, stop_epoch, block_data)

                # Create summary statistics
                tapi_period_length = stop_epoch - start_epoch
                tapi_data[tapi_id]["rates"], tapi_data[tapi_id]["relative_acceptance_rate"], tapi_data[tapi_id]["global_acceptance_rate"] = self.create_summary(tapi_period_length, tapi_data[tapi_id]["accept"])

                # Group acceptance data
                tapi_data[tapi_id]["accept"] = self.convert_binary_acceptance_to_ints(tapi_data[tapi_id]["accept"])

                # Mark this TAPI period as inactive (will be overwritten below if the TAPI period is actually active)
                tapi_data[tapi_id]["active"] = False

                # Save the current epoch per TAPI
                tapi_data[tapi_id]["current_epoch"] = last_epoch

            if last_tapi_id != 0 and last_epoch > last_tapi_start_epoch and last_epoch < last_tapi_stop_epoch:
                logger.info(f"TAPI is active, collecting data between epochs {last_tapi_start_epoch} and {last_tapi_stop_epoch - 1}")

                tapi_data_updated = True

                # Check TAPI acceptance for each epoch
                sql = """
                    SELECT
                        epoch,
                        tapi_accept,
                        confirmed,
                        reverted
                    FROM blocks
                    WHERE
                        epoch BETWEEN %s and %s
                    ORDER BY epoch ASC
                """ % (last_tapi_start_epoch, last_tapi_stop_epoch - 1)
                block_data = witnet_database.sql_return_all(sql)
                tapi_data[last_tapi_id]["previous_epoch"], tapi_data[last_tapi_id]["accept"] = self.collect_acceptance_data(witnet_database, tapi_data[last_tapi_id]["start_epoch"], tapi_data[last_tapi_id]["stop_epoch"], block_data)

                # Create summary statistics
                tapi_period_length = tapi_data[last_tapi_id]["stop_epoch"] - tapi_data[last_tapi_id]["start_epoch"]
                tapi_data[last_tapi_id]["rates"], tapi_data[last_tapi_id]["relative_acceptance_rate"], tapi_data[last_tapi_id]["global_acceptance_rate"] = self.create_summary(tapi_period_length, tapi_data[last_tapi_id]["accept"])

                # Group acceptance data
                tapi_data[last_tapi_id]["accept"] = self.convert_binary_acceptance_to_ints(tapi_data[last_tapi_id]["accept"])

                # Mark this tapi as active
                tapi_data[last_tapi_id]["active"] = True

                # Save the current epoch per TAPI
                tapi_data[last_tapi_id]["current_epoch"] = last_epoch

                tapi_data[last_tapi_id]["last_updated"] = int(time.time() / self.refresh_timeout) * self.refresh_timeout
            # No need to update anything
            else:
                logger.info(f"TAPI is not active at epoch {last_epoch}")
                if last_tapi_id != 0:
                    tapi_data[last_tapi_id]["active"] = False
                    tapi_data[last_tapi_id]["current_epoch"] = last_epoch

            # If the local TAPI data was updated, push it onto the queue
            if tapi_data_updated:
                tapi_data_updated = False

                # Empty queue
                while not tapi_queue.empty():
                    try:
                        # Fetch data in a non-blocking way so we can catch possible concurrency problems
                        tapi_queue.get(False)
                    except Queue.Empty:
                        break

                tapi_queue.put(tapi_data)

            # Rebuild TAPI data every hour
            elapsed = math.ceil(time.time() - start)
            if elapsed < self.refresh_timeout:
                sleep_time = self.refresh_timeout - elapsed
            else:
                sleep_time = self.refresh_timeout - (elapsed % self.refresh_timeout)
            logger.info(f"Collected TAPI data which took {elapsed} seconds, sleeping {sleep_time} seconds")
            time.sleep(sleep_time)

    # Fetch all TAPI data
    def init_tapi(self):
        self.logger.info("init_tapi()")

        if not self.tapi_queue.empty():
            try:
                self.logger.info("Initializing TAPI data")
                # Fetch data in a non-blocking way so we can catch possible concurrency problems
                self.tapi_data = self.tapi_queue.get(False)
            except Empty:
                self.logger.warning("Failed to initialize TAPI data")

        return self.tapi_data

    # Only fetch active TAPI data
    def update_tapi(self):
        self.logger.info(f"update_tapi()")

        if not self.tapi_queue.empty():
            try:
                self.logger.info("Updating TAPI data")
                # Fetch data in a non-blocking way so we can catch possible concurrency problems
                self.tapi_data = self.tapi_queue.get(False)
            except Empty:
                self.logger.warning("Failed to update TAPI data")

        for tapi in sorted(self.tapi_data.keys()):
            # Return active TAPI or the TAPI that is in the future
            if self.tapi_data[tapi]["active"] or self.tapi_data[tapi]["current_epoch"] < self.tapi_data[tapi]["start_epoch"]:
                return self.tapi_data[tapi]
        return {}
