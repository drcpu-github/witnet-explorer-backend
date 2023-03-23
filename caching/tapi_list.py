import optparse
import os
import pylibmc
import sys
import time
import toml

import matplotlib.pyplot as plt
import matplotlib.colors

from caching.client import Client

from util.logger import configure_logger

class TapiList(Client):
    def __init__(self, config):
        self.plot_dir = config["api"]["caching"]["plot_directory"]
        if not os.path.exists(self.plot_dir):
            os.makedirs(self.plot_dir)

        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["tapi_list"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["tapi_list"]["level_file"]
        self.logger = configure_logger("tapi", log_filename, log_level)

        # Initialize self.database, self.memcached_client and self.consensus_constants
        super().__init__(config, database=True, memcached_client=True, consensus_constants=True)

        # Assign some of the consensus constants
        self.start_time = self.consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = self.consensus_constants.checkpoints_period

    def collect_acceptance_data(self, start_epoch, stop_epoch, tapi_bit, blocks):
        # Fetch the last epoch
        _, last_epoch = self.witnet_database.get_last_block(confirmed=False)

        acceptance_data = []

        # This TAPI has not started yet
        if last_epoch < start_epoch:
            return acceptance_data

        # If the initial x blocks since the TAPI start epoch were rolled back, append 0 to indicate reject
        acceptance_data.extend([0] * max(0, blocks[0][0] - start_epoch))

        # Loop over the available blocks and process those
        previous_epoch = start_epoch
        for block in blocks:
            epoch, tapi_signals, confirmed, reverted = block

            # If the previous block was more than 1 epoch ago, extrapolate TAPI acceptance to 0 (reject) for rollbacks
            if epoch > previous_epoch + 1:
                acceptance_data.extend([0 for i in range(epoch - previous_epoch - 1)])

            # If the block was confirmed, check TAPI acceptance
            if confirmed and not reverted:
                if tapi_signals == None:
                    accept = 0
                else:
                    accept = (tapi_signals & (1 << tapi_bit)) >> tapi_bit
                acceptance_data.append(accept)

            # If the block was reverted, hardcode TAPI as not accepted
            if reverted:
                acceptance_data.append(0)

            previous_epoch = epoch

        # If the last x blocks before the TAPI stop epoch were rolled back, append 0 indicating reject
        acceptance_data.extend([0] * (min(last_epoch, stop_epoch - 1) - blocks[-1][0]))

        return acceptance_data

    def create_acceptance_plot(self, tapi, acceptance):
        epochs = len(acceptance)
        self.logger.info(f"Plotting {epochs} epochs for TAPI {tapi['tapi_id']}")

        # Colors for reject and accept
        cmap = matplotlib.colors.ListedColormap(["#12243a", "#0bb1a5"])

        # Transform acceptance list to 2D array for plotting
        # The matplotlib.imsave function can only plot 2D arrays where all rows have the same dimension
        # We dynamically scale the number of epochs per row to maximize the data in a the 2D array
        columns = 480
        tapi_length = tapi["stop_epoch"] - tapi["start_epoch"]

        # TAPI has finished or only one row of data can be shown
        if len(acceptance) == tapi_length or len(acceptance) <= columns:
            acceptance_2d = [acceptance[i : i + columns] for i in range(0, len(acceptance), columns)]
            self.logger.info(f"Plotting the complete TAPI for {tapi['title']}")
            plt.imsave(os.path.join(self.plot_dir, f"tapi-{tapi['tapi_id']}.png"), acceptance_2d, cmap=cmap)
        # Scale the number of epochs per row
        else:
            # Find the best divider, minimizing the amount of epochs left out of the visualization
            smallest_remainder, smallest_column_difference = epochs, epochs
            # To prevent the figure size becoming too high or wide, only try divisions in a specified range
            min_rows = int(epochs / (columns * 2) + 1)
            max_rows = int(epochs / (columns / 2) + 1)
            for rows in range(min_rows, max_rows):
                remainder = epochs % rows
                if remainder == 0:
                    # For perfect dividers, find the one that creates a figure with a number of columns closest to the requested amount
                    if abs(columns - int(epochs / rows)) < smallest_column_difference:
                        smallest_column_difference = abs(columns - int(epochs / rows))
                        dynamic_columns = int(epochs / rows)
                elif remainder < smallest_remainder:
                    smallest_remainder = remainder
                    dynamic_columns = int(epochs / rows)
            # Build the 2D array
            acceptance_2d = [acceptance[i : i + dynamic_columns] for i in range(0, epochs, dynamic_columns)]
            if len(acceptance_2d) >= 2 and len(acceptance_2d[-1]) != len(acceptance_2d[-2]):
                acceptance_2d = acceptance_2d[:-1]
            # Save the figure
            self.logger.info(f"Plotting the TAPI for {tapi['title']} using dimensions {len(acceptance_2d)} x {dynamic_columns} ({remainder} epochs left out)")
            plt.imsave(os.path.join(self.plot_dir, f"tapi-{tapi['tapi_id']}.png"), acceptance_2d, cmap=cmap)

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

    def collect_tapi_data(self):
        start = time.perf_counter()

        self.logger.info("Collecting TAPI data")

        # Update current TAPI starting at the last epoch processed
        _, last_epoch = self.witnet_database.get_last_block(confirmed=False)

        # Setup of all TAPI data, query this table periodically to find newly added TAPI periods
        sql = """
            SELECT
                id,
                title,
                description,
                urls,
                tapi_start_epoch,
                tapi_stop_epoch,
                tapi_bit
            FROM
                wips
            WHERE
                tapi_bit IS NOT NULL
            ORDER BY
                id
            ASC
        """
        tapis = self.witnet_database.sql_return_all(sql)

        self.tapi_data = {}
        for tapi in tapis:
            # Save TAPI metadata
            tapi_id, title, description, urls, start_epoch, stop_epoch, bit = tapi

            # Check if we already saved a (partial) TAPI object
            local_tapi_data = self.memcached_client.get(f"tapi-{tapi_id}")
            if local_tapi_data:
                self.logger.info(f"Fetching TAPI definition for TAPI {tapi_id} from memcached instance")
                self.tapi_data[tapi_id] = local_tapi_data
            # If not, initialize it
            else:
                self.logger.info(f"Building TAPI definition for TAPI {tapi_id}, running from epoch {start_epoch} to epoch {stop_epoch - 1}")
                self.tapi_data[tapi_id] = {
                    "tapi_id": tapi_id,
                    "title": title,
                    "description": description,
                    "urls": urls,
                    "start_epoch": start_epoch,
                    "start_time": self.start_time + (start_epoch + 1) * self.epoch_period,
                    "stop_epoch": stop_epoch,
                    "stop_time": self.start_time + (stop_epoch + 1) * self.epoch_period,
                    "bit": bit,
                    "rates": [],
                    "relative_acceptance_rate": 0,
                    "global_acceptance_rate": 0,
                    "active": False,
                    "finished": False,
                    "activated": False,
                    "current_epoch": last_epoch,
                    "last_updated": int(time.time())
                }

        for tapi_id, tapi in self.tapi_data.items():
            start_epoch, stop_epoch, finished = tapi["start_epoch"], tapi["stop_epoch"], tapi["finished"]
            # Check if the TAPI is active
            if last_epoch > start_epoch and not finished:
                status = "active" if last_epoch < stop_epoch else "finished"
                self.logger.info(f"TAPI {tapi_id} is {status}, collecting data between epochs {start_epoch} and {stop_epoch - 1}")

                # Check TAPI acceptance for each epoch
                sql = """
                    SELECT
                        epoch,
                        tapi_signals,
                        confirmed,
                        reverted
                    FROM blocks
                    WHERE
                        epoch BETWEEN %s and %s
                    ORDER BY epoch ASC
                """ % (start_epoch, stop_epoch - 1)
                block_data = self.witnet_database.sql_return_all(sql)
                acceptance_data = self.collect_acceptance_data(start_epoch, stop_epoch, tapi["bit"], block_data)

                # Create a static acceptance data plot
                self.create_acceptance_plot(tapi, acceptance_data)

                # Create summary statistics
                tapi_period_length = stop_epoch - start_epoch
                self.tapi_data[tapi_id]["rates"], self.tapi_data[tapi_id]["relative_acceptance_rate"], self.tapi_data[tapi_id]["global_acceptance_rate"] = self.create_summary(tapi_period_length, acceptance_data)

                # Mark this tapi as active
                if last_epoch < stop_epoch:
                    self.tapi_data[tapi_id]["active"] = True
                    self.tapi_data[tapi_id]["finished"] = False
                else:
                    self.tapi_data[tapi_id]["active"] = False
                    self.tapi_data[tapi_id]["finished"] = True
                    if self.tapi_data[tapi_id]["global_acceptance_rate"] >= 80:
                        self.tapi_data[tapi_id]["activated"] = True
                    else:
                        self.tapi_data[tapi_id]["activated"] = False

                # Save the current epoch per TAPI
                self.tapi_data[tapi_id]["current_epoch"] = last_epoch

                self.tapi_data[tapi_id]["last_updated"] = int(time.time())
            # No need to update anything
            else:
                self.logger.info(f"TAPI {tapi_id} is not active")

        self.logger.info(f"Collecting TAPI data took {time.perf_counter() - start:.2f}s")

    def save_tapi(self):
        self.logger.info("Saving all data in our memcached instance")
        for tapi_id, tapi in self.tapi_data.items():
            try:
                self.memcached_client.set(f"tapi-{tapi_id}", tapi)
            except pylibmc.TooBig as e:
                self.logger.warning("Could not save items in cache because the item size exceeded 1MB")
        self.memcached_client.set("tapis-cached", list(self.tapi_data.keys()))

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file", help="Specify a configuration file")
    options, args = parser.parse_args()

    # Load config file
    config = toml.load(options.config_file)

    # create TAPI cache
    tapi_cache = TapiList(config)
    tapi_cache.collect_tapi_data()
    tapi_cache.save_tapi()

if __name__ == "__main__":
    main()
