import optparse
import pylibmc
import sys
import time
import toml

from objects.data_request_report import DataRequestReport

from caching.client import Client

from util.logger import configure_logger

class DataRequestReports(Client):
    def __init__(self, config):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["data_request_reports"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["data_request_reports"]["level_file"]
        self.logger = configure_logger("report", log_filename, log_level)

        # Create database client, memcached client and a consensus constants object
        super().__init__(config, database=True, memcached_client=True, consensus_constants=True)

        # Fetch configured timeout for data request report cache expiry
        self.memcached_timeout = config["api"]["caching"]["scripts"]["data_request_reports"]["timeout"]
        # Calculate how many epochs in the past this script has to cache data request reports
        self.lookback_epochs = int(config["api"]["caching"]["scripts"]["data_request_reports"]["timeout"] / self.consensus_constants.checkpoints_period)

        self.cache_time_warning = config["api"]["caching"]["scripts"]["data_request_reports"]["cache_time_warning"]

    def process_data_requests(self):
        start = time.perf_counter()

        _, self.last_epoch = self.witnet_database.get_last_block(confirmed=False)

        # check until which epoch we succesfully added data request reports to the cache
        # if no epoch is found, start at the current confirmed epoch minus the TOML-defined timeout
        report_cache_epoch = self.memcached_client.get("report_cache_epoch")
        if not report_cache_epoch:
            report_cache_epoch = self.last_epoch - self.lookback_epochs

        self.logger.info(f"Fetching data request reports starting at epoch {report_cache_epoch} to {self.last_epoch}")

        sql = """
            SELECT
                data_request_txns.txn_hash,
                blocks.epoch
            FROM
                data_request_txns
            LEFT JOIN
                tally_txns
            ON
                data_request_txns.txn_hash=tally_txns.data_request_txn_hash
            LEFT JOIN
                blocks
            ON
                tally_txns.epoch=blocks.epoch
            WHERE
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
        """ % (report_cache_epoch, self.last_epoch)
        data_requests = self.witnet_database.sql_return_all(sql)

        self.logger.info(f"Collected {len(data_requests)} data requests in {time.perf_counter() - start:.2f}s")
        self.logger.info(f"Building data request reports starting at epoch {report_cache_epoch}")

        new_data_request_reports, updated_data_request_reports = 0, 0
        for txn_hash, epoch in data_requests:
            inner_start = time.perf_counter()

            # Try to fetch this data request report
            txn_hash = txn_hash.hex()
            data_request_report = self.memcached_client.get(txn_hash)

            # Was the data request report already present in the cache?
            if not data_request_report:
                data_request_report = self.cache_data_request_report(txn_hash, epoch, inner_start)
                if data_request_report == None:
                    continue

                confirmed = False
                if data_request_report["tally_txn"] != None and data_request_report["tally_txn"]["confirmed"] == True:
                    # track the last epoch for which we successfully added a confirmed data request report to the cache
                    # on the next execution of this script, it will start processing data request reports from that epoch
                    report_cache_epoch = epoch
                    confirmed = True

                new_data_request_reports += 1

                self.logger.info(f"Built {'confirmed' if confirmed else 'unconfirmed'} data request report {txn_hash} for epoch {epoch} and added it to the memcached cache in {time.perf_counter() - inner_start:.2f}s")
            else:
                if data_request_report["tally_txn"] == None or data_request_report["tally_txn"]["confirmed"] == False:
                    # Replace the current cached data request report with a new one since it could've been updated
                    data_request_report = self.cache_data_request_report(txn_hash, epoch, inner_start)

                    confirmed = False
                    if data_request_report["tally_txn"] != None and data_request_report["tally_txn"]["confirmed"] == True:
                        # track the last epoch for which we successfully added a confirmed data request report to the cache
                        # on the next execution of this script, it will start processing data request reports from that epoch
                        report_cache_epoch = epoch
                        confirmed = True

                    updated_data_request_reports += 1

                    self.logger.info(f"Updated data request report {txn_hash} for epoch {epoch} in memcached cache with a new {'confirmed' if confirmed else 'unconfirmed'} one in {time.perf_counter() - inner_start:.2f}s")
                else:
                    self.logger.debug(f"Found data request report {txn_hash} for epoch {epoch} in memcached cache in {time.perf_counter() - inner_start:.2f}s")

        # Save the most recent epoch for which we sucessfully cached a data request report
        self.memcached_client.set("report_cache_epoch", report_cache_epoch)

        time_elapsed = time.perf_counter() - start
        self.logger.info(f"Cached {new_data_request_reports} and updated {updated_data_request_reports} recent data request reports in {time_elapsed:.2f}s")
        if time_elapsed > self.cache_time_warning:
            self.logger.warning(f"Caching recent data request reports took too much time: {time_elapsed:.2f}s > {self.cache_time_warning:.2f}s")

    def cache_data_request_report(self, txn_hash, epoch, inner_start):
        # Build data request report
        data_request = DataRequestReport("data_request_txn", txn_hash, self.consensus_constants, logger=self.logger, database=self.witnet_database)
        data_request_report = data_request.get_report()
        if "error" in data_request_report:
            self.logger.warning(f"Could not create data request report {txn_hash} for epoch {epoch}")
            return None

        # Try to insert the data request report in the cache
        try:
            # Cache older data request reports for a shorter amount of time proportional to mimic the normal expiry time
            timeout = int(self.memcached_timeout * (self.lookback_epochs - self.last_epoch + epoch) / self.lookback_epochs)
            # Memcached timeouts bigger than 30 days needs to be specified as a unix timestamp
            if timeout > 60*60*24*30:
                timeout = int(time.time()) + timeout
            self.memcached_client.set(txn_hash, data_request_report, time=timeout)
        except pylibmc.TooBig as e:
            self.logger.warning(f"Built data request report {txn_hash} for epoch {epoch} in {time.perf_counter() - inner_start:.2f}s, but could not save it in the memcached instance because its size exceeded 1MB")
            return None

        return data_request_report

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    if options.config_file == None:
        sys.stderr.write("Need to specify a configuration file!\n")
        sys.exit(1)

    # Load config file
    config = toml.load(options.config_file)

    # Create data request report cache
    report_cache = DataRequestReports(config)
    report_cache.process_data_requests()

if __name__ == "__main__":
    main()
