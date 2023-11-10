import json
import optparse
import pylibmc
import sys
import time
import toml

from marshmallow import ValidationError

from objects.data_request_report import DataRequestReport
from caching.client import Client
from util.data_transformer import re_sql
from util.logger import configure_logger
from util.memcached import calculate_timeout
from util.common_sql import sql_last_block

class DataRequestReports(Client):
    def __init__(self, config):
        # Setup logger
        log_filename = config["api"]["caching"]["scripts"]["data_request_reports"]["log_file"]
        log_level = config["api"]["caching"]["scripts"]["data_request_reports"]["level_file"]
        self.logger = configure_logger("report", log_filename, log_level)

        super().__init__(config)

        # Fetch configured timeout for data request report cache expiry
        self.memcached_timeout = config["api"]["caching"]["scripts"]["data_request_reports"]["timeout"]
        # Calculate how many epochs in the past this script has to cache data request reports
        self.lookback_epochs = int(config["api"]["caching"]["scripts"]["data_request_reports"]["timeout"] / self.consensus_constants.checkpoints_period)

        self.cache_time_warning = config["api"]["caching"]["scripts"]["data_request_reports"]["cache_time_warning"]

    def process_data_requests(self):
        start = time.perf_counter()

        last = self.database.sql_return_one(sql_last_block)
        if last:
            self.last_epoch = last[1]
        else:
            return

        # check until which epoch we succesfully added data request reports to the cache
        # if no epoch is found, start at the current confirmed epoch minus the TOML-defined timeout
        data_request_reports_epoch = self.get_start_epoch("data_request_reports_epoch")
        if not data_request_reports_epoch:
            data_request_reports_epoch = self.last_epoch - self.lookback_epochs

        self.logger.info(f"Fetching data request reports starting at epoch {data_request_reports_epoch} to {self.last_epoch}")

        sql = """
            SELECT
                data_request_txns.txn_hash,
                blocks.epoch
            FROM
                data_request_txns
            LEFT JOIN
                tally_txns
            ON
                data_request_txns.txn_hash=tally_txns.data_request
            LEFT JOIN
                blocks
            ON
                tally_txns.epoch=blocks.epoch
            WHERE
                blocks.epoch BETWEEN %s AND %s
            ORDER BY
                blocks.epoch
        """ % (data_request_reports_epoch, self.last_epoch)
        data_requests = self.database.sql_return_all(re_sql(sql))

        self.logger.info(f"Collected {len(data_requests)} data requests in {time.perf_counter() - start:.2f}s")
        self.logger.info(f"Building data request reports starting at epoch {data_request_reports_epoch}")

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
                if data_request_report["tally"] != None and data_request_report["tally"]["confirmed"] == True:
                    # track the last epoch for which we successfully added a confirmed data request report to the cache
                    # on the next execution of this script, it will start processing data request reports from that epoch
                    data_request_reports_epoch = epoch
                    confirmed = True

                    # Save this data request report in the database table
                    self.save_data_request_report(txn_hash, data_request_report)

                new_data_request_reports += 1

                self.logger.info(f"Built {'confirmed' if confirmed else 'unconfirmed'} data request report {txn_hash} for epoch {epoch} and added it to the memcached cache in {time.perf_counter() - inner_start:.2f}s")
            else:
                tally = data_request_report["data_request_report"]["tally"]
                if tally is None or tally["confirmed"] == False:
                    # Replace the current cached data request report with a new one since it could've been updated
                    data_request_report = self.cache_data_request_report(txn_hash, epoch, inner_start)
                    if data_request_report == None:
                        continue

                    confirmed = False
                    if data_request_report["tally"] != None and data_request_report["tally"]["confirmed"] == True:
                        # track the last epoch for which we successfully added a confirmed data request report to the cache
                        # on the next execution of this script, it will start processing data request reports from that epoch
                        data_request_reports_epoch = epoch
                        confirmed = True

                        # Save this data request report in the database table
                        self.save_data_request_report(txn_hash, data_request_report)

                    updated_data_request_reports += 1

                    self.logger.info(f"Updated data request report {txn_hash} for epoch {epoch} in memcached cache with a new {'confirmed' if confirmed else 'unconfirmed'} one in {time.perf_counter() - inner_start:.2f}s")
                else:
                    self.logger.debug(f"Found data request report {txn_hash} for epoch {epoch} in memcached cache in {time.perf_counter() - inner_start:.2f}s")

        # Save the most recently processed epoch in the database to know where to start the next job
        self.set_start_epoch("data_request_reports_epoch", data_request_reports_epoch)

        time_elapsed = time.perf_counter() - start
        self.logger.info(f"Cached {new_data_request_reports} and updated {updated_data_request_reports} recent data request reports in {time_elapsed:.2f}s")
        if time_elapsed > self.cache_time_warning:
            self.logger.warning(f"Caching recent data request reports took too much time: {time_elapsed:.2f}s > {self.cache_time_warning:.2f}s")

    def cache_data_request_report(self, txn_hash, epoch, inner_start):
        # Build data request report
        data_request = DataRequestReport("data_request", txn_hash, self.consensus_constants, logger=self.logger, database=self.database)
        try:
            data_request_report = data_request.get_report()
            if "error" in data_request_report:
                self.logger.warning(f"Could not create data request report {txn_hash} for epoch {epoch}")
                return None
        except ValidationError as err_info:
            self.logger.error(f"Could not validate data request report {txn_hash} for epoch {epoch}: {err_info}")
            return None

        # Try to insert the data request report in the cache
        try:
            # Cache older data request reports for a shorter amount of time proportional to mimic the normal expiry time
            timeout = calculate_timeout(int(self.memcached_timeout * (self.lookback_epochs - self.last_epoch + epoch) / self.lookback_epochs))
            self.memcached_client.set(
                txn_hash,
                {
                    "response_type": "data_request_report",
                    "data_request_report": data_request_report,
                },
                time=timeout,
            )
        except pylibmc.TooBig as e:
            self.logger.warning(f"Built data request report {txn_hash} for epoch {epoch} in {time.perf_counter() - inner_start:.2f}s, but could not save it in the memcached instance because its size exceeded 1MB")
            return None

        return data_request_report

    def save_data_request_report(self, txn_hash, data_request_report):
        sql = """
            INSERT INTO data_request_reports(
                data_request_hash,
                report
            ) VALUES (%s, %s)
            ON CONFLICT ON CONSTRAINT
                data_request_reports_pkey
            DO NOTHING
        """
        self.database.sql_insert_one(sql, (txn_hash, json.dumps(data_request_report)))

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file", help="Specify a configuration file")
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
