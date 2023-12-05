import logging
import logging.handlers

from blockchain.transactions.commit import Commit
from blockchain.transactions.data_request import DataRequest
from blockchain.transactions.reveal import Reveal
from blockchain.transactions.tally import Tally
from schemas.search.data_request_report_schema import (
    DataRequestReport as DataRequestReportSchema,
)
from util.database_manager import DatabaseManager


class DataRequestReport(object):
    def __init__(
        self,
        transaction_type,
        transaction_hash,
        consensus_constants,
        logger=None,
        log_queue=None,
        database=None,
        database_config=None,
    ):
        self.transaction_type = transaction_type
        self.transaction_hash = transaction_hash

        self.consensus_constants = consensus_constants
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period
        self.collateral_minimum = consensus_constants.collateral_minimum

        # Set up logger
        if logger:
            self.logger = logger
        elif log_queue:
            self.log_queue = log_queue
            self.configure_logging_process(log_queue, "node-manager")
            self.logger = logging.getLogger("node-manager")
        else:
            self.logger = None

        if database:
            self.database = database
        elif database_config:
            self.database = DatabaseManager(
                database_config, logger=self.logger, custom_types=["utxo", "filter"]
            )
        else:
            self.database = None

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def get_data_request_hash(self):
        # Set the data request hash based on the transaction type
        if self.transaction_type == "data_request":
            data_request_hash = self.transaction_hash
            self.logger.info(f"data_request, get_report({data_request_hash})")
        elif self.transaction_type == "commit":
            self.logger.info(f"commit, get_report({self.transaction_hash})")
            self.commit = Commit(
                self.consensus_constants, logger=self.logger, database=self.database
            )
            data_request_hash = self.commit.get_data_request_hash(self.transaction_hash)
            self.logger.info(f"data_request, get_report({data_request_hash})")
        elif self.transaction_type == "reveal":
            self.logger.info(f"reveal, get_report({self.transaction_hash})")
            self.reveal = Reveal(
                self.consensus_constants, logger=self.logger, database=self.database
            )
            data_request_hash = self.reveal.get_data_request_hash(self.transaction_hash)
            self.logger.info(f"data_request, get_report({data_request_hash})")
        elif self.transaction_type == "tally":
            self.logger.info(f"tally, get_report({self.transaction_hash})")
            self.tally = Tally(
                self.consensus_constants, logger=self.logger, database=self.database
            )
            data_request_hash = self.tally.get_data_request_hash(self.transaction_hash)
            self.logger.info(f"data_request, get_report({data_request_hash})")
        return data_request_hash

    def get_report(self):
        self.data_request_hash = self.get_data_request_hash()

        # If there was an error, return the error message
        if "error" in self.data_request_hash:
            self.logger.error(
                f"Error when fetching data request hash: {self.data_request_hash['error']}"
            )
            return {"error": self.data_request_hash["error"]}

        # Get details from data request transaction
        self.get_data_request_details()

        # Get all commit, reveal and tally transactions
        self.get_commit_details()
        self.get_reveal_details()
        self.get_tally_details()

        # Add empty reveals for all commits that did not have a matching reveal
        self.add_missing_reveals()
        # Sort commit, reveals and tally by address
        self.sort_by_address()
        # Mark errors and liars
        self.mark_errors()
        self.mark_liars()

        return DataRequestReportSchema().load(
            {
                "transaction_type": self.transaction_type,
                "data_request": self.data_request,
                "commits": self.commits,
                "reveals": self.reveals,
                "tally": self.tally,
            }
        )

    def get_data_request_details(self):
        self.logger.info(f"get_data_request_details({self.data_request_hash})")
        data_request = DataRequest(
            self.consensus_constants, logger=self.logger, database=self.database
        )
        self.data_request = data_request.get_transaction_from_database(
            self.data_request_hash
        )

    def get_commit_details(self):
        self.logger.info(f"get_commit_details({self.data_request_hash})")
        commit = Commit(
            self.consensus_constants, logger=self.logger, database=self.database
        )
        self.commits = commit.get_commits_for_data_request(self.data_request_hash)

    def get_reveal_details(self):
        self.logger.info(f"get_reveal_details({self.data_request_hash})")
        reveal = Reveal(
            self.consensus_constants, logger=self.logger, database=self.database
        )
        self.reveals = reveal.get_reveals_for_data_request(self.data_request_hash)

    def get_tally_details(self):
        self.logger.info(f"get_tally_details({self.data_request_hash})")
        tally = Tally(
            self.consensus_constants, logger=self.logger, database=self.database
        )
        self.tally = tally.get_tally_for_data_request(self.data_request_hash)

    def add_missing_reveals(self):
        if self.commits and self.reveals:
            commit_addresses = [commit["address"] for commit in self.commits]
            reveal_addresses = [reveal["address"] for reveal in self.reveals]
            for commit_address in commit_addresses:
                if commit_address not in reveal_addresses:
                    # At least one reveal, assume the missing reveal would have been in the same epoch
                    if len(self.reveals) > 0:
                        missing_epoch = self.reveals[0]["epoch"]
                        missing_time = self.reveals[0]["timestamp"]
                        missing_confirmed = self.reveals[0]["confirmed"]
                        missing_reverted = self.reveals[0]["reverted"]
                    # No reveals, assume they would have been created the epoch after the commit
                    else:
                        missing_epoch = self.commits[0]["epoch"] + 1
                        missing_time = (
                            self.start_time + (missing_epoch + 1) * self.epoch_period
                        )
                        missing_confirmed = False
                        missing_reverted = False

                    self.reveals.append(
                        {
                            "block": None,
                            "hash": None,
                            "address": commit_address,
                            "reveal": "No reveal",
                            "success": False,
                            "error": False,
                            "liar": True,
                            "epoch": missing_epoch,
                            "timestamp": missing_time,
                            "confirmed": missing_confirmed,
                            "reverted": missing_reverted,
                        }
                    )

    def sort_by_address(self):
        if self.commits:
            self.commits = sorted(self.commits, key=lambda commit: commit["address"])
        if self.reveals:
            self.reveals = sorted(self.reveals, key=lambda commit: commit["address"])

    def mark_errors(self):
        if self.reveals:
            for reveal in self.reveals:
                if self.tally and reveal["address"] in self.tally["error_addresses"]:
                    reveal["error"] = True

    def mark_liars(self):
        if self.reveals:
            for reveal in self.reveals:
                if self.tally and reveal["address"] in self.tally["liar_addresses"]:
                    reveal["liar"] = True
