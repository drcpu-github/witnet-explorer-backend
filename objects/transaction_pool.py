import time

from blockchain.witnet_database import WitnetDatabase

from transactions.data_request import DataRequest
from transactions.value_transfer import ValueTransfer

class TransactionPool(object):
    def __init__(self, database_config, log_queue):
        self.witnet_database = WitnetDatabase(database_config, log_queue=log_queue, log_label="db-pool")

    def insert_empty_lists(self, start_timestamp, stop_timestamp, lst):
        # Edge case where there are no transactions
        if len(lst) == 0:
            interpolated_lst = []
            timestamp = start_timestamp
            while stop_timestamp - timestamp > 60:
                interpolated_lst.append((timestamp + 60, [], []))
                timestamp += 60
        # Normal case
        else:
            interpolated_lst = []
            # First check if we need to insert empty lists at the start
            timestamp = lst[0][0]
            while timestamp - start_timestamp > 60:
                interpolated_lst.append((start_timestamp + 60, [], []))
                start_timestamp += 60
            interpolated_lst.append(lst[0])

            # Then loop over the available data and check if we need to interpolate
            timestamp = interpolated_lst[-1][0]
            for l in lst[1:]:
                while l[0] - timestamp > 60:
                    interpolated_lst.append((timestamp + 60, [], []))
                    timestamp += 60
                interpolated_lst.append(l)
                timestamp = l[0]

            # Last check if we need to append empty lists at the end
            timestamp = interpolated_lst[-1][0]
            while stop_timestamp - timestamp > 60:
                interpolated_lst.append((timestamp + 60, [], []))
                timestamp += 60

        return interpolated_lst

    def transform_to_dict(self, lst):
        lst_dict = []
        unique_fees = set()
        for l in lst:
            for fee in l[1]:
                unique_fees.add(fee)
        unique_fees.add(0)
        unique_fees = sorted(list(unique_fees))

        if len(unique_fees) > 9:
            boundary = len(unique_fees) / 9
            fee_categories = {fee: str(unique_fees[int((int(i / boundary) + 1) * boundary - 1)]) for i, fee in enumerate(unique_fees)}
            fee_categories[0] = "0"
        else:
            fee_categories = {fee: str(fee) for fee in unique_fees}
            fee_categories[0] = "0"
        unique_fee_categories = sorted(list(set(fee_categories.values())), key=lambda l: int(l))

        # Transform list of lists to a list of dicts
        for l in lst:
            lst_dict.append({
                "timestamp": l[0]
            })
            # Add zero value for pretty labels
            if len(l[1]) == 0:
                lst_dict[-1]["0"] = 0
            else:
                for fee, txns in zip(l[1], l[2]):
                    fee_category = fee_categories[fee]
                    if fee_category in lst_dict[-1]:
                        lst_dict[-1][fee_category] += txns
                    else:
                        lst_dict[-1][fee_category] = txns

        return lst_dict, unique_fee_categories

    def get_mempool_transactions(self):
        timestamp = int(time.time())
        timestamp_stop = int(timestamp / 10) * 10
        timestamp_start = int((timestamp - 24 * 60 * 60) / 60) * 60

        # Get dictionaries for the last 24h
        sql = """
            SELECT
                timestamp,
                fee_per_unit,
                num_txns
            FROM pending_data_request_txns
            WHERE
                timestamp > %s
            ORDER BY timestamp ASC
        """ % timestamp_start
        pending_data_requests = self.witnet_database.sql_return_all(sql)

        sql = """
            SELECT
                timestamp,
                fee_per_unit,
                num_txns
            FROM pending_value_transfer_txns
            WHERE
                timestamp > %s
            ORDER BY timestamp ASC
        """ % timestamp_start
        pending_value_transfers = self.witnet_database.sql_return_all(sql)

        # Loop over the values and check if they're spaced by 60 seconds
        # If they are not, there were no pending transactions, add empty lists
        pending_data_requests = self.insert_empty_lists(timestamp_start, timestamp_stop, pending_data_requests)
        pending_value_transfers = self.insert_empty_lists(timestamp_start, timestamp_stop, pending_value_transfers)

        # Transform list to a list of dicts and get a set of unique fees
        pending_data_requests, data_request_fees = self.transform_to_dict(pending_data_requests)
        pending_value_transfers, value_transfer_fees = self.transform_to_dict(pending_value_transfers)

        return {
            "pending_data_requests": pending_data_requests,
            "data_request_fees": data_request_fees,
            "pending_value_transfers": pending_value_transfers,
            "value_transfer_fees": value_transfer_fees,
            "last_updated": timestamp
        }
