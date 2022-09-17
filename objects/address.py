import logging
import sys
import time

from node.witnet_node import WitnetNode

from transactions.reveal import translate_reveal
from transactions.tally import translate_tally

from util.database_manager import DatabaseManager
from util.witnet_functions import calculate_block_reward

class Address(object):
    def __init__(self, address, database_config, node_config, consensus_constants, logging_queue=None):
        # Set address
        self.address = address.strip()

        # Save configs
        self.database_config = database_config
        self.node_config = node_config

        # Save consensus constants
        self.consensus_constants = consensus_constants
        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

        # Create logger
        if logging_queue:
            self.configure_logging_process(logging_queue, "address")
            self.logger = logging.getLogger("address")
        else:
            self.logger = None

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def connect_to_database(self):
        self.db_mngr = DatabaseManager(self.database_config, named_cursor=False, logger=self.logger)

    def close_database_connection(self):
        self.db_mngr.terminate(verbose=False)

    def get_details(self):
        # Connect to node pool
        witnet_node = WitnetNode(self.node_config, logger=self.logger)

        # Get balance
        balance = witnet_node.get_balance(self.address)
        if type(balance) is dict and "error" in balance:
            balance = "Could not retrieve balance"
        else:
            balance = balance["result"]
            balance = balance[self.address]["total"]

        # Get reputation
        reputation = witnet_node.get_reputation(self.address)
        if type(reputation) is dict and "error" in reputation:
            total_reputation = "Could not retrieve total reputation"
            eligibility = "Could not retrieve eligibility"
            reputation = "Could not retrieve reputation"
        else:
            result = reputation["result"]
            total_reputation = result["total_reputation"]
            reputation = result["stats"][self.address]
            eligibility = reputation["eligibility"]
            reputation = reputation["reputation"]

        return {
            "type": "address",
            "address": self.address,
            "balance": balance,
            "reputation": reputation,
            "eligibility": eligibility,
            "total_reputation": total_reputation,
        }

    def get_value_transfers(self, limit, epoch):
        num_value_transfers_in, value_transfers_in = self.get_value_transfer_txns_in(limit, epoch)
        num_value_transfers_out, value_transfers_out = self.get_value_transfer_txns_out(limit, epoch)
        value_transfers = self.merge_value_transfer_txns(value_transfers_in, value_transfers_out, limit)

        return {
            "type": "address",
            "address": self.address,
            "num_value_transfers": len(value_transfers),
            "value_transfers": value_transfers,
        }

    def get_value_transfer_txns_in(self, limit, epoch):
        # get value transfers arriving at our address
        start = time.time()

        sql = """
            SELECT
                value_transfer_txns.txn_hash,
                value_transfer_txns.input_addresses,
                value_transfer_txns.input_values,
                value_transfer_txns.output_addresses,
                value_transfer_txns.output_values,
                value_transfer_txns.timelocks,
                value_transfer_txns.weight,
                value_transfer_txns.epoch,
                blocks.reverted
            FROM value_transfer_txns
            LEFT JOIN blocks ON
                value_transfer_txns.epoch=blocks.epoch
            WHERE
                output_addresses @> ARRAY['%s']::CHAR(43)[] AND
                NOT ('%s' = ANY(input_addresses))
        """ % (self.address, self.address)
        if epoch > 0:
            sql += " AND epoch > %s" % epoch
        sql += " ORDER BY epoch DESC"
        if limit > 0 and epoch == 0:
            sql += " LIMIT %s" % limit
        result = self.db_mngr.sql_return_all(sql)

        value_transfers_in = []
        if result:
            for value_transfer in result:
                txn_hash, input_addresses, input_values, output_addresses, output_values, timelocks, weight, txn_epoch, block_reverted = value_transfer

                timestamp = self.start_time + (txn_epoch + 1) * self.epoch_period

                if len(set(input_addresses)) > 1:
                    source = "multiple input addresses"
                elif len(input_addresses) == 0:
                    source = "genesis block"
                else:
                    source = input_addresses[0]

                total_value = 0
                for output_address, output_value in zip(output_addresses, output_values):
                    if output_address == self.address:
                        total_value += output_value

                fee = sum(input_values) - sum(output_values) if len(input_values)  > 0 else 0

                priority = max(1, int(fee / weight))

                # Only account for timelocks for when the output_address is this address
                now = int(time.time())
                locked = any([output_address == self.address and timelock > now for output_address, timelock in zip(output_addresses, timelocks)])

                txn_type = 1
                value_transfers_in.append([txn_type, txn_hash.hex(), txn_epoch, timestamp, source, self.address, total_value, fee, priority, locked, block_reverted])

        return len(value_transfers_in), value_transfers_in

    def get_value_transfer_txns_out(self, limit, epoch):
        # get value transfers starting at our address
        start = time.time()

        sql = """
            SELECT
                value_transfer_txns.txn_hash,
                value_transfer_txns.input_addresses,
                value_transfer_txns.input_values,
                value_transfer_txns.output_addresses,
                value_transfer_txns.output_values,
                value_transfer_txns.timelocks,
                value_transfer_txns.weight,
                value_transfer_txns.epoch,
                blocks.reverted
            FROM value_transfer_txns
            LEFT JOIN blocks ON
                value_transfer_txns.epoch=blocks.epoch
            WHERE
                input_addresses @> ARRAY['%s']::CHAR(42)[]
        """ % self.address
        if epoch > 0:
            sql += " AND epoch > %s" % epoch
        sql += " ORDER BY epoch DESC"
        if limit > 0 and epoch == 0:
            sql += " LIMIT %s" % limit
        result = self.db_mngr.sql_return_all(sql)

        value_transfers_out = []
        if result:
            for value_transfer in result:
                txn_hash, input_addresses, input_values, output_addresses, output_values, timelocks, weight, txn_epoch, block_reverted = value_transfer

                timestamp = self.start_time + (txn_epoch + 1) * self.epoch_period

                total_value = 0
                for output_address, output_value in zip(output_addresses, output_values):
                    # Discount change output
                    if output_address != self.address:
                        total_value += output_value

                unique_output_addresses = list(set(output_addresses) - set([self.address]))
                # Transaction with multiple output_addresses different from the source address
                if len(unique_output_addresses) > 1:
                    txn_type = 2
                    output_address = "multiple output addresses"
                else:
                    # Split or merge UTXO transaction where the output_address address is also the source address
                    if len(unique_output_addresses) == 0:
                        txn_type = 0
                        output_address = self.address
                    # Transaction with a output_address different from the source address
                    else:
                        txn_type = 2
                        output_address = unique_output_addresses[0]

                fee = sum(input_values) - sum(output_values)

                priority = max(1, int(fee / weight))

                # Account for all timelocks
                now = int(time.time())
                locked = any([timelock > now for timelock in timelocks])

                value_transfers_out.append([txn_type, txn_hash.hex(), txn_epoch, timestamp, self.address, output_address, total_value, fee, priority, locked, block_reverted])

        return len(value_transfers_out), value_transfers_out

    def merge_value_transfer_txns(self, value_transfers_in, value_transfers_out, limit):
        value_transfers = []
        for vt_out in value_transfers_out:
            value_transfers.append(vt_out)
        for vt_in in value_transfers_in:
            value_transfers.append(vt_in)
        value_transfers = sorted(value_transfers, key=lambda l: l[2], reverse=True)
        if limit > 0:
            return value_transfers[:limit]
        else:
            return value_transfers

    def get_blocks(self, limit, epoch):
        start = time.time()

        sql = """
            SELECT
                blocks.block_hash,
                blocks.value_transfer,
                blocks.data_request,
                blocks.commit,
                blocks.reveal,
                blocks.tally,
                blocks.epoch,
                blocks.reverted,
                mint_txns.output_values
            FROM blocks
            LEFT JOIN mint_txns ON
                mint_txns.epoch=blocks.epoch
            WHERE
                mint_txns.miner='%s'
        """ % self.address
        if epoch > 0:
            sql += " AND blocks.epoch > %s" % epoch
        sql += " ORDER BY blocks.epoch DESC"
        if limit > 0 and epoch == 0:
            sql += " LIMIT %s" % limit
        result = self.db_mngr.sql_return_all(sql)

        blocks_minted = []
        if result:
            for block in result:
                block_hash, value_transfers, data_requests, commits, reveals, tallies, block_epoch, block_reverted, output_values = block

                timestamp = self.start_time + (block_epoch + 1) * self.epoch_period

                block_reward = sum(output_values)
                block_fees = sum(output_values) - calculate_block_reward(block_epoch, self.consensus_constants)

                blocks_minted.append((block_hash.hex(), timestamp, block_epoch, block_reward, block_fees, value_transfers, data_requests, commits, reveals, tallies, block_reverted))

        return {
            "type": "address",
            "address": self.address,
            "num_blocks_minted": len(blocks_minted),
            "blocks": blocks_minted,
        }

    def get_data_requests_solved(self, limit, epoch):
        start = time.time()

        sql = """
            SELECT
                data_request_txns.collateral,
                data_request_txns.witness_reward,
                commit_txns.data_request_txn_hash,
                reveal_txns.txn_hash,
                reveal_txns.result,
                tally_txns.epoch,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.success,
                blocks.reverted
            FROM commit_txns
            LEFT JOIN data_request_txns ON
                commit_txns.data_request_txn_hash=data_request_txns.txn_hash
            LEFT JOIN reveal_txns ON
                commit_txns.data_request_txn_hash=reveal_txns.data_request_txn_hash
                AND
                commit_txns.txn_address=reveal_txns.txn_address
            LEFT JOIN tally_txns ON
                commit_txns.data_request_txn_hash=tally_txns.data_request_txn_hash
            LEFT JOIN blocks ON
                tally_txns.epoch=blocks.epoch
            WHERE
                commit_txns.txn_address='%s'
        """ % self.address
        if epoch > 0:
            sql += " AND commit_txns.epoch > %s" % epoch
        sql += " ORDER BY commit_txns.epoch DESC"
        if limit > 0 and epoch == 0:
            sql += " LIMIT %s" % limit
        result = self.db_mngr.sql_return_all(sql)

        solved_data_request_txns = []
        if result:
            for data_request in result:
                collateral, witness_reward, data_request_txn_hash, reveal_txn_hash, reveal_value, tally_epoch, error_addresses, liar_addresses, success, block_reverted = data_request

                # Check if any of the values is None which would indicate this data request was not completed (or incorrectly processed)
                if error_addresses == None or liar_addresses == None or success == None or block_reverted == None:
                    continue

                # Calculate timestamp
                timestamp = self.start_time + (tally_epoch + 1) * self.epoch_period

                # Translate reveal value
                if reveal_value:
                    succes, translated_reveal = translate_reveal(reveal_txn_hash, reveal_value)
                else:
                    translated_reveal = ""

                # Check if we were marked as an error revealer
                error = self.address in error_addresses

                # Check if we were marked as a liar
                liar = self.address in liar_addresses

                # Check if there was an error in the tally or if it was reverted (and not redone)
                success = success and not block_reverted

                solved_data_request_txns.append((success, data_request_txn_hash.hex(), tally_epoch, timestamp, collateral, witness_reward, translated_reveal, error, liar))

        return {
            "type": "address",
            "address": self.address,
            "num_data_requests_solved": len(solved_data_request_txns),
            "data_requests_solved": solved_data_request_txns,
        }

    def get_data_requests_launched(self, limit, epoch):
        start = time.time()

        sql = """
            SELECT
                data_request_txns.txn_hash,
                data_request_txns.input_values,
                data_request_txns.output_values,
                data_request_txns.witnesses,
                data_request_txns.collateral,
                data_request_txns.consensus_percentage,
                tally_txns.txn_hash,
                tally_txns.epoch,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.result,
                tally_txns.success,
                blocks.reverted
            FROM data_request_txns
            LEFT JOIN tally_txns ON
                data_request_txns.txn_hash=tally_txns.data_request_txn_hash
            LEFT JOIN blocks ON
                tally_txns.epoch=blocks.epoch
            WHERE
                data_request_txns.input_addresses @> ARRAY['%s']::CHAR(43)[]
        """ % self.address
        if epoch > 0:
            sql += " AND data_request_txns.epoch > %s" % epoch
        sql += " ORDER BY data_request_txns.epoch DESC"
        if limit > 0 and epoch == 0:
            sql += " LIMIT %s" % limit
        result = self.db_mngr.sql_return_all(sql)

        launched_data_request_txns = []
        if result:
            # Loop and process
            for data_request in result:
                data_request_hash, input_values, output_values, witnesses, collateral, consensus_percentage, tally_txn_hash, tally_epoch, error_addresses, liar_addresses, result, success, block_reverted = data_request

                # Check if any of the values is None which would indicate this data request was not completed (or incorrectly processed)
                if any(dr == None for dr in data_request):
                    continue

                # Calculate timestamp
                timestamp = self.start_time + (tally_epoch + 1) * self.epoch_period

                # Calculate total fee of the data request (witnesses * witness_reward + mining fees per transaction)
                total_fee = sum(input_values) - sum(output_values)

                # Count the total number of error committers and liar_addresses
                num_errors, num_liars = len(error_addresses), len(liar_addresses)

                # Translate the cbor-encoded tally result
                translated_result = translate_tally(tally_txn_hash, result)

                # Check if the tally transaction was reverted or there was an error
                success = success and not block_reverted

                launched_data_request_txns.append((success, data_request_hash.hex(), tally_epoch, timestamp, total_fee, witnesses, collateral, consensus_percentage, num_errors, num_liars, translated_result))

        return {
            "type": "address",
            "address": self.address,
            "num_data_requests_launched": len(launched_data_request_txns),
            "data_requests_launched": launched_data_request_txns,
        }

    def get_last_epoch_processed(self):
        sql = """
            SELECT
                MAX(epoch)
            FROM reputation
        """
        last_epoch = self.db_mngr.sql_return_one(sql)[0]
        if last_epoch:
            return last_epoch
        else:
            return 0

    def get_reputation(self):
        last_epoch = self.get_last_epoch_processed()

        sql = """
            SELECT
                epoch,
                reputation
            FROM reputation
            WHERE
                address='%s'
            ORDER BY
                epoch ASC
        """ % self.address
        reputations = self.db_mngr.sql_return_all(sql)

        # Interpolate the reputation of the address
        interpolated_reputation = []
        if reputations:
            # First merge reputation differences with the same epoch
            merged_reputations = []
            for row in reputations:
                epoch, reputation = row
                if len(merged_reputations) == 0 or epoch != merged_reputations[-1][0]:
                    merged_reputations.append([epoch, reputation])
                else:
                    merged_reputations[-1][1] += reputation

            # Add zeros for the range from epoch one up to the first reputation gain
            for i in range(1, merged_reputations[0][0]):
                interpolated_reputation.append(0)

            # Sum reputation differences and interpolate all regions in between
            for index, reputation in enumerate(merged_reputations):
                interpolated_reputation.append(interpolated_reputation[-1] + reputation[1])
                if index < len(merged_reputations) - 1:
                    for i in range(reputation[0] + 1, merged_reputations[index + 1][0]):
                        interpolated_reputation.append(interpolated_reputation[-1])

            # Add zeros for the range from the last reputation gain epoch to the last observed epoch
            for i in range(merged_reputations[-1][0] + 1, last_epoch):
                interpolated_reputation.append(0)

        # Get the non-zero reputation regions
        reset = False
        non_zero_reputation, non_zero_reputation_regions = [], []
        for i, reputation in enumerate(interpolated_reputation):
            if reputation != 0:
                if not reset:
                    non_zero_reputation.append([])
                    non_zero_reputation_regions.append([i])
                reset = True
                non_zero_reputation[-1].append(reputation)
            if reset and reputation == 0:
                reset = False
                non_zero_reputation_regions[-1].append(i)

        return non_zero_reputation, non_zero_reputation_regions
