import time

from psycopg.sql import SQL, Literal

from blockchain.transactions.reveal import translate_reveal
from blockchain.transactions.tally import translate_tally
from node.witnet_node import WitnetNode
from util.blockchain_functions import (
    calculate_block_reward,
    calculate_timestamp_from_epoch,
)
from util.data_transformer import re_sql
from util.database_manager import DatabaseManager


class Address(object):
    def __init__(
        self,
        address,
        database=None,
        witnet_node=None,
        logger=None,
        connect=True,
    ):
        # Set address
        self.address = address.strip()

        # Initialize database manager if provided
        self.db_mngr = None
        if database:
            self.db_mngr = database

        # Initialize the witnet node if provided
        self.witnet_node = None
        if witnet_node:
            self.witnet_node = witnet_node

        # Create logger
        if logger:
            self.logger = logger
        else:
            self.logger = None

        # Finish connecting to database, witnet_node and get the consensus constants
        # Do not automatically initialize when the address object is used from the caching server
        if connect:
            self.initialize_connections()

    def initialize_connections(self):
        # Connect to the database if necessary
        if self.db_mngr is None:
            self.db_mngr = DatabaseManager(named_cursor=False, logger=self.logger)

        # Connect to node pool
        if self.witnet_node is None:
            self.witnet_node = WitnetNode(logger=self.logger)

    def close_connections(self):
        self.db_mngr.terminate()
        self.witnet_node.close_connection()

    def get_details(self):
        # Get balance
        balance = self.witnet_node.get_balance(self.address)
        if type(balance) is dict and "error" in balance:
            balance = "Could not retrieve balance"
        else:
            balance = balance["result"]["total"]

        # Get staked amounts
        staked_validator = self.witnet_node.get_stakes(self.address, None)
        if type(staked_validator) is dict and "reason" in staked_validator:
            if (
                staked_validator["reason"]
                == f"Tried to query for a stake entry by validator ({self.address}) that is not registered in Stakes"
            ):
                staked_validator = 0
            else:
                staked_validator = "Could not retrieve validator staked balance"
        else:
            staked_validator = sum(
                [stake["value"]["coins"] for stake in staked_validator["result"]]
            )

        staked_withdrawer = self.witnet_node.get_stakes(None, self.address)
        if type(staked_withdrawer) is dict and "reason" in staked_withdrawer:
            if (
                staked_withdrawer["reason"]
                == f"Tried to query for a stake entry by withdrawer ({self.address}) that is not registered in Stakes"
            ):
                staked_withdrawer = 0
            else:
                staked_withdrawer = "Could not retrieve withdrawer staked balance"
        else:
            staked_withdrawer = sum(
                [stake["value"]["coins"] for stake in staked_withdrawer["result"]]
            )

        # Get label
        label = ""
        sql = (
            """
            SELECT
                label
            FROM
                addresses
            WHERE
                address='%s'
        """
            % self.address
        )
        result = self.db_mngr.sql_return_one(sql)
        if result:
            label = result[0]

        return {
            "balance": balance,
            "staked_validator": staked_validator,
            "staked_withdrawer": staked_withdrawer,
            "label": label,
        }

    def get_value_transfers(self):
        value_transfers = []
        value_transfers.extend(self.get_value_transfers_in())
        value_transfers.extend(self.get_value_transfers_out())
        return sorted(
            value_transfers,
            key=lambda vt: (vt["epoch"], vt["hash"]),
            reverse=True,
        )

    def get_value_transfers_in(self):
        # get value transfers arriving at our address
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
                blocks.confirmed
            FROM value_transfer_txns
            LEFT JOIN blocks ON
                value_transfer_txns.epoch=blocks.epoch
            WHERE
                output_addresses @> ARRAY[%s]::CHAR({length})[] AND
                NOT (%s = ANY(input_addresses))
            ORDER BY
                blocks.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(
            SQL(re_sql(sql)).format(length=Literal(len(self.address))),
            parameters=[self.address, self.address],
        )

        value_transfers_in = []
        if result:
            for value_transfer in result:
                (
                    txn_hash,
                    input_addresses,
                    input_values,
                    output_addresses,
                    output_values,
                    timelocks,
                    weight,
                    txn_epoch,
                    block_confirmed,
                ) = value_transfer

                total_value = 0
                for output_address, output_value in zip(
                    output_addresses, output_values
                ):
                    if output_address == self.address:
                        total_value += output_value

                fee = (
                    sum(input_values) - sum(output_values)
                    if len(input_values) > 0
                    else 0
                )

                priority = max(1, int(fee / weight))

                # Only account for timelocks if any of the output_address are self.address
                now = int(time.time())
                locked = any(
                    [
                        output_address == self.address and timelock > now
                        for output_address, timelock in zip(output_addresses, timelocks)
                    ]
                )

                value_transfers_in.append(
                    {
                        "hash": txn_hash.hex(),
                        "epoch": txn_epoch,
                        "timestamp": calculate_timestamp_from_epoch(txn_epoch),
                        "direction": "in",
                        "input_addresses": sorted(list(set(input_addresses))),
                        "output_addresses": sorted(list(set(output_addresses))),
                        "value": total_value,
                        "fee": fee,
                        "priority": priority,
                        "weight": weight,
                        "locked": locked,
                        "confirmed": block_confirmed,
                    }
                )

        return value_transfers_in

    def get_value_transfers_out(self):
        # get value transfers starting at our address
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
                blocks.confirmed
            FROM value_transfer_txns
            LEFT JOIN blocks ON
                value_transfer_txns.epoch=blocks.epoch
            WHERE
                input_addresses @> ARRAY[%s]::CHAR({length})[]
            ORDER BY
                blocks.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(
            SQL(re_sql(sql)).format(length=Literal(len(self.address))),
            parameters=[self.address],
        )

        value_transfers_out = []
        if result:
            for value_transfer in result:
                (
                    txn_hash,
                    input_addresses,
                    input_values,
                    output_addresses,
                    output_values,
                    timelocks,
                    weight,
                    txn_epoch,
                    block_confirmed,
                ) = value_transfer

                total_value = 0
                for output_address, output_value in zip(
                    output_addresses, output_values
                ):
                    # Discount change output
                    if output_address != self.address:
                        total_value += output_value

                unique_output_addresses = list(
                    set(output_addresses) - set([self.address])
                )
                # Transaction with multiple output_addresses different from the source address
                if len(unique_output_addresses) > 1:
                    direction = "out"
                    output_addresses = unique_output_addresses
                else:
                    # Split or merge UTXO transaction where the output_address address is also the source address
                    if len(unique_output_addresses) == 0:
                        direction = "self"
                        output_addresses = [self.address]
                    # Transaction with a output_address different from the source address
                    else:
                        direction = "out"
                        output_addresses = unique_output_addresses

                fee = sum(input_values) - sum(output_values)

                priority = max(1, int(fee / weight))

                # Account for all timelocks
                now = int(time.time())
                locked = any([timelock > now for timelock in timelocks])

                value_transfers_out.append(
                    {
                        "hash": txn_hash.hex(),
                        "epoch": txn_epoch,
                        "timestamp": calculate_timestamp_from_epoch(txn_epoch),
                        "direction": direction,
                        "input_addresses": sorted(list(set(input_addresses))),
                        "output_addresses": sorted(output_addresses),
                        "value": total_value,
                        "fee": fee,
                        "priority": priority,
                        "weight": weight,
                        "locked": locked,
                        "confirmed": block_confirmed,
                    }
                )

        return value_transfers_out

    def get_blocks(self):
        sql = """
            SELECT
                blocks.block_hash,
                blocks.value_transfer,
                blocks.data_request,
                blocks.commit,
                blocks.reveal,
                blocks.tally,
                blocks.stake,
                blocks.unstake,
                blocks.txns_fees,
                blocks.epoch,
                blocks.confirmed
            FROM
                blocks
            LEFT JOIN mint_txns ON
                mint_txns.epoch=blocks.epoch
            WHERE
                mint_txns.miner=%s
            ORDER BY
                blocks.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(sql, parameters=[self.address])

        blocks_minted = []
        if result:
            for block in result:
                (
                    block_hash,
                    value_transfers,
                    data_requests,
                    commits,
                    reveals,
                    tallies,
                    stakes,
                    unstakes,
                    txns_fees,
                    block_epoch,
                    block_confirmed,
                ) = block

                blocks_minted.append(
                    {
                        "hash": block_hash.hex(),
                        "miner": self.address,
                        "timestamp": calculate_timestamp_from_epoch(block_epoch),
                        "epoch": block_epoch,
                        "block_reward": calculate_block_reward(block_epoch) + txns_fees,
                        "block_fees": txns_fees,
                        "value_transfers": value_transfers,
                        "data_requests": data_requests,
                        "commits": commits,
                        "reveals": reveals,
                        "tallies": tallies,
                        "stakes": stakes,
                        "unstakes": unstakes,
                        "confirmed": block_confirmed,
                    }
                )

        return blocks_minted

    def get_mints(self):
        sql = """
            SELECT
                mint_txns.txn_hash,
                mint_txns.miner,
                mint_txns.output_addresses,
                mint_txns.output_values,
                mint_txns.epoch,
                blocks.confirmed
            FROM
                mint_txns
            LEFT JOIN blocks ON
                mint_txns.epoch=blocks.epoch
            WHERE
                mint_txns.output_addresses @> ARRAY[%s]::CHAR({length})[]
            ORDER BY
                mint_txns.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(
            SQL(re_sql(sql)).format(length=Literal(len(self.address))),
            parameters=[self.address],
        )

        mints = []
        if result:
            for mint in result:
                (
                    txn_hash,
                    miner,
                    output_addresses,
                    output_values,
                    epoch,
                    confirmed,
                ) = mint

                value = 0
                for output_address, output_value in zip(
                    output_addresses, output_values
                ):
                    if output_address == self.address:
                        value = output_value

                mints.append(
                    {
                        "hash": txn_hash.hex(),
                        "epoch": epoch,
                        "timestamp": calculate_timestamp_from_epoch(epoch),
                        "miner": miner,
                        "output_value": value,
                        "confirmed": confirmed,
                    }
                )

        return mints

    def get_data_requests_solved(self):
        sql = """
            SELECT
                data_request_txns.collateral,
                data_request_txns.witness_reward,
                commit_txns.data_request,
                reveal_txns.txn_hash,
                reveal_txns.result,
                tally_txns.epoch,
                tally_txns.error_addresses,
                tally_txns.liar_addresses,
                tally_txns.success
            FROM
                commit_txns
            LEFT JOIN
                data_request_txns
            ON
                commit_txns.data_request=data_request_txns.txn_hash
            LEFT JOIN
                reveal_txns
            ON
                commit_txns.data_request=reveal_txns.data_request
            AND
                commit_txns.txn_address=reveal_txns.txn_address
            LEFT JOIN
                tally_txns
            ON
                commit_txns.data_request=tally_txns.data_request
            LEFT JOIN
                blocks
            ON
                tally_txns.epoch=blocks.epoch
            WHERE
                commit_txns.txn_address=%s
            AND
                blocks.reverted=false
            AND
                tally_txns.success IS NOT NULL
            ORDER BY
                tally_txns.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(sql, parameters=[self.address])

        data_requests_solved = []
        if result:
            for data_request in result:
                (
                    collateral,
                    witness_reward,
                    data_request_hash,
                    reveal_txn_hash,
                    reveal_value,
                    tally_epoch,
                    error_addresses,
                    liar_addresses,
                    success,
                ) = data_request

                # Translate reveal value
                if reveal_value:
                    _, translated_reveal = translate_reveal(
                        reveal_txn_hash, reveal_value
                    )
                else:
                    translated_reveal = ""

                # Check if we were marked as an error revealer
                error = self.address in error_addresses

                # Check if we were marked as a liar
                liar = self.address in liar_addresses

                data_requests_solved.append(
                    {
                        "hash": data_request_hash.hex(),
                        "success": success,
                        "epoch": tally_epoch,
                        "timestamp": calculate_timestamp_from_epoch(tally_epoch),
                        "collateral": collateral,
                        "witness_reward": witness_reward,
                        "reveal": translated_reveal,
                        "error": error,
                        "liar": liar,
                    }
                )

        return data_requests_solved

    def get_data_requests_created(self):
        sql = """
            SELECT
                data_request_txns.txn_hash,
                data_request_txns.input_values,
                data_request_txns.output_value,
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
                data_request_txns.input_addresses @> ARRAY[%s]::CHAR({length})[]
            ORDER BY
                tally_txns.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(
            SQL(re_sql(sql)).format(length=Literal(len(self.address))),
            parameters=[self.address],
        )

        data_requests_created = []
        if result:
            # Loop and process
            for data_request in result:
                (
                    data_request_hash,
                    input_values,
                    output_value,
                    witnesses,
                    collateral,
                    consensus_percentage,
                    tally_txn_hash,
                    tally_epoch,
                    error_addresses,
                    liar_addresses,
                    result,
                    success,
                    block_reverted,
                ) = data_request

                # Check if any of the values is None which would indicate this data request was not completed (or incorrectly processed)
                if any(dr is None for dr in data_request):
                    continue

                # Calculate total fee of the data request (witnesses * witness_reward + mining fees per transaction)
                # Note that this is the sum of the DRO and miner fees to display how much that data request payed in total
                total_fee = sum(input_values) - output_value

                # Count the total number of error committers and liar_addresses
                num_errors = len(error_addresses)
                num_liars = len(liar_addresses)

                # Translate the cbor-encoded tally result
                _, translated_tally = translate_tally(tally_txn_hash, result)

                # Check if the tally transaction was reverted or there was an error
                success = success and not block_reverted

                data_requests_created.append(
                    {
                        "hash": data_request_hash.hex(),
                        "success": success,
                        "epoch": tally_epoch,
                        "timestamp": calculate_timestamp_from_epoch(tally_epoch),
                        "total_fee": total_fee,
                        "witnesses": witnesses,
                        "collateral": collateral,
                        "consensus_percentage": consensus_percentage,
                        "num_errors": num_errors,
                        "num_liars": num_liars,
                        "result": translated_tally,
                    }
                )

        return data_requests_created

    def get_stakes(self):
        sql = """
            SELECT
                stake_txns.txn_hash,
                stake_txns.epoch,
                stake_txns.input_addresses,
                stake_txns.input_values,
                stake_txns.change_value,
                stake_txns.validator,
                stake_txns.withdrawer,
                stake_txns.stake_value,
                blocks.confirmed
            FROM
                stake_txns
            LEFT JOIN blocks ON
                stake_txns.epoch=blocks.epoch
            WHERE
                stake_txns.input_addresses @> ARRAY[%s]::CHAR({length})[]
            OR
                stake_txns.validator=%s
            OR
                stake_txns.withdrawer=%s
            ORDER BY
                stake_txns.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(
            SQL(re_sql(sql)).format(length=Literal(len(self.address))),
            parameters=[self.address, self.address, self.address],
        )

        stakes = []
        if result:
            for stake in result:
                (
                    txn_hash,
                    epoch,
                    input_addresses,
                    input_values,
                    change_value,
                    validator,
                    withdrawer,
                    stake_value,
                    confirmed,
                ) = stake

                fee = sum(input_values) - stake_value - (change_value or 0)

                # Add all inputs from this address (if any)
                value = 0
                for input_address, input_value in zip(input_addresses, input_values):
                    if input_address == self.address:
                        value += input_value

                if validator == self.address:
                    if validator in input_addresses:
                        direction = "self"
                    else:
                        direction = "in"
                elif withdrawer == self.address:
                    direction = "in"
                else:
                    direction = "out"

                stakes.append(
                    {
                        "hash": txn_hash.hex(),
                        "epoch": epoch,
                        "timestamp": calculate_timestamp_from_epoch(epoch),
                        "direction": direction,
                        "validator": validator,
                        "withdrawer": withdrawer,
                        "input_value": value,
                        "fee": fee,
                        "stake_value": stake_value,
                        "confirmed": confirmed,
                    }
                )

        return stakes

    def get_unstakes(self):
        sql = """
            SELECT
                unstake_txns.txn_hash,
                unstake_txns.epoch,
                unstake_txns.validator,
                unstake_txns.withdrawer,
                unstake_txns.fee,
                unstake_txns.unstake_value,
                blocks.confirmed
            FROM
                unstake_txns
            LEFT JOIN blocks ON
                unstake_txns.epoch=blocks.epoch
            WHERE
                unstake_txns.validator=%s
            OR
                unstake_txns.withdrawer=%s
            ORDER BY
                unstake_txns.epoch
            DESC
        """
        result = self.db_mngr.sql_return_all(
            sql, parameters=[self.address, self.address]
        )

        unstakes = []
        if result:
            for unstake in result:
                (
                    txn_hash,
                    epoch,
                    validator,
                    withdrawer,
                    fee,
                    unstake_value,
                    confirmed,
                ) = unstake

                if validator == self.address:
                    if validator == withdrawer:
                        direction = "self"
                    else:
                        direction = "out"
                else:
                    direction = "in"

                unstakes.append(
                    {
                        "hash": txn_hash.hex(),
                        "epoch": epoch,
                        "timestamp": calculate_timestamp_from_epoch(epoch),
                        "direction": direction,
                        "validator": validator,
                        "withdrawer": withdrawer,
                        "fee": fee,
                        "unstake_value": unstake_value,
                        "confirmed": confirmed,
                    }
                )

        return unstakes

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

        sql = (
            """
            SELECT
                epoch,
                reputation
            FROM reputation
            WHERE
                address='%s'
            ORDER BY
                epoch ASC
        """
            % self.address
        )
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
            for _ in range(1, merged_reputations[0][0]):
                interpolated_reputation.append(0)

            # Sum reputation differences and interpolate all regions in between
            for index, reputation in enumerate(merged_reputations):
                interpolated_reputation.append(
                    interpolated_reputation[-1] + reputation[1]
                )
                if index < len(merged_reputations) - 1:
                    for _ in range(reputation[0] + 1, merged_reputations[index + 1][0]):
                        interpolated_reputation.append(interpolated_reputation[-1])

            # Add zeros for the range from the last reputation gain epoch to the last observed epoch
            for _ in range(merged_reputations[-1][0] + 1, last_epoch):
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

    def get_utxos(self):
        utxos = self.witnet_node.get_utxos(self.address)
        return utxos
