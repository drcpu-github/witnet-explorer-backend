import logging
import logging.handlers
import os
import sys

from util.database_manager import DatabaseManager


class WitnetDatabase(object):
    def __init__(
        self,
        db_config,
        named_cursor=False,
        logger=None,
        log_queue=None,
        log_label=None,
    ):
        # Set up logger
        if logger:
            self.logger = logger
        elif log_queue:
            self.configure_logging_process(log_queue, log_label)
            self.logger = logging.getLogger(log_label)
        else:
            self.logger = None

        self.db_mngr = DatabaseManager(
            db_config, named_cursor=named_cursor, logger=self.logger
        )

        # Register types created for this database
        self.register_types()

        # Create arrays for all insert and update operations
        self.hashes = []
        self.blocks = []
        self.mints = []
        self.value_transfers = []
        self.data_requests = []
        self.commits = []
        self.reveals = []
        self.tallies = []

        self.last_epoch = 0

    def register_types(self):
        self.db_mngr.register_type("utxo")
        self.db_mngr.register_type("filter")

    ###################################################
    #     Insert / update transactions and blocks     #
    ###################################################

    def insert_block(self, block_json):
        # Insert hash type
        self.hashes.append(
            (
                bytearray.fromhex(block_json["details"]["hash"]),
                "block",
                block_json["details"]["epoch"],
            )
        )

        # Insert block
        self.blocks.append(
            (
                bytearray.fromhex(block_json["details"]["hash"]),
                len(block_json["transactions"]["value_transfer"]),
                len(block_json["transactions"]["data_request"]),
                len(block_json["transactions"]["commit"]),
                len(block_json["transactions"]["reveal"]),
                len(block_json["transactions"]["tally"]),
                block_json["details"]["data_request_weight"],
                block_json["details"]["value_transfer_weight"],
                block_json["details"]["weight"],
                block_json["details"]["epoch"],
                block_json["tapi"],
                block_json["details"]["confirmed"],
            )
        )

    def insert_mint_txn(self, txn_details, epoch):
        # Insert hash type
        self.hashes.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                "mint_txn",
                epoch,
            )
        )

        # Insert transaction
        self.mints.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                txn_details["miner"],
                txn_details["output_addresses"],
                txn_details["output_values"],
                epoch,
            )
        )

    def insert_value_transfer_txn(self, txn_details, epoch):
        # Insert hash type
        self.hashes.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                "value_transfer_txn",
                epoch,
            )
        )

        # Insert transaction
        self.value_transfers.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                txn_details["input_addresses"],
                txn_details["input_values"],
                txn_details["input_utxos"],
                txn_details["output_addresses"],
                txn_details["output_values"],
                txn_details["timelocks"],
                txn_details["weight"],
                epoch,
            )
        )

    def insert_data_request_txn(self, txn_details, epoch):
        # Insert hash types
        self.hashes.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                "data_request_txn",
                epoch,
            )
        )

        # RAD bytes hashes can be duplicated in a single epoch, only insert them once
        RAD_bytes_hash = bytearray.fromhex(txn_details["RAD_bytes_hash"])
        if (RAD_bytes_hash, "RAD_bytes_hash", None) not in self.hashes:
            self.hashes.append(
                (
                    RAD_bytes_hash,
                    "RAD_bytes_hash",
                    None,
                )
            )

        # DRO bytes hashes can be duplicated in a single epoch, only insert them once
        DRO_bytes_hash = bytearray.fromhex(txn_details["DRO_bytes_hash"])
        if (DRO_bytes_hash, "DRO_bytes_hash", None) not in self.hashes:
            self.hashes.append(
                (
                    DRO_bytes_hash,
                    "DRO_bytes_hash",
                    None,
                )
            )

        self.data_requests.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                txn_details["input_addresses"],
                txn_details["input_values"],
                txn_details["input_utxos"],
                txn_details["output_address"],
                txn_details["output_value"],
                txn_details["witnesses"],
                txn_details["witness_reward"],
                txn_details["collateral"],
                txn_details["consensus_percentage"],
                txn_details["commit_and_reveal_fee"],
                txn_details["weight"],
                txn_details["kinds"],
                txn_details["urls"],
                txn_details["bodies"],
                txn_details["scripts"],
                txn_details["aggregate_filters"],
                txn_details["aggregate_reducer"],
                txn_details["tally_filters"],
                txn_details["tally_reducer"],
                RAD_bytes_hash,
                DRO_bytes_hash,
                epoch,
            )
        )

    def insert_commit_txn(self, txn_details, epoch):
        # Insert hash type
        self.hashes.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                "commit_txn",
                epoch,
            )
        )

        # Insert transaction
        self.commits.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                txn_details["address"],
                txn_details["input_values"],
                txn_details["input_utxos"],
                txn_details["output_value"],
                bytearray.fromhex(txn_details["data_request"]),
                epoch,
            )
        )

    def insert_reveal_txn(self, txn_details, epoch):
        # Insert hash type
        self.hashes.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                "reveal_txn",
                epoch,
            )
        )

        # Insert transaction
        self.reveals.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                txn_details["address"],
                bytearray.fromhex(txn_details["data_request"]),
                txn_details["reveal"],
                txn_details["success"],
                epoch,
            )
        )

    def insert_tally_txn(self, txn_details, epoch):
        # Insert hash type
        self.hashes.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                "tally_txn",
                epoch,
            )
        )

        # Insert tally transaction
        self.tallies.append(
            (
                bytearray.fromhex(txn_details["hash"]),
                txn_details["output_addresses"],
                txn_details["output_values"],
                bytearray.fromhex(txn_details["data_request"]),
                txn_details["error_addresses"],
                txn_details["liar_addresses"],
                txn_details["tally"],
                txn_details["success"],
                epoch,
            )
        )

    def insert_addresses(self, addresses):
        sql = """
            INSERT INTO addresses(
                address,
                active,
                block,
                mint,
                value_transfer,
                data_request,
                commit,
                reveal,
                tally
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ON CONSTRAINT
                addresses_pkey
            DO UPDATE SET
                active = EXCLUDED.active,
                block = addresses.block + EXCLUDED.block,
                mint = addresses.mint + EXCLUDED.mint,
                value_transfer = addresses.value_transfer + EXCLUDED.value_transfer,
                data_request = addresses.data_request + EXCLUDED.data_request,
                commit = addresses.commit + EXCLUDED.commit,
                reveal = addresses.reveal + EXCLUDED.reveal,
                tally = addresses.tally + EXCLUDED.tally
            WHERE
                addresses.active < EXCLUDED.active
        """
        self.db_mngr.sql_execute_many(sql, addresses)

    def finalize(self, epoch=-1):
        if epoch == -1:
            epoch = self.last_epoch
        else:
            self.last_epoch = epoch
        self.finalize_insert(epoch)

    def finalize_insert(self, epoch):
        # insert all hashes
        if len(self.hashes) > 0:
            sql = """
                INSERT INTO hashes (
                    hash,
                    type,
                    epoch
                ) VALUES (%s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    hashes_pkey
                DO UPDATE SET
                    epoch=EXCLUDED.epoch
            """
            self.db_mngr.sql_execute_many(sql, self.hashes)
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.hashes)} hashes for epoch {epoch}"
                )
        self.hashes = []

        # insert blocks
        if len(self.blocks) > 0:
            sql = """
                INSERT INTO blocks (
                    block_hash,
                    value_transfer,
                    data_request,
                    commit,
                    reveal,
                    tally,
                    dr_weight,
                    vt_weight,
                    block_weight,
                    epoch,
                    tapi_signals,
                    confirmed
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    blocks_pkey
                DO UPDATE SET
                    confirmed=EXCLUDED.confirmed
            """
            self.db_mngr.sql_execute_many(sql, self.blocks)
            if self.logger:
                self.logger.info(f"Inserted {len(self.blocks)} block for epoch {epoch}")
        self.blocks = []

        # insert mint transactions
        if len(self.mints) > 0:
            sql = """
                INSERT INTO mint_txns (
                    txn_hash,
                    miner,
                    output_addresses,
                    output_values,
                    epoch
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    mint_txns_pkey
                DO NOTHING
            """
            self.db_mngr.sql_execute_many(sql, self.mints)
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.mints)} mint transaction for epoch {epoch}"
                )
        self.mints = []

        # insert value transfer transactions
        if len(self.value_transfers) > 0:
            sql = """
                INSERT INTO value_transfer_txns (
                    txn_hash,
                    input_addresses,
                    input_values,
                    input_utxos,
                    output_addresses,
                    output_values,
                    timelocks,
                    weight,
                    epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    value_transfer_txns_pkey
                DO UPDATE SET
                    epoch=EXCLUDED.epoch
            """
            self.db_mngr.sql_execute_many(
                sql,
                self.value_transfers,
            )
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.value_transfers)} value transfer transaction(s) for epoch {epoch}"
                )
        self.value_transfers = []

        # insert data request transactions
        if len(self.data_requests) > 0:
            sql = """
                INSERT INTO data_request_txns (
                    txn_hash,
                    input_addresses,
                    input_values,
                    input_utxos,
                    output_address,
                    output_value,
                    witnesses,
                    witness_reward,
                    collateral,
                    consensus_percentage,
                    commit_and_reveal_fee,
                    weight,
                    kinds,
                    urls,
                    bodies,
                    scripts,
                    aggregate_filters,
                    aggregate_reducer,
                    tally_filters,
                    tally_reducer,
                    RAD_bytes_hash,
                    DRO_bytes_hash,
                    epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    data_request_txns_pkey
                DO UPDATE SET
                    epoch=EXCLUDED.epoch
            """
            self.db_mngr.sql_execute_many(
                sql,
                self.data_requests,
            )
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.data_requests)} data request transaction(s) for epoch {epoch}"
                )
        self.data_requests = []

        # insert commit transactions
        if len(self.commits) > 0:
            sql = """
                INSERT INTO commit_txns (
                    txn_hash,
                    txn_address,
                    input_values,
                    input_utxos,
                    output_value,
                    data_request,
                    epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    commit_txns_pkey
                DO NOTHING
            """
            self.db_mngr.sql_execute_many(
                sql, self.commits
            )
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.commits)} commit transaction(s) for epoch {epoch}"
                )
        self.commits = []

        # insert reveal transactions
        if len(self.reveals) > 0:
            sql = """
                INSERT INTO reveal_txns (
                    txn_hash,
                    txn_address,
                    data_request,
                    result,
                    success,
                    epoch
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    reveal_txns_pkey
                DO UPDATE SET
                    result=EXCLUDED.result,
                    success=EXCLUDED.success,
                    epoch=EXCLUDED.epoch
            """
            self.db_mngr.sql_execute_many(sql, self.reveals)
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.reveals)} reveal transaction(s) for epoch {epoch}"
                )
        self.reveals = []

        # insert tally transactions
        if len(self.tallies) > 0:
            sql = """
                INSERT INTO tally_txns (
                    txn_hash,
                    output_addresses,
                    output_values,
                    data_request,
                    error_addresses,
                    liar_addresses,
                    result,
                    success,
                    epoch
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT
                    tally_txns_pkey
                DO UPDATE SET
                    output_addresses=EXCLUDED.output_addresses,
                    output_values=EXCLUDED.output_values,
                    error_addresses=EXCLUDED.error_addresses,
                    liar_addresses=EXCLUDED.liar_addresses,
                    result=EXCLUDED.result,
                    success=EXCLUDED.success,
                    epoch=EXCLUDED.epoch
            """
            self.db_mngr.sql_execute_many(sql, self.tallies)
            if self.logger:
                self.logger.info(
                    f"Inserted {len(self.tallies)} tally transaction(s) for epoch {epoch}"
                )
        self.tallies = []

    def confirm_block(self, block_hash, epoch):
        sql = """
            UPDATE
                blocks
            SET
                confirmed=true
            WHERE
                block_hash=%s
        """
        result = self.db_mngr.sql_update_table(sql, parameters=[bytearray.fromhex(block_hash)])
        if self.logger:
            self.logger.info(f"Confirmed block {block_hash} for epoch {epoch}")

    def revert_block(self, block_hash, epoch):
        sql = """
            UPDATE
                blocks
            SET
                confirmed=false,
                reverted=true
            WHERE block_hash=%s
        """
        result = self.db_mngr.sql_update_table(sql, parameters=[bytearray.fromhex(block_hash)])
        if self.logger:
            self.logger.info(f"Reverted block {block_hash} for epoch {epoch}")

    def remove_block(self, block_hash, epoch):
        sql = """
            DELETE FROM
                blocks
            WHERE
                block_hash=%s
        """
        result = self.db_mngr.sql_update_table(sql, parameters=[bytearray.fromhex(block_hash)])
        if self.logger:
            self.logger.info(f"Deleted block {block_hash} for epoch {epoch}")

    #####################################################
    #       Create pending transactions histograms      #
    #####################################################

    def insert_pending_data_request_txns(self, timestamp, fee_per_unit, num_txns):
        if self.logger:
            self.logger.info(f"Inserting pending data requests at {timestamp}")
        sql = """
            INSERT INTO pending_data_request_txns (
                timestamp,
                fee_per_unit,
                num_txns
            ) VALUES (%s, %s, %s)
        """
        self.db_mngr.sql_insert_one(sql, (timestamp, fee_per_unit, num_txns))

    def insert_pending_value_transfer_txns(self, timestamp, fee_per_unit, num_txns):
        if self.logger:
            self.logger.info(f"Inserting pending value transfers at {timestamp}")
        sql = """
            INSERT INTO pending_value_transfer_txns (
                timestamp,
                fee_per_unit,
                num_txns
            ) VALUES (%s, %s, %s)
        """
        self.db_mngr.sql_insert_one(sql, (timestamp, fee_per_unit, num_txns))

    #####################################################
    #                  Helper functions                 #
    #####################################################

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def terminate(self):
        self.finalize()
        self.db_mngr.terminate()

    def sql_return_one(self, sql):
        result = self.db_mngr.sql_return_one(sql)
        return result

    def sql_return_all(self, sql):
        result = self.db_mngr.sql_return_all(sql)
        return result

    def sql_execute_many(self, sql, data):
        self.db_mngr.sql_execute_many(sql, data)
