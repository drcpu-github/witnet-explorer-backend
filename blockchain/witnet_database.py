import logging
import logging.handlers
import os
import psycopg2
import sys

from blockchain.database_manager import DatabaseManager

class WitnetDatabase(object):
    def __init__(self, db_user, db_name, db_pass, logging_queue, log_label):
        # Set up logger
        self.configure_logging_process(logging_queue, log_label)
        self.logger = logging.getLogger(log_label)

        self.db_mngr = DatabaseManager(db_user, db_name, db_pass, self.logger)

        # Create all database tables if they do not exist yet
        self.create_tables_and_indexes()

        # Register types created for this database
        self.register_types()

        # Create arrays for all insert and update operations
        self.insert_hashes, self.update_hashes = [], []
        self.insert_blocks, self.update_blocks = [], []
        self.insert_mint_txns = []
        self.insert_value_transfer_txns, self.update_value_transfer_txns = [], []
        self.insert_data_request_txns, self.update_data_request_txns = [], []
        self.insert_commit_txns = []
        self.insert_reveal_txns, self.update_reveal_txns = [], []
        self.insert_tally_txns, self.update_tally_txns = [], []

        self.last_epoch = 0

    #################################################
    #       Create tables, indexes and types        #
    #################################################

    def create_tables_and_indexes(self):
        tables_indexes_and_enums = [
            """CREATE TABLE IF NOT EXISTS addresses (
                address CHAR(42) PRIMARY KEY,
                label VARCHAR(64) NOT NULL
            );""",

            """DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'hash_type') THEN
                        CREATE TYPE hash_type AS ENUM (
                            'block',
                            'mint_txn',
                            'value_transfer_txn',
                            'data_request_txn',
                            'RAD_bytes_hash',
                            'data_request_bytes_hash',
                            'commit_txn',
                            'reveal_txn',
                            'tally_txn'
                        );
                    END IF;
                END
            $$;
            COMMIT;""",

            """CREATE TABLE IF NOT EXISTS hashes (
                hash BYTEA PRIMARY KEY,
                type hash_type NOT NULL,
                epoch INT
            );""",

            """CREATE TABLE IF NOT EXISTS blocks (
                block_hash BYTEA PRIMARY KEY,
                value_transfer SMALLINT NOT NULL,
                data_request SMALLINT NOT NULL,
                commit SMALLINT NOT NULL,
                reveal SMALLINT NOT NULL,
                tally SMALLINT NOT NULL,
                epoch INT NOT NULL,
                tapi_accept BOOLEAN,
                confirmed BOOLEAN NOT NULL,
                reverted BOOLEAN DEFAULT false
            );""",

            """CREATE TABLE IF NOT EXISTS mint_txns (
                txn_hash BYTEA PRIMARY KEY,
                miner CHAR(42) NOT NULL,
                output_addresses CHAR(42) ARRAY NOT NULL,
                output_values BIGINT ARRAY NOT NULL,
                epoch INT NOT NULL
            );""",

            """DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'utxo') THEN
                        CREATE TYPE utxo AS (
                            transaction BYTEA,
                            idx SMALLINT
                        );
                    END IF;
                END
            $$;
            COMMIT;""",

            """CREATE TABLE IF NOT EXISTS value_transfer_txns (
                txn_hash BYTEA PRIMARY KEY,
                input_addresses CHAR(42) ARRAY NOT NULL,
                input_values BIGINT ARRAY NOT NULL,
                input_utxos utxo ARRAY NOT NULL,
                output_addresses CHAR(42) ARRAY NOT NULL,
                output_values BIGINT ARRAY NOT NULL,
                timelocks BIGINT ARRAY NOT NULL,
                weight INT NOT NULL,
                epoch INT NOT NULL
            );""",

            """DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'kind') THEN
                        CREATE TYPE kind AS ENUM (
                            'Unknown',
                            'HTTP-GET',
                            'RNG'
                        );
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'filter') THEN
                        CREATE TYPE filter AS (
                            type SMALLINT,
                            args BYTEA
                        );
                    END IF;
                END
            $$;
            COMMIT;""",

            """CREATE TABLE IF NOT EXISTS data_request_txns (
                txn_hash BYTEA PRIMARY KEY,
                txn_kind kind NOT NULL,
                input_addresses CHAR(42) ARRAY NOT NULL,
                input_values BIGINT ARRAY NOT NULL,
                input_utxos utxo ARRAY NOT NULL,
                output_addresses CHAR(42) ARRAY NOT NULL,
                output_values BIGINT ARRAY NOT NULL,
                witnesses SMALLINT NOT NULL,
                witness_reward BIGINT NOT NULL,
                collateral BIGINT NOT NULL,
                consensus_percentage SMALLINT NOT NULL,
                commit_and_reveal_fee BIGINT NOT NULL,
                weight INT NOT NULL,
                urls VARCHAR ARRAY,
                scripts BYTEA ARRAY,
                aggregate_filters filter ARRAY NOT NULL,
                aggregate_reducer INT ARRAY NOT NULL,
                tally_filters filter ARRAY NOT NULL,
                tally_reducer INT ARRAY NOT NULL,
                RAD_bytes_hash BYTEA NOT NULL,
                data_request_bytes_hash BYTEA NOT NULL,
                epoch INT NOT NULL
            );""",

            """CREATE TABLE IF NOT EXISTS commit_txns (
                txn_hash BYTEA PRIMARY KEY,
                txn_address CHAR(42) NOT NULL,
                input_values BIGINT ARRAY NOT NULL,
                input_utxos utxo ARRAY NOT NULL,
                output_values BIGINT ARRAY NOT NULL,
                data_request_txn_hash BYTEA NOT NULL,
                epoch INT NOT NULL
            );""",
            "CREATE INDEX IF NOT EXISTS idx_commit_txn_address ON commit_txns (txn_address);",
            "CREATE INDEX IF NOT EXISTS idx_commit_dr_hash ON commit_txns (data_request_txn_hash);",

            """CREATE TABLE IF NOT EXISTS reveal_txns (
                txn_hash BYTEA PRIMARY KEY,
                txn_address CHAR(42) NOT NULL,
                data_request_txn_hash BYTEA NOT NULL,
                result BYTEA NOT NULL,
                success BOOL NOT NULL,
                epoch INT NOT NULL
            );""",
            "CREATE INDEX IF NOT EXISTS idx_reveal_txn_address ON reveal_txns (txn_address);",
            "CREATE INDEX IF NOT EXISTS idx_reveal_dr_hash ON reveal_txns (data_request_txn_hash);",

            """CREATE TABLE IF NOT EXISTS tally_txns (
                txn_hash BYTEA PRIMARY KEY,
                output_values BIGINT ARRAY NOT NULL,
                data_request_txn_hash BYTEA NOT NULL,
                error_addresses CHAR(42) ARRAY NOT NULL,
                liar_addresses CHAR(42) ARRAY NOT NULL,
                result BYTEA NOT NULL,
                success BOOL NOT NULL,
                epoch INT NOT NULL
            );""",

            """CREATE TABLE IF NOT EXISTS pending_data_request_txns (
                timestamp INT NOT NULL,
                fee_per_unit INT ARRAY NOT NULL,
                num_txns INT ARRAY NOT NULL
            );""",

            """CREATE TABLE IF NOT EXISTS pending_value_transfer_txns (
                timestamp INT NOT NULL,
                fee_per_unit INT ARRAY NOT NULL,
                num_txns INT ARRAY NOT NULL
            );""",

            """CREATE TABLE IF NOT EXISTS tapi (
                id SMALLSERIAL,
                title VARCHAR NOT NULL,
                description VARCHAR NOT NULL,
                start_epoch INT NOT NULL,
                stop_epoch INT NOT NULL,
                bit SMALLINT NOT NULL,
                urls VARCHAR ARRAY NOT NULL
            );"""
        ]

        self.db_mngr.execute_create_statements(tables_indexes_and_enums)

    def register_types(self):
        self.db_mngr.register_type("utxo")
        self.db_mngr.register_type("filter")

    ###################################################
    #     Insert / update transactions and blocks     #
    ###################################################

    def insert_block(self, block_json):
        block_hash = bytearray.fromhex(block_json["details"]["block_hash"])
        epoch = block_json["details"]["epoch"]
        confirmed = block_json["details"]["confirmed"]

        # Check if the block hash exists
        # If it does, do not create an insert operation
        if not self.check_hash(block_hash):
            # Insert hash type
            self.insert_hashes.append((
                block_hash,
                "block",
                epoch,
            ))

            # Insert block
            self.insert_blocks.append((
                block_hash,
                len(block_json["value_transfer_txns"]),
                len(block_json["data_request_txns"]),
                len(block_json["commit_txns"]),
                len(block_json["reveal_txns"]),
                len(block_json["tally_txns"]),
                epoch,
                block_json["tapi_accept"],
                confirmed,
            ))
        # If it does, generate an update statement
        else:
            # Update the confirmed status of the block
            self.update_blocks.append((
                block_hash,
                confirmed,
            ))

    def insert_mint_txn(self, txn_details, epoch):
        txn_hash = bytearray.fromhex(txn_details["txn_hash"])
        # Check if the mint txn hash exists
        # If it does, ignore the insert operation
        if not self.check_hash(txn_hash):
            # Insert hash type
            self.insert_hashes.append((
                txn_hash,
                "mint_txn",
                epoch,
            ))

            # Insert transaction
            self.insert_mint_txns.append((
                txn_hash,
                txn_details["miner"],
                txn_details["output_addresses"],
                txn_details["output_values"],
                epoch,
            ))
        # Nothing to do if we see a mint transaction with a hash we already inserted

    def insert_value_transfer_txn(self, txn_details, epoch):
        txn_hash = bytearray.fromhex(txn_details["txn_hash"])
        # Check if value transfer txn exists
        # If it does not, generate an insert statement
        if not self.check_hash(txn_hash):
            # Insert hash type
            self.insert_hashes.append((
                txn_hash,
                "value_transfer_txn",
                epoch,
            ))

            # Insert transaction
            self.insert_value_transfer_txns.append((
                txn_hash,
                txn_details["input_addresses"],
                txn_details["input_values"],
                txn_details["input_utxos"],
                txn_details["output_addresses"],
                txn_details["output_values"],
                txn_details["timelocks"],
                txn_details["weight"],
                epoch,
            ))
        # If it does, generate an update statement
        else:
            # Update epoch only for value transfer transactions that are restarted
            self.update_hashes.append((
                txn_hash,
                epoch,
            ))
            self.update_value_transfer_txns.append((
                txn_hash,
                epoch,
            ))

    def insert_data_request_txn(self, txn_details, epoch):
        txn_hash = bytearray.fromhex(txn_details["txn_hash"])
        RAD_bytes_hash = bytearray.fromhex(txn_details["RAD_bytes_hash"])
        data_request_bytes_hash = bytearray.fromhex(txn_details["data_request_bytes_hash"])

        # Check if data request txn exists
        # If it does not, generate an insert statement
        if not self.check_hash(txn_hash):
            # Insert hash type
            self.insert_hashes.append((
                txn_hash,
                "data_request_txn",
                epoch,
            ))

            self.insert_data_request_txns.append((
                txn_hash,
                txn_details["txn_kind"],
                txn_details["input_addresses"],
                txn_details["input_values"],
                txn_details["input_utxos"],
                txn_details["output_addresses"],
                txn_details["output_values"],
                txn_details["witnesses"],
                txn_details["witness_reward"],
                txn_details["collateral"],
                txn_details["consensus_percentage"],
                txn_details["commit_and_reveal_fee"],
                txn_details["weight"],
                txn_details["urls"],
                txn_details["scripts"],
                txn_details["aggregate_filters"],
                txn_details["aggregate_reducer"],
                txn_details["tally_filters"],
                txn_details["tally_reducer"],
                RAD_bytes_hash,
                data_request_bytes_hash,
                epoch,
            ))
        # If it does, generate an update statement
        else:
            # Update epoch only for data request transactions that are restarted
            self.update_hashes.append((
                txn_hash,
                epoch,
            ))
            self.update_data_request_txns.append((
                txn_hash,
                epoch,
            ))

        # Check if the RAD bytes hash exists
        # If it does not, generate an insert statement
        if not self.check_hash(RAD_bytes_hash):
            # Insert RAD bytes hash
            self.insert_hashes.append((
                RAD_bytes_hash,
                "RAD_bytes_hash",
                None,
            ))

        # Check if the data request bytes hash exists
        # If it does not, generate an insert statement
        if not self.check_hash(data_request_bytes_hash):
            # Insert data request bytes hash
            self.insert_hashes.append((
                data_request_bytes_hash,
                "data_request_bytes_hash",
                None,
            ))

    def insert_commit_txn(self, txn_details, epoch):
        txn_hash = bytearray.fromhex(txn_details["txn_hash"])
        # Check if commit txn exists
        # If it does not, generate an insert statement
        if not self.check_hash(txn_hash):
            # Insert hash type
            self.insert_hashes.append((
                txn_hash,
                "commit_txn",
                epoch,
            ))

            # Insert transaction
            self.insert_commit_txns.append((
                txn_hash,
                txn_details["txn_address"],
                txn_details["input_values"],
                txn_details["input_utxos"],
                txn_details["output_values"],
                bytearray.fromhex(txn_details["data_request_txn_hash"]),
                epoch,
            ))
        # Nothing to do if we see a commit transaction with a hash we already inserted

    def insert_reveal_txn(self, txn_details, epoch):
        txn_hash = bytearray.fromhex(txn_details["txn_hash"])
        # Check if reveal txn exists
        # If it does not, generate an insert statement
        if not self.check_hash(txn_hash):
            # Insert hash type
            self.insert_hashes.append((
                txn_hash,
                "reveal_txn",
                epoch,
            ))

            # Insert transaction
            self.insert_reveal_txns.append((
                txn_hash,
                txn_details["txn_address"],
                bytearray.fromhex(txn_details["data_request_txn_hash"]),
                txn_details["reveal_value"],
                txn_details["success"],
                epoch,
            ))
        # If it does, generate an update statement
        else:
            # Update epoch only
            self.update_hashes.append((
                txn_hash,
                epoch,
            ))

            # The hash of a reveal transaction is not unique to an epoch
            # If they are restarted and updated (due to a rollback), the resulting value may have been updated too
            self.update_reveal_txns.append((
                txn_hash,
                txn_details["reveal_value"],
                txn_details["success"],
                epoch,
            ))

    def insert_tally_txn(self, txn_details, epoch):
        txn_hash = bytearray.fromhex(txn_details["txn_hash"])
        # Check if tally txn exists
        # If it does not, generate an insert statement
        if not self.check_hash(txn_hash):
            # Insert hash type
            self.insert_hashes.append((
                txn_hash,
                "tally_txn",
                epoch,
            ))

            # Insert tally transaction
            self.insert_tally_txns.append((
                txn_hash,
                txn_details["output_values"],
                bytearray.fromhex(txn_details["data_request_txn_hash"]),
                txn_details["error_addresses"],
                txn_details["liar_addresses"],
                txn_details["tally_value"],
                txn_details["success"],
                epoch,
            ))
        # If it does, generate an update statement
        else:
            # Update epoch status only
            self.update_hashes.append((
                txn_hash,
                epoch,
            ))

            # Update fields that may have changed when a tally transaction was rolled back and restarted
            self.update_tally_txns.append((
                txn_hash,
                txn_details["output_values"],
                txn_details["error_addresses"],
                txn_details["liar_addresses"],
                txn_details["tally_value"],
                txn_details["success"],
                epoch,
            ))

    def finalize(self, epoch=-1):
        if epoch == -1:
            epoch = self.last_epoch
        else:
            self.last_epoch = epoch
        self.finalize_insert(epoch)
        self.finalize_update(epoch)

    def finalize_insert(self, epoch):
        # insert all hashes
        if len(self.insert_hashes) > 0:
            sql = """
                INSERT INTO hashes (
                    hash,
                    type,
                    epoch
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_hashes)
            self.logger.info(f"Inserted {len(self.insert_hashes)} hashes for epoch {epoch}")
        self.insert_hashes = []

        # insert blocks
        if len(self.insert_blocks) > 0:
            sql = """
                INSERT INTO blocks (
                    block_hash,
                    value_transfer,
                    data_request,
                    commit,
                    reveal,
                    tally,
                    epoch,
                    tapi_accept,
                    confirmed
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_blocks)
            self.logger.info(f"Inserted {len(self.insert_blocks)} block for epoch {epoch}")
        self.insert_blocks = []

        # insert mint transactions
        if len(self.insert_mint_txns) > 0:
            sql = """
                INSERT INTO mint_txns (
                    txn_hash,
                    miner,
                    output_addresses,
                    output_values,
                    epoch
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_mint_txns)
            self.logger.info(f"Inserted {len(self.insert_mint_txns)} mint transaction for epoch {epoch}")
        self.insert_mint_txns = []

        # insert value transfer transactions
        if len(self.insert_value_transfer_txns) > 0:
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
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_value_transfer_txns, template="(%s, %s::CHAR(42)[], %s, %s::utxo[], %s::CHAR(42)[], %s, %s, %s, %s)")
            self.logger.info(f"Inserted {len(self.insert_value_transfer_txns)} value transfer transaction(s) for epoch {epoch}")
        self.insert_value_transfer_txns = []

        # insert data request transactions
        if len(self.insert_data_request_txns) > 0:
            sql = """
                INSERT INTO data_request_txns (
                    txn_hash,
                    txn_kind,
                    input_addresses,
                    input_values,
                    input_utxos,
                    output_addresses,
                    output_values,
                    witnesses,
                    witness_reward,
                    collateral,
                    consensus_percentage,
                    commit_and_reveal_fee,
                    weight,
                    urls,
                    scripts,
                    aggregate_filters,
                    aggregate_reducer,
                    tally_filters,
                    tally_reducer,
                    RAD_bytes_hash,
                    data_request_bytes_hash,
                    epoch
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_data_request_txns, template="(%s, %s, %s::CHAR(42)[], %s, %s::utxo[], %s::CHAR(42)[], %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::filter[], %s, %s::filter[], %s, %s, %s, %s)")
            self.logger.info(f"Inserted {len(self.insert_data_request_txns)} data request transaction(s) for epoch {epoch}")
        self.insert_data_request_txns = []

        # insert commit transactions
        if len(self.insert_commit_txns) > 0:
            sql = """
                INSERT INTO commit_txns (
                    txn_hash,
                    txn_address,
                    input_values,
                    input_utxos,
                    output_values,
                    data_request_txn_hash,
                    epoch
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_commit_txns, template="(%s, %s, %s, %s::utxo[], %s, %s, %s)")
            self.logger.info(f"Inserted {len(self.insert_commit_txns)} commit transaction(s) for epoch {epoch}")
        self.insert_commit_txns = []

        # insert reveal transactions
        if len(self.insert_reveal_txns) > 0:
            sql = """
                INSERT INTO reveal_txns (
                    txn_hash,
                    txn_address,
                    data_request_txn_hash,
                    result,
                    success,
                    epoch
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_reveal_txns)
            self.logger.info(f"Inserted {len(self.insert_reveal_txns)} reveal transaction(s) for epoch {epoch}")
        self.insert_reveal_txns = []

        # insert tally transactions
        if len(self.insert_tally_txns) > 0:
            sql = """
                INSERT INTO tally_txns (
                    txn_hash,
                    output_values,
                    data_request_txn_hash,
                    error_addresses,
                    liar_addresses,
                    result,
                    success,
                    epoch
                ) VALUES %s
            """
            self.db_mngr.sql_execute_many(sql, self.insert_tally_txns)
            self.logger.info(f"Inserted {len(self.insert_tally_txns)} tally transaction(s) for epoch {epoch}")
        self.insert_tally_txns = []

    def finalize_update(self, epoch):
        # update hashes
        if len(self.update_hashes) > 0:
            sql = """
                UPDATE hashes
                SET
                    epoch=update.epoch
                FROM (VALUES %s)
                AS update(
                    hash,
                    epoch
                )
                WHERE
                    hashes.hash=update.hash
            """
            self.db_mngr.sql_execute_many(sql, self.update_hashes, template="(%s, %s)")
            self.logger.info(f"Updated {len(self.update_hashes)} hash(es) for epoch {epoch}")
        self.update_hashes = []

        # update blocks
        if len(self.update_blocks) > 0:
            sql = """
                UPDATE blocks
                SET
                    confirmed=update.confirmed
                FROM (VALUES %s)
                AS update(
                    block_hash,
                    confirmed
                )
                WHERE
                    blocks.block_hash=update.block_hash
            """
            self.db_mngr.sql_execute_many(sql, self.update_blocks, template="(%s, %s)")
            self.logger.info(f"Updated {len(self.update_blocks)} block(s) for epoch {epoch}")
        self.update_blocks = []

        # update value transfer transactions
        if len(self.update_value_transfer_txns) > 0:
            sql = """
                UPDATE value_transfer_txns
                SET
                    epoch=update.epoch
                FROM (VALUES %s)
                AS update(
                    txn_hash,
                    epoch
                )
                WHERE
                    value_transfer_txns.txn_hash=update.txn_hash
            """
            self.db_mngr.sql_execute_many(sql, self.update_value_transfer_txns, template="(%s, %s)")
            self.logger.info(f"Updated {len(self.update_value_transfer_txns)} value transfer transaction(s) for epoch {epoch}")
        self.update_value_transfer_txns = []

        # update data request transactions
        if len(self.update_data_request_txns) > 0:
            sql = """
                UPDATE data_request_txns
                SET
                    epoch=update.epoch
                FROM (VALUES %s)
                AS update(
                    txn_hash,
                    epoch
                )
                WHERE
                    data_request_txns.txn_hash=update.txn_hash
                """
            self.db_mngr.sql_execute_many(sql, self.update_data_request_txns, template="(%s, %s)")
            self.logger.info(f"Updated {len(self.update_data_request_txns)} data request transaction(s) for epoch {epoch}")
        self.update_data_request_txns = []

        # update reveal transactions
        if len(self.update_reveal_txns) > 0:
            sql = """
                UPDATE reveal_txns
                SET
                    result=update.result,
                    success=update.success,
                    epoch=update.epoch
                FROM (VALUES %s)
                AS update(
                    txn_hash,
                    result,
                    success,
                    epoch
                )
                WHERE
                    reveal_txns.txn_hash=update.txn_hash
            """
            self.db_mngr.sql_execute_many(sql, self.update_reveal_txns, template="(%s, %s, %s, %s)")
            self.logger.info(f"Updated {len(self.update_reveal_txns)} reveal transaction(s) for epoch {epoch}")
        self.update_reveal_txns = []

        # update tally transactions
        if len(self.update_tally_txns) > 0:
            sql = """
                UPDATE tally_txns
                SET
                    output_values=update.output_values,
                    error_addresses=update.error_addresses,
                    liar_addresses=update.liar_addresses,
                    result=update.result,
                    success=update.success,
                    epoch=update.epoch
                FROM (VALUES %s)
                AS update(
                    txn_hash,
                    output_values,
                    error_addresses,
                    liar_addresses,
                    result,
                    success,
                    epoch
                )
                WHERE tally_txns.txn_hash=update.txn_hash
            """
            self.db_mngr.sql_execute_many(sql, self.update_tally_txns, template="(%s, %s, %s::CHAR(42)[], %s::CHAR(42)[], %s, %s, %s)")
            self.logger.info(f"Updated {len(self.update_tally_txns)} tally transaction(s) for epoch {epoch}")
        self.update_tally_txns = []

    def confirm_block(self, block_hash, epoch):
        sql = """
            UPDATE blocks
            SET
                confirmed=true
            WHERE
                block_hash=%s
        """ % psycopg2.Binary(bytearray.fromhex(block_hash))
        result = self.db_mngr.sql_update_table(sql)
        self.logger.info(f"Confirmed block {block_hash} for epoch {epoch}")

    def revert_block(self, block_hash, epoch):
        sql = """
            UPDATE blocks
            SET
                confirmed=false,
                reverted=true
            WHERE block_hash=%s
        """ % psycopg2.Binary(bytearray.fromhex(block_hash))
        result = self.db_mngr.sql_update_table(sql)
        self.logger.info(f"Reverted block {block_hash} for epoch {epoch}")

    def remove_block(self, block_hash, epoch):
        sql = """
            DELETE FROM blocks
            WHERE
                block_hash=%s
        """ % psycopg2.Binary(bytearray.fromhex(block_hash))
        result = self.db_mngr.sql_update_table(sql)
        self.logger.info(f"Deleted block {block_hash} for epoch {epoch}")

    #####################################################
    #       Create pending transactions histograms      #
    #####################################################

    def insert_pending_data_request_txns(self, timestamp, fee_per_unit, num_txns):
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

    def check_hash(self, item_hash):
        if item_hash in [insert_hash[0] for insert_hash in self.insert_hashes]:
            return True
        sql = "SELECT * FROM hashes WHERE hash=%s" % psycopg2.Binary(item_hash)
        result = self.db_mngr.sql_return_one(sql)
        if result:
            return True
        return False

    def get_last_block(self, confirmed=True):
        if confirmed:
            sql = """
                SELECT
                    block_hash,
                    epoch,
                    confirmed
                FROM blocks
                WHERE
                    confirmed=true
                ORDER BY epoch DESC
                LIMIT 1
            """
        else:
            sql = """
                SELECT
                    block_hash,
                    epoch,
                    confirmed
                FROM blocks
                ORDER BY epoch DESC
                LIMIT 1
            """
        result = self.db_mngr.sql_return_one(sql)
        if result:
            return result[0].hex(), int(result[1])
        else:
            return "", -1
