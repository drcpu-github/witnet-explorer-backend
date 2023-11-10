import optparse
import psycopg2
import psycopg2.extras
import subprocess
import sys
import toml

def execute_command(command):
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = p.communicate()
    if len(stderr) > 0:
        sys.stderr.write(stderr + "\n")
        sys.exit(1)
    return stdout

def check_version():
    stdout = execute_command("psql --version")
    major, minor = stdout.decode("utf-8").split()[2].split(".")
    if int(major) < 15:
        sys.stderr.write("Minimum required version of PostgreSQL is 15")
        sys.exit(1)

def create_user(user, password):
    # Check if user exists
    stdout = execute_command(f"sudo -u postgres psql -c \"SELECT 1 FROM pg_roles WHERE rolname='{user}'\"")
    # Does the user exist?
    if stdout == b' ?column? \n----------\n(0 rows)\n\n':
        # Create user
        if password == "":
            execute_command(f"sudo -u postgres psql -c \"CREATE USER {user}\"")
        else:
            execute_command(f"sudo -u postgres psql -c \"CREATE USER {user} PASSWORD '{password}'\"")
        # Allow user to create databases
        execute_command(f"sudo -u postgres psql -c \"ALTER USER {user} CREATEDB\"")
        print(f"Ceated user '{user}'")
    else:
        print(f"User '{user}' already exists")

def create_database(name, user):
    connection, cursor = connect_to_database("postgres")
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname='{name}'")
    result = cursor.fetchone()
    if not result:
        cursor.execute(f"CREATE DATABASE {name} OWNER {user}")
        print(f"Created database '{name}'")
    else:
        print(f"Database '{name}' already exists")

def connect_to_database(name, user="", password=""):
    try:
        if user == "":
            if password == "":
                connection = psycopg2.connect(dbname=name)
            else:
                connection = psycopg2.connect(dbname=name, password=password)
        else:
            if password == "":
                connection = psycopg2.connect(dbname=name, user=user)
            else:
                connection = psycopg2.connect(dbname=name, user=user, password=password)
        cursor = connection.cursor()
    except psycopg2.OperationalError as e:
        str_error = str(e).replace("\n", "").replace("\t", " ")
        sys.stderr.write(f"Could not connect to database, error message: {str_error}\n")
        sys.exit(2)
    return connection, cursor

def execute_create_statement(connection, cursor, sql):
    try:
        cursor.execute(sql)
    except Exception as e:
        sys.stderr.write(f"Could not execute SQL statement '{sql}', error: {e}\n")
    connection.commit()

def create_enums(connection, cursor):
    enums = [
        """DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'hash_type') THEN
                    CREATE TYPE hash_type AS ENUM (
                        'block',
                        'mint_txn',
                        'value_transfer_txn',
                        'data_request_txn',
                        'RAD_bytes_hash',
                        'DRO_bytes_hash',
                        'commit_txn',
                        'reveal_txn',
                        'tally_txn'
                    );
                END IF;
            END
        $$;
        COMMIT;""",

        """DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'retrieve_kind') THEN
                    CREATE TYPE retrieve_kind AS ENUM (
                        'Unknown',
                        'HTTP-GET',
                        'HTTP-POST',
                        'RNG'
                    );
                END IF;
            END
        $$;
        COMMIT;""",

        """DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'reputation_type') THEN
                    CREATE TYPE reputation_type AS ENUM (
                        'gain',
                        'expire',
                        'lie'
                    );
                END IF;
            END
        $$;
        COMMIT;""",

        """DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'network_stat') THEN
                    CREATE TYPE network_stat AS ENUM (
                        'epoch',
                        'rollbacks',
                        'miners',
                        'data_request_solvers',
                        'data_requests',
                        'lie_rate',
                        'burn_rate',
                        'trs',
                        'value_transfers',
                        'staking'
                    );
                END IF;
            END
        $$;
        COMMIT;""",
    ]

    for enum in enums:
        execute_create_statement(connection, cursor, enum)

    print("Created all enums")

def create_types(connection, cursor):
    types = [
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

        """DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'filter') THEN
                    CREATE TYPE filter AS (
                        type SMALLINT,
                        args BYTEA
                    );
                END IF;
            END
        $$;
        COMMIT;""",
    ]

    for db_type in types:
        execute_create_statement(connection, cursor, db_type)

    print("Created all types")

def create_tables(connection, cursor):
    tables = [
        """CREATE TABLE IF NOT EXISTS addresses (
            id INT GENERATED ALWAYS AS IDENTITY,
            address CHAR(42) PRIMARY KEY,
            label VARCHAR(64),
            active INT,
            block INT,
            mint INT,
            value_transfer INT,
            data_request INT,
            commit INT,
            reveal INT,
            tally INT
        );""",

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
            dr_weight INT NOT NULL,
            vt_weight INT NOT NULL,
            block_weight INT NOT NULL,
            epoch INT NOT NULL,
            tapi_signals INT,
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

        """CREATE TABLE IF NOT EXISTS data_request_txns (
            txn_hash BYTEA PRIMARY KEY,
            input_addresses CHAR(42) ARRAY NOT NULL,
            input_values BIGINT ARRAY NOT NULL,
            input_utxos utxo ARRAY NOT NULL,
            output_address CHAR(42),
            output_value BIGINT,
            witnesses SMALLINT NOT NULL,
            witness_reward BIGINT NOT NULL,
            collateral BIGINT NOT NULL,
            consensus_percentage SMALLINT NOT NULL,
            commit_and_reveal_fee BIGINT NOT NULL,
            weight INT NOT NULL,
            kinds retrieve_kind ARRAY NOT NULL,
            urls VARCHAR ARRAY NOT NULL,
            bodies BYTEA ARRAY NOT NULL,
            scripts BYTEA ARRAY NOT NULL,
            aggregate_filters filter ARRAY NOT NULL,
            aggregate_reducer INT ARRAY NOT NULL,
            tally_filters filter ARRAY NOT NULL,
            tally_reducer INT ARRAY NOT NULL,
            RAD_bytes_hash BYTEA NOT NULL,
            DRO_bytes_hash BYTEA NOT NULL,
            epoch INT NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS commit_txns (
            txn_hash BYTEA PRIMARY KEY,
            txn_address CHAR(42) NOT NULL,
            input_values BIGINT ARRAY NOT NULL,
            input_utxos utxo ARRAY NOT NULL,
            output_value BIGINT,
            data_request BYTEA NOT NULL,
            epoch INT NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS reveal_txns (
            txn_hash BYTEA PRIMARY KEY,
            txn_address CHAR(42) NOT NULL,
            data_request BYTEA NOT NULL,
            result BYTEA NOT NULL,
            success BOOL NOT NULL,
            epoch INT NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS tally_txns (
            txn_hash BYTEA PRIMARY KEY,
            output_addresses CHAR(42) ARRAY NOT NULL,
            output_values BIGINT ARRAY NOT NULL,
            data_request BYTEA NOT NULL,
            error_addresses CHAR(42) ARRAY NOT NULL,
            liar_addresses CHAR(42) ARRAY NOT NULL,
            result BYTEA NOT NULL,
            success BOOL NOT NULL,
            epoch INT NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS pending_data_request_txns (
            timestamp INT NOT NULL,
            fee_per_unit BIGINT ARRAY NOT NULL,
            num_txns INT ARRAY NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS pending_value_transfer_txns (
            timestamp INT NOT NULL,
            fee_per_unit BIGINT ARRAY NOT NULL,
            num_txns INT ARRAY NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS wips (
            id SMALLINT GENERATED ALWAYS AS IDENTITY,
            title VARCHAR NOT NULL,
            description VARCHAR NOT NULL,
            urls VARCHAR ARRAY NOT NULL,
            activation_epoch INT,
            tapi_start_epoch INT,
            tapi_stop_epoch INT,
            tapi_bit SMALLINT
        );""",

        """CREATE TABLE IF NOT EXISTS reputation (
            address CHAR(42) NOT NULL,
            epoch INT NOT NULL,
            reputation INT NOT NULL,
            type reputation_type NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS trs (
            epoch INT PRIMARY KEY,
            addresses INT ARRAY NOT NULL,
            reputations INT ARRAY NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS network_stats (
            stat network_stat NOT NULL,
            from_epoch INT,
            to_epoch INT,
            data JSONB NOT NULL,
            UNIQUE NULLS NOT DISTINCT (stat, from_epoch, to_epoch)
        );""",

        """CREATE TABLE IF NOT EXISTS data_request_reports (
            data_request_hash BYTEA PRIMARY KEY,
            report JSONB NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS cron_data (
            key VARCHAR PRIMARY KEY,
            data INT NOT NULL
        );""",

        """CREATE TABLE IF NOT EXISTS consensus_constants (
            key VARCHAR PRIMARY KEY,
            int_val BIGINT,
            str_val VARCHAR ARRAY
        );""",
    ]

    for table in tables:
        execute_create_statement(connection, cursor, table)

    print("Created all tables")

def create_indexes(connection, cursor):
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_block_epoch ON blocks (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_commit_txn_address ON commit_txns USING HASH (txn_address);",
        "CREATE INDEX IF NOT EXISTS idx_commit_txn_data_request ON commit_txns USING HASH (data_request);",
        "CREATE INDEX IF NOT EXISTS idx_reveal_txn_address ON reveal_txns USING HASH (txn_address);",
        "CREATE INDEX IF NOT EXISTS idx_reveal_txn_data_request ON reveal_txns USING HASH (data_request);",
        "CREATE INDEX IF NOT EXISTS idx_reputation_address ON reputation USING HASH (address);",
        "CREATE INDEX IF NOT EXISTS idx_value_transfer_input_addresses ON value_transfer_txns USING GIN (input_addresses);",
        "CREATE INDEX IF NOT EXISTS idx_value_transfer_output_addresses ON value_transfer_txns USING GIN (output_addresses);",
        "CREATE INDEX IF NOT EXISTS idx_mint_txn_epoch ON mint_txns (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_data_request_txn_epoch ON data_request_txns (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_commit_txn_epoch ON commit_txns (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_reveal_txn_epoch ON reveal_txns (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_tally_txn_epoch ON tally_txns (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_value_transfer_txn_epoch ON value_transfer_txns (epoch);",
        "CREATE INDEX IF NOT EXISTS idx_trs_epoch ON trs (epoch);",
    ]

    for index in indexes:
        execute_create_statement(connection, cursor, index)

    print("Created all indexes")

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    config = toml.load(options.config_file)

    check_version()

    create_user(config["database"]["user"], config["database"]["password"])

    create_database(config["database"]["name"], config["database"]["user"])
    connection, cursor = connect_to_database(config["database"]["name"], config["database"]["user"], config["database"]["password"])

    create_enums(connection, cursor)

    create_types(connection, cursor)

    create_tables(connection, cursor)

    create_indexes(connection, cursor)

if __name__ == "__main__":
    main()
