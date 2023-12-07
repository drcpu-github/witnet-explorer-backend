import optparse
import sys

import toml
from psycopg.sql import SQL, Identifier

from util.database_manager import DatabaseManager


def delete_hash(db_mngr, h):
    sql_statement = "SELECT type FROM hashes WHERE hashes.hash=%s"
    (hash_type,) = db_mngr.sql_return_one(
        sql_statement, parameters=[bytearray.fromhex(h)]
    )

    sql_statement = "DELETE FROM hashes WHERE hashes.hash=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[bytearray.fromhex(h)])
    print(f"Deleted {result} hash from hash table")

    sql_statement = "DELETE FROM {table} WHERE {table}.txn_hash=%s"
    sql_statement = SQL(sql_statement).format(table=Identifier(f"{hash_type}s"))
    result = db_mngr.sql_update_table(sql_statement, parameters=[bytearray.fromhex(h)])
    print(f"Deleted {result} hash from transaction table")


def main():
    parser = optparse.OptionParser()
    parser.add_option("--hashes", type="string", dest="hashes")
    parser.add_option(
        "--config-file",
        type="string",
        default="explorer.toml",
        dest="config_file",
        help="Specify a configuration file",
    )
    options, args = parser.parse_args()

    config = toml.load(options.config_file)
    db_mngr = DatabaseManager(config["database"])

    if options.hashes is not None:
        hashes = options.hashes.split(",")
        for h in hashes:
            delete_hash(db_mngr, h)
    else:
        sys.stderr.write("Usage of this script:\n")
        sys.stderr.write("./delete_blocks --hashes <comma-separated string>\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
