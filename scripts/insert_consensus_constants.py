import argparse
import time

import toml

from node.witnet_node import WitnetNode
from util.database_manager import DatabaseManager


def get_consensus_constants(config):
    witnet_node = WitnetNode(config["node-pool"])

    response = witnet_node.get_consensus_constants()
    while type(response) is dict and "error" in response:
        time.sleep(10)
        response = witnet_node.get_consensus_constants()
    witnet_node.close_connection()

    return response["result"]


def insert_consensus_constants(config, consensus_constants):
    db_mngr = DatabaseManager(config["database"])

    for key, value in consensus_constants.items():
        if isinstance(value, int) or isinstance(value, float):
            sql = """
                INSERT INTO consensus_constants (
                    key,
                    int_val
                ) VALUES (%s, %s)
                ON CONFLICT  ON CONSTRAINT
                    consensus_constants_pkey
                DO NOTHING
            """
            if isinstance(value, float):
                value = int(value * 100)
        elif isinstance(value, str) or isinstance(value, list):
            sql = """
                INSERT INTO consensus_constants (
                    key,
                    str_val
                ) VALUES (%s, %s)
                ON CONFLICT  ON CONSTRAINT
                    consensus_constants_pkey
                DO NOTHING
            """
            if isinstance(value, str):
                value = [value]

        db_mngr.sql_insert_one(sql, (key, value))


def main():
    parser = argparse.ArgumentParser(
        prog="Insert consensus constants",
        description="Run as a one-off script to insert the crrent consensus constants into the database",
    )
    parser.add_argument(
        "--config-file",
        type=str,
        default="explorer.toml",
        dest="config_file",
    )
    args = parser.parse_args()

    config = toml.load(args.config_file)

    consensus_constants = get_consensus_constants(config)

    insert_consensus_constants(config, consensus_constants)


if __name__ == "__main__":
    main()
