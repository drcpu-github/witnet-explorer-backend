import psycopg
import pylibmc
import sys

from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode
from util.socket_manager import SocketManager
from util.database_manager import DatabaseManager


class Client(object):
    def __init__(self, config, node_timeout=0, named_cursor=False):
        self.config = config

        # Connect to node pool
        try:
            self.witnet_node = WitnetNode(
                config["node-pool"],
                timeout=node_timeout,
                logger=self.logger,
            )
        except ConnectionRefusedError:
            self.logger.error("Could not connect to the node pool!")
            sys.exit(1)

        # Connect to database
        try:
            self.database = DatabaseManager(
                config["database"],
                named_cursor=named_cursor,
                logger=self.logger,
                custom_types=["utxo", "filter"],
            )
            if named_cursor:
                self.database_client = DatabaseManager(
                    config["database"],
                    named_cursor=False,
                    logger=self.logger,
                    custom_types=["utxo", "filter"],
                )
        except psycopg.OperationalError:
            self.logger.error("Could not connect to the database!")
            sys.exit(1)

        # Memcached client
        cache_config = config["api"]["caching"]
        servers = cache_config["server"].split(",")
        self.memcached_client = pylibmc.Client(
            servers,
            binary=True,
            username=cache_config["user"],
            password=cache_config["password"],
            behaviors={"tcp_nodelay": True, "ketama": True},
        )

        try:
            for server in servers:
                # Try to connect to the memcached server to check if it is running
                socket_mngr = SocketManager(server, "11211", 1)
                socket_mngr.connect()
                socket_mngr.disconnect()
        except ConnectionRefusedError:
            self.logger.error("Could not connect to the memcached server!")
            sys.exit(1)

        # Get consensus constants
        try:
            self.consensus_constants = ConsensusConstants(
                database=self.database,
                witnet_node=self.witnet_node,
                error_retry=config["api"]["error_retry"],
            )
        except ConnectionRefusedError:
            self.logger.error("Could not connect to the node pool!")
            sys.exit(1)

    def get_start_epoch(self, key):
        sql = """
            SELECT
                data
            FROM
                cron_data
            WHERE
                key=%s
        """
        epoch = self.database.sql_return_one(sql, parameters=[key])
        if epoch:
            return epoch[0]
        else:
            return None

    def set_start_epoch(self, key, epoch):
        sql = """
            INSERT INTO cron_data(
                key,
                data
            ) VALUES (%s, %s)
            ON CONFLICT ON CONSTRAINT
                cron_data_pkey
            DO UPDATE SET
                data=EXCLUDED.data
        """
        self.database.sql_insert_one(sql, [key, epoch])
