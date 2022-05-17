import psycopg2
import pylibmc

from blockchain.witnet_database import WitnetDatabase

from node.consensus_constants import ConsensusConstants
from node.witnet_node import WitnetNode

from util.socket_manager import SocketManager

class Client(object):
    def __init__(self, config, node=False, database=False, memcached_client=False, consensus_constants=False):
        # Connect to node pool
        if node:
            node_config = config["node-pool"]
            try:
                self.witnet_node = WitnetNode(node_config["host"], node_config["port"], 300, logger=self.logger)
            except ConnectionRefusedError:
                self.logger.error(f"Could not connect to the node pool!")
                sys.exit(1)

        # Connect to database
        if database:
            db_config = config["database"]
            try:
                self.witnet_database = WitnetDatabase(db_config["user"], db_config["name"], db_config["password"], logger=self.logger)
            except psycopg2.OperationalError:
                self.logger.error(f"Could not connect to the database!")
                sys.exit(2)

        # Memcached client
        if memcached_client:
            cache_config = config["api"]["caching"]
            servers = cache_config["server"].split(",")
            self.memcached_client = pylibmc.Client(servers, binary=True, username=cache_config["user"], password=cache_config["password"], behaviors={"tcp_nodelay": True, "ketama": True})

            try:
                for server in servers:
                    # Try to connect to the memcached server to check if it is running
                    socket_mngr = SocketManager(server, "11211", 1)
                    socket_mngr.connect()
                    socket_mngr.disconnect()
            except ConnectionRefusedError:
                self.logger.error(f"Could not connect to the memcached server!")
                sys.exit(3)

        # Get consensus constants
        if consensus_constants:
            node_config = config["node-pool"]
            try:
                self.consensus_constants = ConsensusConstants(node_config["host"], node_config["port"], config["api"]["error_retry"], logger=self.logger)
            except ConnectionRefusedError:
                self.logger.error(f"Could not connect to the node pool!")
                sys.exit(1)
