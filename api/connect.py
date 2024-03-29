from mockups.cache import MockCache
from mockups.database import MockDatabase
from mockups.socket_manager import MockSocketManager
from mockups.witnet_node import MockWitnetNode
from node.witnet_client_pool import WitnetClientPool
from util.database_pool import DatabasePool
from util.memcached import MemcachedPool
from util.socket_manager import SocketManager


def create_address_caching_server(config, mock=False):
    if mock:
        address_caching_server = MockSocketManager()
    else:
        caching_config = config["api"]["caching"]
        address_caching_server = SocketManager(
            caching_config["scripts"]["addresses"]["host"],
            caching_config["scripts"]["addresses"]["port"],
            caching_config["scripts"]["addresses"]["default_timeout"],
        )
    return address_caching_server


def create_cache(config, mock=False):
    if mock:
        cache = MockCache()
    else:
        caching_config = config["api"]["caching"]
        cache = MemcachedPool(
            caching_config["server"].split(","),
            caching_config["user"],
            caching_config["password"],
            caching_config["threads"],
            caching_config["blocking"],
        )
    return cache


def create_database(config, mock=False):
    if mock:
        database = MockDatabase()
    else:
        database = DatabasePool(config["database"])
    return database


def create_witnet_node(config, mock=False):
    if mock:
        witnet_node = MockWitnetNode()
    else:
        witnet_node = WitnetClientPool(config["node-pool"])
    return witnet_node
