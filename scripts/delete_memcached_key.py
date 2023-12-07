import optparse
import sys

import pylibmc
import toml

from util.socket_manager import SocketManager


def create_memcached_client(config):
    cache_config = config["api"]["caching"]
    servers = cache_config["server"].split(",")
    memcached_client = pylibmc.Client(
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
        sys.stderr.write("Could not connect to the memcached server!")
        sys.exit(3)

    return memcached_client


def main():
    parser = optparse.OptionParser()
    parser.add_option(
        "--config-file",
        type="string",
        default="explorer.toml",
        dest="config_file",
        help="Specify a configuration file",
    )
    parser.add_option(
        "--key",
        type="string",
        dest="key",
        help="Specify a key to delete",
    )
    options, args = parser.parse_args()

    config = toml.load(options.config_file)

    memcached_client = create_memcached_client(config)
    memcached_client.delete(options.key)


if __name__ == "__main__":
    main()
