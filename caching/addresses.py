#!/usr/bin/python3

import gc
import json
import logging
import logging.handlers
import numpy
import optparse
import os
import pylibmc
import signal
import socket
import sys
import time
import toml

from marshmallow import ValidationError

from matplotlib import pyplot as plt

from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import Manager

from blockchain.objects.address import Address

from schemas.address.block_view_schema import BlockView
from schemas.address.data_request_view_schema import DataRequestCreatedView, DataRequestSolvedView
from schemas.address.mint_view_schema import MintView
from schemas.address.value_transfer_view_schema import ValueTransferView

from util.logger import create_logging_listener
from util.logger import select_logging_level
from util.pickle_process import PickleProcess
from util.socket_manager import SocketManager

class Addresses(object):
    def __init__(self, config, queue):
        self.config = config

        # Set up logger
        self.configure_logging_process(queue, "server")
        self.logger = logging.getLogger("server")
        # Set up logging queue for logging from different processes
        self.logging_queue = queue

        # Check if the memcached server is running and exit if it is not
        self.check_memcached_server_running()

        # Create address stack of addresses to monitor
        self.address_stack = Manager().list(self.load_address_stack())
        self.epoch_addresses = Manager().dict({})

        self.server_process = PickleProcess(target=self.start_server, args=(self.logging_queue, self.config, self.address_stack, self.epoch_addresses))

    def start(self):
        self.logger.info("Starting server process")
        # Infinite loop
        self.server_process.start()
        # This will just run forever
        self.server_process.join()

    def configure_logging_process(self, queue, name):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(name)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    def check_memcached_server_running(self):
        servers = self.config["api"]["caching"]["server"].split(",")
        try:
            for server in servers:
                # Try to connect to the memcached server to check if it is running
                socket_mngr = SocketManager(server, "11211", 1)
                socket_mngr.connect()
                socket_mngr.disconnect()
        except ConnectionRefusedError:
            self.logger.error(f"Could not connect to the memcached server!")
            sys.exit(1)

    def load_address_stack(self):
        filename = self.config["api"]["caching"]["scripts"]["addresses"]["address_stack_file"]
        # Return empty address stack
        if not os.path.exists(filename):
            return []
        with open(filename, "r") as address_stack_file:
            # Try and return a saved address stack
            try:
                address_stack = json.load(address_stack_file)
                self.logger.info(f"Loaded {len(address_stack)} addresses into address stack: {address_stack}")
                return address_stack
            except json.decoder.JSONDecodeError:
                return []
        # Return empty address stack
        return []

    def save_address_stack(self):
        filename = self.config["api"]["caching"]["scripts"]["addresses"]["address_stack_file"]
        # Create directory if necessary
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        # Dump address stack
        with open(filename, "w+") as address_stack_file:
            json.dump(list(self.address_stack), address_stack_file)
            self.logger.info("Saved address stack")

    ###############################################
    #    Functions to process caching requests    #
    ###############################################

    def start_server(self, logging_queue, config, address_stack, epoch_addresses):
        # Set up logger
        self.configure_logging_process(logging_queue, "server")
        logger = logging.getLogger("server")

        address_config = config["api"]["caching"]["scripts"]["addresses"]

        func_pool = Manager().Pool(address_config["processes"])

        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Set socket options: allows close and immediate reuse of an address, ignoring TIME_WAIT
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((address_config["host"], address_config["port"]))
        server_socket.listen()

        while True:
            # Wait for a connection
            connection, client = server_socket.accept()

            # Get request from the socket
            logger.info("Starting client process")
            p = PickleProcess(target=self.client, args=(logging_queue, config, connection, address_stack, epoch_addresses, func_pool))
            p.start()

    def client(self, logging_queue, config, connection, address_stack, epoch_addresses, func_pool):
        # Set up logger
        self.configure_logging_process(logging_queue, "client")
        logger = logging.getLogger("client")

        # Keep the connection open until a single "\n" is sent
        counter = 0
        while True:
            # Receive the data
            received = ""
            connection_closed = False
            while True:
                try:
                    data = connection.recv(1024)
                    received += data.decode("utf-8")
                    if not data:
                        break
                    if received[-1] == "\n":
                        break
                except ConnectionResetError:
                    connection_closed = True
                    logger.info("Closing connection because the connection was reset by the peer")
                    break

            # Single "\n" is sent, close the connection
            if received == "\n" or connection_closed:
                logger.info("Closing connection as requested by the client")
                break

            if len(received) == 0:
                continue

            request_buffer = received.split("\n")
            for request in request_buffer:
                # Break if the last entry is empty
                if len(request) == 0:
                    break

                counter += 1

                # Parse request
                request = json.loads(request)
                request_id = request["id"]

                # Log the complete request
                logger.info(f"Request {counter}: {request}")

                # A request always needs to specify a method to execute
                if "method" not in request:
                    logger.warning("Missing argument 'method' in request")
                    continue
                method = request["method"]

                # Most requests need to specify the addresses argument
                if method not in ("confirm", "revert"):
                    if "addresses" not in request:
                        logger.warning(f"Missing argument 'addresses' in {method} request")
                        continue
                    addresses = request["addresses"]

                # Update the address stack (only for requests originating from the API)
                if method == "track":
                    # API track requests should ask for one address only
                    assert len(addresses) == 1, "Expected only one address from an API request"
                    address = addresses[0]

                    # Create cache client
                    cache_config = self.config["api"]["caching"]
                    servers = cache_config["server"].split(",")
                    memcached_client = pylibmc.Client(servers, binary=True, username=cache_config["user"], password=cache_config["password"], behaviors={"tcp_nodelay": True, "ketama": True})

                    # Check if we recently received a request for this address
                    if memcached_client.get(f"{address}"):
                        logger.info(f"Received concurrent request for {address}")
                        continue

                    # Add this address to the memcache indicating we recently received a request for it
                    concurrent_timeout = config["api"]["caching"]["scripts"]["addresses"]["concurrent_request_timeout"]
                    memcached_client.set(f"{address}", "Processing", time=concurrent_timeout)

                    cache_size = config["api"]["caching"]["scripts"]["addresses"]["cache_size"]
                    removed_addresses = self.update_address_stack(logger, address_stack, cache_size, address)
                    # Invalidate all cached views for addresses that were removed from the tracker
                    if len(removed_addresses) > 0:
                        # Remove all cached views
                        for address in removed_addresses:
                            memcached_client.delete(f"{address}-utxos")
                            memcached_client.delete(f"{address}-blocks")
                            memcached_client.delete(f"{address}-mints")
                            memcached_client.delete(f"{address}-value-transfers")
                            memcached_client.delete(f"{address}-data-requests-solved")
                            memcached_client.delete(f"{address}-data-requests-created")
                            logger.info(f"Removed all cached views for {address}")

                # Data in the cache should timeout after some time to prevent stale data
                views_timeout = config["api"]["caching"]["scripts"]["addresses"]["views_timeout"]
                reputation_timeout = config["api"]["caching"]["scripts"]["addresses"]["reputation_timeout"]
                utxos_timeout = config["api"]["caching"]["scripts"]["addresses"]["utxos_timeout"]

                # Update cached address data on receiving a request from the explorer
                if method == "update":
                    if "function" not in request:
                        logger.warning("Missing argument 'function' in update request")
                        continue
                    function = request["function"]
                    if "epoch" not in request:
                        logger.warning("Missing argument 'epoch' in update request")
                        continue
                    epoch = request["epoch"]

                    # Only trigger below updates for addresses that are being tracked actively
                    functions, monitor_addresses = [], []
                    for address in addresses:
                        if address in address_stack:
                            # Add address to update list
                            functions.append(function)
                            monitor_addresses.append(address)
                            # Track the epoch in which an update for this address was received to later process a confirm / revert request
                            if epoch not in epoch_addresses:
                                epoch_addresses[epoch] = [[], []]
                            epoch_addresses[epoch][0].append(method)
                            epoch_addresses[epoch][1].append(address)

                    if len(monitor_addresses) == 0:
                        logger.info("No addresses to monitor in update request")
                        continue
                    else:
                        logger.info(f"Received a request to update {function} for {monitor_addresses}")
                # Update cached address data on receiving a request from the explorer
                elif method == "confirm" or method == "revert":
                    if "epoch" not in request:
                        logger.warning(f"Missing argument 'epoch' in {method} request")
                        continue
                    epoch = request["epoch"]

                    # Check if addresses were tracked and had their views updated during this epoch
                    if epoch in epoch_addresses:
                        functions = epoch_addresses[epoch][0]
                        monitor_addresses = epoch_addresses[epoch][1]
                        logger.info(f"Received a request to {method} views for {monitor_addresses} in epoch {epoch}")
                        del epoch_addresses[epoch]
                    else:
                        logger.info(f"No addresses to monitor in {method} request")
                        continue
                # Request received from API
                elif method == "track":
                    # On receiving a track request, check which data is still cached.
                    # If it is still cached, do not update it, this should be done through update requests from the explorer
                    functions = []
                    all_functions = [
                        "blocks",
                        "mints",
                        "value-transfers",
                        "data-requests-solved",
                        "data-requests-created",
                        "utxos",
                    ]
                    for function in all_functions:
                        data = memcached_client.get(f"{addresses[0]}_{function}")
                        if not data:
                            logger.debug(f"{function} for {addresses[0]} not found in cache")
                            functions.append(function)
                        else:
                            logger.debug(f"{function} for {addresses[0]} are still cached")
                    monitor_addresses = addresses * len(functions)
                else:
                    logger.info(f"Unknown request method received: {method}")
                    continue

                for function, m_address in zip(functions, monitor_addresses):
                    # Create address object
                    address = Address(m_address, config, logger=logger, connect=False)

                    # Complete the request
                    # This block of code is surrounded with a try-except to catch a known Python bug with the Manager multi-processing Pool
                    # https://github.com/python/cpython/issues/80100
                    try:
                        # Execute requested method asynchronously
                        if function == "blocks":
                            logger.info(f"Queueing execution of cache_address_data({m_address}, {views_timeout}) for blocks")
                            func_args = (logging_queue, "blocks", address, address.get_blocks, views_timeout)
                            func_pool.apply_async(self.cache_address_data, args=func_args, callback=self.log_completed)
                        elif function == "mints":
                            logger.info(f"Queueing execution of cache_address_data({m_address}, {views_timeout}) for mints")
                            func_args = (logging_queue, "mints", address, address.get_mints, views_timeout)
                            func_pool.apply_async(self.cache_address_data, args=func_args, callback=self.log_completed)
                        elif function == "value-transfers":
                            logger.info(f"Queueing execution of cache_address_data({m_address}, {views_timeout}) for value transfers")
                            func_args = (logging_queue, "value transfers", address, address.get_value_transfers, views_timeout)
                            func_pool.apply_async(self.cache_address_data, args=func_args, callback=self.log_completed)
                        elif function == "data-requests-solved":
                            logger.info(f"Queueing execution of cache_address_data({m_address}, {views_timeout}) for solved data requests")
                            func_args = (logging_queue, "data requests solved", address, address.get_data_requests_solved, views_timeout)
                            func_pool.apply_async(self.cache_address_data, args=func_args, callback=self.log_completed)
                        elif function == "data-requests-created":
                            logger.info(f"Queueing execution of cache_address_data({m_address}, {views_timeout}) for created data requests")
                            func_args = (logging_queue, "data requests created", address, address.get_data_requests_created, views_timeout)
                            func_pool.apply_async(self.cache_address_data, args=func_args, callback=self.log_completed)
                        elif function == "utxos":
                            logger.info(f"Queueing execution of cache_address_data({m_address}, {utxos_timeout}) for utxos")
                            func_args = (logging_queue, "utxos", address, address.get_utxos, utxos_timeout)
                            func_pool.apply_async(self.cache_address_data, args=func_args, callback=self.log_completed)
                        else:
                            logger.warning(f"Unknown request method {function}")
                    except AttributeError:
                        logger.debug("Manager shared manager Pool failure: this is a known Python bug, check for a fix in the next Python release (> 3.10).")

        # Close the connection
        connection.close()
        logger.info("Stopping node client process")

    ###########################################################
    #    Functions called directly from the client process    #
    ###########################################################

    def update_address_stack(self, logger, address_stack, cache_size, address):
        removed_addresses = []

        if len(address_stack) < cache_size:
            if address in address_stack:
                address_stack.remove(address)
            address_stack.append(address)
        else:
            if address in address_stack:
                address_stack.remove(address)
                address_stack.append(address)
            else:
                removed_addresses.append(address_stack.pop(0))
                address_stack.append(address)
        logger.debug(f"New stack of addresses to monitor is: {address_stack}")

        return removed_addresses

    #################################################################
    #    Functions called asynchronously from the client process    #
    #################################################################

    def log_completed(self, result):
        self.configure_logging_process(result[0], "function")
        logger = logging.getLogger("function")
        if result[1]:
            logger.info(result[1])

    def cache_address_data(self, logging_queue, label, address, address_function, timeout):
        # Set up logger
        self.configure_logging_process(logging_queue, "function")
        logger = logging.getLogger("function")

        address.initialize_connections()

        identity = address.address
        logger.info(f"Fetching {label} data for {identity}")
        if label == "utxos":
            address_data = address_function()
            if "result" in address_data:
                address_data = address_data["result"]["utxos"]
            else:
                logger.warning(f"Could not save {label} data for {identity} in the memcached instance: {address_data}")
                return logging_queue, None
        else:
            address_data = address_function()

            try:
                if label == "blocks":
                    BlockView(many=True).load(address_data)
                elif label == "mints":
                    MintView(many=True).load(address_data)
                elif label == "value transfers":
                    ValueTransferView(many=True).load(address_data)
                elif label == "data requests solved":
                    DataRequestSolvedView(many=True).load(address_data)
                elif label == "data requests created":
                    DataRequestCreatedView(many=True).load(address_data)
            except ValidationError:
                logger.error(f"Could not save {label} data for {identity} because it did not conform with the Marshmallow format")

        address.close_connections()

        # Create memcached client
        cache_config = self.config["api"]["caching"]
        servers = cache_config["server"].split(",")
        memcached_client = pylibmc.Client(servers, binary=True, username=cache_config["user"], password=cache_config["password"], behaviors={"tcp_nodelay": True, "ketama": True})

        # Attempt to cache the address data
        try:
            memcached_client.set(f"{identity}_{label.replace(' ', '-')}", address_data, time=timeout)
        except pylibmc.TooBig as e:
            logger.warning(f"Could not save {label} data for {identity} in the memcached instance because its size exceeded 1MB")
            return logging_queue, None

        return logging_queue, f"Cached {label} data for {identity}"

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    # Load config file
    config = toml.load(options.config_file)

    # Start logging process
    logging_queue = Manager().Queue()
    listener = PickleProcess(target=create_logging_listener, args=(config["api"]["caching"]["scripts"]["addresses"], logging_queue))
    listener.start()

    # Create address server
    addresses = Addresses(config, logging_queue)

    # Catch ctrl+c signal
    def signal_handler(*args):
        addresses.save_address_stack()
        # End the logging process
        logging_queue.put(None)
        # Sleep 1 second to make sure everything ended
        time.sleep(1)
        # Exit cleanly
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    addresses.start()

if __name__ == "__main__":
    main()
