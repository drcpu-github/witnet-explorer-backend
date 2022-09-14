#!/usr/bin/python3

import datetime
import errno
import json
import logging
import logging.handlers
import optparse
import os
import psutil
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import toml

from multiprocessing import Process
from multiprocessing import Queue
from multiprocessing import Value
from multiprocessing.managers import SyncManager

from util.logger import create_logging_listener
from util.logger import select_logging_level
from util.socket_manager import SocketManager

class NodePool(object):
    def __init__(self, config, queue):
        self.config = config

        # Using a SyncManager instead of Manager so signals can be caught and the shared data structures keep living
        self.mngr = SyncManager()
        self.mngr.start(self.mngr_init)
        # Create shared lists
        self.request_counter, self.process_pids, self.node_synced, self.node_lock, self.node_unsynced_time, self.node_sockets = Value('i', 1), self.mngr.list(), self.mngr.list(), self.mngr.list(), self.mngr.list(), self.mngr.list()

        # Create list for node termination
        self.terminate_node_lock = []

        # Set up logger
        self.configure_logging_process(queue, "server")
        self.logger = logging.getLogger("server")
        # Set up logging queue for logging from different processes
        self.logging_queue = queue

        # Create all variables consumed by the nodes
        self.default_timeout = self.config["node-pool"]["default_timeout"]
        for node in range(self.config["node-pool"]["nodes"]["number"]):
            node_str = f"node-{node + 1}"
            # Create shared variables for the nodes
            self.process_pids.append(0)
            self.node_synced.append(False)
            self.node_lock.append(self.mngr.Lock())
            self.node_unsynced_time.append(0)
            # Create sockets
            node_ip = self.config["node-pool"]["nodes"][node_str]["ip"]
            node_port = self.config["node-pool"]["nodes"][node_str]["port"]
            self.node_sockets.append(SocketManager(node_ip, node_port, self.default_timeout))
            # Create locks only used to terminate nodes in the main process to prevent we try to kill them from every child process
            self.terminate_node_lock.append(self.mngr.Lock())

        self.logger.info("Starting server process")
        self.server_process = Process(target=self.start_server, args=(self.logging_queue, self.config, self.request_counter, self.process_pids, self.node_sockets, self.node_synced, self.node_lock, self.node_unsynced_time))

    ###############################################
    #    Functions called from NodePool object    #
    ###############################################

    def mngr_init(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)

    def start(self):
        self.logger.info("Starting nodes and server processes")

        # Start all local nodes
        for node in range(self.config["node-pool"]["nodes"]["number"]):
            self.logger.info(f"Starting node process node-{node + 1}")
            p = Process(target=self.start_node, args=(node, self.config, self.request_counter, self.process_pids, self.node_sockets, self.logging_queue, self.node_synced, self.node_lock))
            p.start()

        # Infinite loop
        self.server_process.start()
        # This will just run forever
        self.server_process.join()

    def terminate(self, witnet_binary):
        self.logger.info("Terminating all nodes and server processes")

        # Terminate witnet node processes
        for i in range(0, len(self.process_pids)):
            if self.terminate_node_lock[i].acquire():
                if self.process_pids[i] != 0:
                    self.terminate_process(witnet_binary, self.logger, self.process_pids[i], socket=self.node_sockets[i])
                    self.node_synced[i] = False
                    self.process_pids[i] = 0
                self.terminate_node_lock[i].release()

    ############################################
    #    Function used to configure logging    #
    ############################################

    def configure_logging_process(self, queue, name):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(name)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    ######################################################
    #    Functions possibly called from new processes    #
    ######################################################

    def start_server(self, logging_queue, config, request_counter, process_pids, node_sockets, node_synced, node_lock, node_unsynced_time):
        # Set up logger
        self.configure_logging_process(logging_queue, "server")
        logger = logging.getLogger("server")

        address = (config["node-pool"]["host"], config["node-pool"]["port"])
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Set socket options: allows close and immediate reuse of an address, ignoring TIME_WAIT
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(address)
        server_socket.listen()

        while True:
            # Wait for a connection
            connection, client = server_socket.accept()
            logger.info("Starting node client process")
            p = Process(target=self.node_client, args=(logging_queue, config, connection, request_counter, process_pids, node_sockets, node_synced, node_lock, node_unsynced_time))
            p.start()

    def node_client(self, logging_queue, config, connection, request_counter, process_pids, node_sockets, node_synced, node_lock, node_unsynced_time):
        # Set up logger
        self.configure_logging_process(logging_queue, "client")
        logger = logging.getLogger("client")

        # Keep the connection open until a single "\n" is sent
        counter = 1
        while True:
            # Receive the data
            request = ""
            connection_closed = False
            while True:
                try:
                    data = connection.recv(1024)
                    request += data.decode("utf-8")
                    if not data:
                        break
                    if request[-1] == "\n":
                        break
                except ConnectionResetError:
                    connection_closed = True
                    logger.info("Closing connection because the connection was reset by the peer")
                    break

            # Single "\n" is sent, close the connection
            if request == "\n" or connection_closed:
                logger.info("Closing connection as requested by the client")
                break

            if len(request) == 0:
                continue

            # Parse request
            request = json.loads(request)
            request_id = request["id"]

            # Log the complete request
            logger.info(f"Request {counter}: {request}")

            # Check if a non-default timeout was specified and propagate it
            request_timeout = 0
            if "timeout" in request:
                request_timeout = request["timeout"]
                del request["timeout"]

            request_served = False
            nodes_to_restart = set()
            # If all variables in node_synced are False, there are no nodes synced

            # Check all locks
            node_locked = [[f"node-{n + 1}", True] for n in range(0, len(node_lock))]
            for i, lock in enumerate(node_lock):
                acq = lock.acquire(False)
                if acq:
                    node_locked[i][1] = False
                    lock.release()
            logger.info(f"Node synced: {node_synced}, node locked: {node_locked}")

            for i, node_socket in enumerate(node_sockets):
                node_str = f"node-{i + 1}"
                # Check that the node is not in use
                if node_lock[i].acquire(False):
                    # Check that it is currently synced (if required)
                    if self.check_node_synced(logger, i, request_counter, node_socket):
                        logger.info(f"{node_str} can serve the request")
                        # If requested set a different socket timeout
                        if request_timeout != 0:
                            node_socket.set_timeout(request_timeout)
                        response = self.execute_request(logger, node_socket, request)
                        # Reset socket timeout
                        if request_timeout != 0:
                            node_socket.reset_timeout()
                        request_served = True
                    else:
                        # If this is the first time we notice the node is unsynced, start the unsynced timer
                        if node_synced[i]:
                            logger.warning(f"Node {i + 1} is not synced anymore, setting restart timer")
                            node_synced[i] = False
                            node_unsynced_time[i] = int(time.time())
                    node_lock[i].release()

                if request_served:
                    break

            # If request_served is still False, no nodes were available or all nodes were not synced anymore
            if not request_served:
                if sum(node_synced) == 0:
                    response = {"error": "no synced nodes found", "id": request_id}
                    logger.warning("No synced nodes found")
                else:
                    response = {"error": "no available nodes found", "id": request_id}
                    logger.warning("No available nodes found")

            connection.sendall((json.dumps(response) + "\n").encode("utf-8"))

            # Double check if the node is synced if it was marked as unsynced or mark it as restartable
            for i, node_socket in enumerate(node_sockets):
                if node_synced[i] == False:
                    synced = self.check_node_synced(logger, i, request_counter, node_socket)
                    if synced:
                        node_synced[i] = True
                        node_unsynced_time[i] = 0
                    # If the timer elapsed, add the node to the restart list
                    restart_unsynced_timeout = config["node-pool"]["nodes"]["restart_unsynced_timeout"]
                    if node_unsynced_time[i] > 0 and int(time.time()) - node_unsynced_time[i] > restart_unsynced_timeout:
                        logger.warning(f"Restart timer of node-{i + 1} elapsed and it will be restarted")
                        nodes_to_restart.add(i)
                        # Reset unsynced time here so concurrent accesses don't try to restart the nodes again
                        node_unsynced_time[i] = 0

            # Restart nodes that are not synced
            if len(nodes_to_restart) > 0:
                for node in nodes_to_restart:
                    node_str = f"node-{node  + 1}"
                    node_type = config["node-pool"]["nodes"][node_str]["type"]
                    if node_type == "local":
                        logger.warning(f"Restarting {node_str}, PID {process_pids[node]}")
                        p = Process(target=self.restart_node, args=(node, config, request_counter, process_pids, node_sockets, logging_queue, node_synced, node_lock))
                        p.start()
                    else:
                        logger.warning("Cannot restart a remote node")

            counter += 1

        # Close the connection
        connection.close()
        logger.info("Stopping node client process")

    def start_node(self, node, config, request_counter, process_pids, node_sockets, logging_queue, node_synced, node_lock):
        node_str = f"node-{node  + 1}"

        # Set up logger
        self.configure_logging_process(logging_queue, node_str)
        logger = logging.getLogger(node_str)

        # Try to acquire lock if necessary (non-blocking because the lock may have been acquired by restart_node)
        if node_lock:
            node_lock[node].acquire(False)

        node_type = config["node-pool"]["nodes"][node_str]["type"]
        if node_type == "local":
            # Parse some configuration variables
            witnet_exec = config["node-pool"]["nodes"]["binary"]
            binary_path = os.path.dirname(witnet_exec)
            witnet_binary = os.path.basename(witnet_exec)
            sync_sleep = config["node-pool"]["nodes"]["sync_sleep"]
            no_peers_restart = config["node-pool"]["nodes"]["no_peers_restart"]
            # Parse config file if it exists
            if "config" in config["node-pool"]["nodes"][node_str]:
                config_file = config["node-pool"]["nodes"][node_str]["config"]
            else:
                config_file = None
            # Parse master key if it exists
            if "master_key" in config["node-pool"]["nodes"][node_str]:
                master_key = config["node-pool"]["nodes"][node_str]["master_key"]
            else:
                master_key = None
            node_log_file = config["node-pool"]["nodes"][node_str]["log_file"]

            while process_pids[node] == 0 or not self.check_process_alive(witnet_binary, logger, process_pids[node]):
                logger.info("Starting and syncing node")

                original_dir = os.getcwd()

                os.chdir(binary_path)

                command_line = witnet_exec
                if config_file:
                    command_line += f" -c {config_file}"
                command_line += " node server"
                if master_key:
                    command_line += f" --master-key-import {master_key}"
                command_line += f" > {node_log_file} 2> {node_log_file}"
                logger.info(command_line)
                p = subprocess.Popen(command_line, preexec_fn=os.setsid, shell=True)
                process_pids[node] = p.pid

                os.chdir(original_dir)

                # Give the node some time to properly start
                time.sleep(10)

                # Connect to the node
                if self.connect_to_node(logger, node_str, node_sockets[node]):
                    break
        else:
            # Connect to the node, if this fails, we cannot use the remote node
            if not self.connect_to_node(logger, node_str, node_sockets[node]):
                logger.warning("Failed to connect to the remote node")
                node_synced[node] = False
                # Do not release the node lock so this node is never restarted
                return

        # Wait for the node to sync
        total_wait_time = 0
        while not self.check_node_synced(logger, node, request_counter, node_sockets[node]):
            logger.info("Waiting for the node to synchronize")
            # Check if we have sufficient peers
            outbound_peers = self.count_outbound_peers(logger, node, request_counter, node_sockets[node])
            if outbound_peers < config["node-pool"]["nodes"]["outbound_connections"]:
                logger.info(f"Not enough peers ({outbound_peers}) found to synchronize node")
                total_wait_time += sync_sleep
            # Reset the wait counter so we only restart the node after a continuous time of "no_peers_restart" seconds
            else:
                logger.info(f"Found enough peers ({outbound_peers}), synchronizing node")
                total_wait_time = 0
            # If the "total_wait_time" has elapsed, restart the node, hoping for more luck to find peers
            if total_wait_time > no_peers_restart:
                logger.warning(f"Node failed to find sufficient peers after {no_peers_restart}s")
                if node_type == "local":
                    # Restart node, do not pass node_synced and node_lock because we already have those
                    self.restart_node(node, config, request_counter, process_pids, node_sockets, logging_queue, node_synced)
                    break
                else:
                    logger.warning("Remote node did not find enough peers, giving up")
                    node_synced[node] = False
                    # Do not release the node lock so this node is never restarted
                    return
            # If the node died during synchronization, check_node_synced will return false due to socket errors, so also check if it is still running
            if node_type == "local" and not self.check_process_alive(witnet_binary, logger, process_pids[node]):
                logger.warning("Node died during the synchronization process")
                # Restart node, do not pass node_synced and node_lock because we already have those
                self.restart_node(node, config, request_counter, process_pids, node_sockets, logging_queue, node_synced)
                break
            time.sleep(sync_sleep)

        if node_synced:
            node_synced[node] = True
            if node_type == "local":
                logger.info(f"Local node synced ({process_pids[node]})")
            else:
                logger.info("Remote node synced")

        if node_lock:
            node_lock[node].release()

    def connect_to_node(self, logger, prefix, node_socket):
        logger.info("Connecting to node")
        try:
            node_socket.connect()
            return True
        except socket.error as e:
            if e.errno == errno.EISCONN:
                logger.info("Node is running, socket already connected")
                return True
            logger.warning(f"Socket connection error: {os.strerror(e.errno)} ({e.errno})")
            return False

    def count_outbound_peers(self, logger, node, request_counter, node_socket):
        node_str = f"node-{node  + 1}"
        logger.info("count_outbound_peers()")
        with request_counter.get_lock():
            request = {"jsonrpc": "2.0", "method": "peers", "id": str(request_counter.value)}
            request_counter.value += 1
        try:
            response = self.execute_request(logger, node_socket, request)
        except socket.error as e:
            logger.warning(f"Could not execute request: {os.strerror(e.errno)} ({e.errno})")
            return 0

        if response and "error" in response:
            logger.warning(f"Could not execute request: {response['error']}")
            return 0
        elif response and "result" in response:
            return sum([1 for peer in response["result"] if peer["type"] == "outbound"])
        else:
            logger.error(f"Could not execute request: {response}")
            return 0

    def check_node_synced(self, logger, node, request_counter, node_socket):
        node_str = f"node-{node  + 1}"
        logger.info(node_str + " get_sync_status()")
        with request_counter.get_lock():
            request = {"jsonrpc": "2.0", "method": "syncStatus", "id": str(request_counter.value)}
            request_counter.value += 1
        try:
            response = self.execute_request(logger, node_socket, request)
        except socket.error as e:
            logger.warning(f"{node_str} could not execute request: {os.strerror(e.errno)} ({e.errno})")
            return False

        if response and "result" in response:
            response = response["result"]
            if response["node_state"] != "Synced":
                logger.info(node_str + " not synced")
            return response["node_state"] == "Synced"
        else:
            logger.info(node_str + " not synced")
            if response and "error" in response and "reason" in response:
                logger.warning(f"Request returned an unexpected value: {response['error']}, {response['reason']}")
            elif response and "error" in response:
                logger.warning(f"Request returned an unexpected value: {response['error']}")
            return False

    def check_process_alive(self, witnet_binary, logger, pid):
        logger.info(f"Checking if process {pid} is alive")
        # Check if a process with this PID exists
        if psutil.pid_exists(pid):
            try:
                process = psutil.Process(pid)
                # Check if the process has a child named witnet (witnet processes are started using subprocess, so are children)
                for child in process.children(recursive=True):
                    if child.name() == witnet_binary:
                        logger.info(f"Found witnet process with PID {pid}")
                        return True
            # If the process dies between the PID check and checking its name, catch the exception
            except psutil.NoSuchProcess:
                pass
        logger.warning(f"No witnet process with PID {pid} found")
        return False

    def terminate_process(self, witnet_binary, logger, pid, socket=None):
        logger.info(f"Terminating process ({pid})")
        # If a socket was supplied, disconnect from it
        if socket:
            socket.disconnect()
        # If the supplied PID was non zero and the process group still exists, kill it
        if pid != 0 and self.check_process_alive(witnet_binary, logger, pid):
            try:
                os.killpg(pid, signal.SIGINT)
            # Just make sure the node pool does not crash even though this process most definitely should exist
            except ProcessLookupError:
                logger.warning(f"Terminating process ({pid}) failed")
                pass

    def restart_node(self, node, config, request_counter, process_pids, node_sockets, logging_queue, node_synced, node_lock=None):
        # Check if we need to acquire the lock in a blocking manner (we may already have it)
        if node_lock:
            node_lock[node].acquire()

        node_str = f"node-{node  + 1}"

        # If we don't have a logger, we are being called as a new process, create one
        self.configure_logging_process(logging_queue, node_str)
        logger = logging.getLogger(node_str)

        # Get binary name
        witnet_binary = os.path.basename(config["node-pool"]["nodes"]["binary"])
        # Terminate node
        self.terminate_process(witnet_binary, logger, process_pids[node], socket=node_sockets[node])
        # Give the process some time to properly terminate
        time.sleep(1)
        while self.check_process_alive(witnet_binary, logger, process_pids[node]):
            logger.warning(f"Node ({process_pids[node]}) is still alive")
            time.sleep(1)
        # Recreate socket
        node_ip = config["node-pool"]["nodes"][node_str]["ip"]
        node_port = config["node-pool"]["nodes"][node_str]["port"]
        node_sockets[node] = SocketManager(node_ip, node_port, 15)
        # Restart node
        self.start_node(node, config, request_counter, process_pids, node_sockets, logging_queue, node_synced, node_lock)
        # Node lock was released by start_node

    def execute_request(self, logger, node_socket, request):
        response = node_socket.query(request)
        logger.debug(f"Result for {request}: {response}")
        return response

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="node_pool.toml", dest="config_file")
    options, args = parser.parse_args()

    # Load config file
    config = toml.load(options.config_file)

    # Start logging process
    logging_queue = Queue()
    listener = Process(target=create_logging_listener, args=(config["node-pool"]["log"], logging_queue))
    listener.start()

    # Create node pool
    node_pool = NodePool(config, logging_queue)

    # Catch ctrl+c signal
    def signal_handler(*args):
        # Terminate all node pool processes
        witnet_binary = os.path.basename(config["node-pool"]["nodes"]["binary"])
        node_pool.terminate(witnet_binary)
        # End the logging process
        logging_queue.put(None)
        # Sleep 1 second to make sure everything ended
        time.sleep(1)
        # Exit cleanly
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)

    node_pool.start()

if __name__ == "__main__":
    main()
