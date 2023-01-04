import json
import logging
import socket

from prometheus_client import Gauge

class NodeMonitor():
    def __init__(self, config, debug=False):
        self.node_rpcs = config["nodes"]["rpc"]

        logging.debug(f"Found {len(self.node_rpcs)} node definitions")

        self.current_epoch, self.synchronization_status = [], []
        self.current_epoch_gauges, self.synchronization_status_gauges = [], []
        for n in range(len(self.node_rpcs)):
            self.current_epoch.append(0)
            self.synchronization_status.append("Offline")

            # Create Prometheus gauges if the exporter was not started in debug mode
            if not debug:
                self.current_epoch_gauges.append(Gauge(f"node_epoch_node_{n + 1}", f"Current epoch for Witnet node {n + 1}"))
                self.synchronization_status_gauges.append(Gauge(f"node_synchronization_status_node_{n + 1}", f"Synchronization status for Witnet node {n + 1}"))

        self.synchronization_request = {"jsonrpc": "2.0", "method": "syncStatus", "id": "1"}

    def connect(self, node_idx):
        logging.debug(f"Connect to socket ({self.node_rpcs[node_idx]}) for node {node_idx}")

        self.node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Try to connect to the node RPC
        ip = self.node_rpcs[node_idx].split(":")[0]
        port = int(self.node_rpcs[node_idx].split(":")[1])
        try:
            self.node_socket.connect((ip, port))
        except ConnectionRefusedError:
            logging.warning(f"Could not connect to node {node_idx}")
            return False
        return True

    def disconnect(self, node_idx):
        logging.debug(f"Disconnect from socket ({self.node_rpcs[node_idx]}) for node {node_idx}")
        self.node_socket.close()

    def get_sync_status(self, node_idx):
        # Send request
        try:
            self.node_socket.send((json.dumps(self.synchronization_request) + "\n").encode("utf-8"))
        except socket.error:
            logging.warning(f"Could not send synchronization status request to node {node_idx}")
            return

        # Receive response
        response = ""
        while True:
            try:
                response += self.node_socket.recv(1024).decode("utf-8")
                if len(response) == 0 or response[-1] == "\n":
                    break
            except socket.error as e:
                logging.warning(f"Could not retrieve synchronization status from node {node_idx}")
                return

        logging.debug(f"Response for synchronization status request of node {node_idx}: {response}")

        # Decode response if it was not empty
        if response != "":
            try:
                response = json.loads(response)
            except json.decoder.JSONDecodeError:
                logging.warning(f"Could not decode synchronization response from node {node_idx}")
                return
        else:
            logging.warning(f"No synchronization response received from node {node_idx}")
            return

        # Save node status variables
        if "result" in response:
            self.current_epoch[node_idx] = response["result"]["current_epoch"]
            self.synchronization_status[node_idx] = response["result"]["node_state"]
        else:
            logging.warning(f"Could not parse result received from node {node_idx}")

    def collect(self):
        for node_idx in range(len(self.node_rpcs)):
            # Reset status variables
            self.current_epoch[node_idx] = 0
            self.synchronization_status[node_idx] = "Offline"

            # Get synchronization status
            success = self.connect(node_idx)
            if success:
                logging.debug(f"Fetching synchronization status for node {node_idx}")
                self.get_sync_status(node_idx)
                self.disconnect(node_idx)

    def save(self, debug=False):
        # Save status variables for each of the nodes or log debug info
        for node_idx in range(len(self.node_rpcs)):
            logging.debug(f"Current epoch for node {node_idx}: {self.current_epoch[node_idx]}")
            if self.current_epoch[node_idx] != 0:
                if debug:
                    logging.debug(f"Current epoch for node {node_idx}: {self.current_epoch[node_idx]}")
                else:
                    self.current_epoch_gauges[node_idx].set(self.current_epoch[node_idx])

            if debug:
                logging.debug(f"Synchronization status for node {node_idx}: {self.synchronization_status[node_idx]}")
            else:
                if self.synchronization_status[node_idx] == "Offline":
                    self.synchronization_status_gauges[node_idx].set(0)
                elif self.synchronization_status[node_idx] == "WaitingConsensus":
                    self.synchronization_status_gauges[node_idx].set(1)
                elif self.synchronization_status[node_idx] == "AlmostSynced":
                    self.synchronization_status_gauges[node_idx].set(2)
                elif self.synchronization_status[node_idx] == "Synced":
                    self.synchronization_status_gauges[node_idx].set(3)
