from contextlib import contextmanager
from queue import Queue

from node.witnet_node import WitnetNode

class WitnetClientPool(Queue):
    def __init__(self, config):
        clients = config["nodes"]["number"]
        Queue.__init__(self, clients)
        for i in range(clients):
            self.put(WitnetNode(config))

    def init_app(self, app):
        app.extensions = getattr(app, "extensions", {})
        if "witnet_node" not in app.extensions:
            app.extensions["witnet_node"] = self

    # Yield a node to use in a with statement
    # If block is True, wait until one is free
    # Otherwise raise a queue Full exception
    @contextmanager
    def reserve(self, block=True):
        node = self.get(block)
        try:
            yield node
        finally:
            self.put(node)

    ############################################
    # RPC functions with a connection resource #
    ############################################

    def get_consensus_constants(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_consensus_constants()

    def get_block(self, block_hash):
        with self.reserve() as witnet_node:
            return witnet_node.get_block(block_hash)

    def get_blockchain(self, epoch=0, num_blocks=0):
        with self.reserve() as witnet_node:
            return witnet_node.get_blockchain(epoch=epoch, num_blocks=num_blocks)

    def get_balance(self, node_address, simple=True):
        with self.reserve() as witnet_node:
            return witnet_node.get_balance(node_address, simple=simple)

    def get_balance_all(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_balance_all()

    def get_reputation(self, node_address):
        with self.reserve() as witnet_node:
            return witnet_node.get_reputation(node_address)

    def get_reputation_all(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_reputation_all()

    def get_transaction(self, txn_hash):
        with self.reserve() as witnet_node:
            return witnet_node.get_transaction(txn_hash)

    def get_sync_status(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_sync_status()

    def get_known_peers(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_known_peers()

    def get_mempool(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_mempool()

    def get_supply_info(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_supply_info()

    def get_utxos(self, address):
        with self.reserve() as witnet_node:
            return witnet_node.get_utxos(address)

    def send_vtt(self, vtt):
        with self.reserve() as witnet_node:
            return witnet_node.send_vtt(vtt)

    def get_priority(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_priority()

    def get_current_epoch(self):
        with self.reserve() as witnet_node:
            return witnet_node.get_current_epoch()
