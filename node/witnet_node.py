#!/usr/bin/python3

import json
import logging
import logging.handlers
import os
import re
import socket
import sys

from util.data_transformer import hex2bytes
from util.socket_manager import SocketManager

class WitnetNode(object):
    request_id = 1

    def __init__(self, node_config, timeout=0, logger=None, log_queue=None, log_label=""):
        # If a timeout is specified, save it here so it can be propagated into the request
        self.request_timeout = timeout if timeout != node_config["default_timeout"] else 0

        # Set the local socket to the default timeout or the one passed to the constructor
        socket_timeout = node_config["default_timeout"] if timeout == 0 else timeout
        self.socket_mngr = SocketManager(node_config["host"], node_config["port"], socket_timeout)
        self.socket_mngr.connect()

        self.vtt_regex = re.compile(r'\{"transaction":\{"ValueTransfer":\{"body":\{"inputs":\[(\{"output_pointer":"\w{64}:\d{1,3}"\},*)+\],"outputs":\[(\{"pkh":"wit1\w{38}","time_lock":\d+,"value":\d+\},*)+\]\},"signatures":\[(\{"public_key":\{"bytes":"[a-f0-9]+","compressed":\d+\},"signature":\{"Secp256k1":\{"der":"[a-f0-9]+"\}\}\},*)+\]\}\}\}')

        # Set up logger
        if logger:
            self.logger = logger
        elif log_queue:
            self.configure_logging_process(log_queue, log_label)
            self.logger = logging.getLogger(log_label)
        else:
            self.logger = None

    def close_connection(self):
        self.socket_mngr.close_connection()

    def configure_logging_process(self, queue, label):
        handler = logging.handlers.QueueHandler(queue)
        root = logging.getLogger(label)
        root.handlers = []
        root.addHandler(handler)
        root.setLevel(logging.DEBUG)

    #################
    # RPC functions #
    #################

    def get_consensus_constants(self):
        if self.logger:
            self.logger.info("get_consensus_constants()")
        request = {"jsonrpc": "2.0", "method": "getConsensusConstants", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_block(self, block_hash):
        if self.logger:
            self.logger.info("get_block("+ str(block_hash) + ")")
        request = {"jsonrpc": "2.0", "method": "getBlock", "params": [block_hash], "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    # Return a <num_blocks> hashes, starting at <epoch>
    # <num_blocks>: default is 0 (returning all blocks), a negative value returns the hashes for the last x epochs
    # <epoch>: default is 0 (start at epoch 0), a negative value returns the hashes for the last x epochs
    # Note that one should combine a positive <epoch> with a positive <num_blocks>
    def get_blockchain(self, epoch=0, num_blocks=0):
        if self.logger:
            self.logger.info(f"get_blockchain({epoch}, {num_blocks})")
        request = {"jsonrpc": "2.0", "method": "getBlockChain", "params": [epoch, num_blocks], "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_address(self):
        if self.logger:
            self.logger.info("get_address()")
        request = {"jsonrpc": "2.0", "method": "getPkh", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_balance(self, node_address, simple=True):
        if self.logger:
            self.logger.info(f"get_balance({node_address}, {simple})")
        request = {"jsonrpc": "2.0", "method": "getBalance", "params": {"pkh": node_address, "simple": simple}, "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_balance_all(self):
        if self.logger:
            self.logger.info("get_balance_all()")
        request = {"jsonrpc": "2.0", "method": "getBalanceAll", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_reputation(self, node_address):
        if self.logger:
            self.logger.info(f"get_reputation({node_address})")
        request = {"jsonrpc": "2.0", "method": "getReputation", "params": [node_address], "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_reputation_all(self):
        if self.logger:
            self.logger.info("get_reputation_all()")
        request = {"jsonrpc": "2.0", "method": "getReputationAll", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_transaction(self, txn_hash):
        if self.logger:
            self.logger.info(f"get_transaction({txn_hash})")
        request = {"jsonrpc": "2.0", "method": "getTransaction", "params": [txn_hash], "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_data_request_report(self, txn_hash):
        if self.logger:
            self.logger.info(f"get_data_request_report({txn_hash})")
        request = {"jsonrpc": "2.0", "method": "dataRequestReport", "params": [txn_hash], "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_sync_status(self):
        if self.logger:
            self.logger.info("get_sync_status()")
        request = {"jsonrpc": "2.0", "method": "syncStatus", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_known_peers(self):
        if self.logger:
            self.logger.info("get_known_peers()")
        request = {"jsonrpc": "2.0", "method": "knownPeers", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_mempool(self):
        if self.logger:
            self.logger.info("get_mempool()")
        request = {"jsonrpc": "2.0", "method": "getMempool", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_current_epoch(self):
        if self.logger:
            self.logger.info("get_current_epoch()")
        blockchain = self.get_blockchain(-1, -1)
        return blockchain[0][0]

    def get_supply_info(self):
        if self.logger:
            self.logger.info("get_supply_info()")
        request = {"jsonrpc": "2.0", "method": "getSupplyInfo", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def get_utxos(self, address):
        if self.logger:
            self.logger.info(f"get_utxos({address})")
        request = {"jsonrpc": "2.0", "method": "getUtxoInfo", "params": [address], "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def send_vtt(self, vtt, test):
        if self.logger:
            self.logger.info(f"send_vtt({vtt}, {test})")
        if self.vtt_regex.match(vtt):
            try:
                vtt_transformed = json.loads(vtt)
            except json.decoder.JSONDecodeError:
                return {"error": "could not decode request, invalid JSON format"}

            for idx, signature in enumerate(vtt_transformed["transaction"]["ValueTransfer"]["signatures"]):
                vtt_transformed["transaction"]["ValueTransfer"]["signatures"][idx] = {
                    "public_key": {
                        "bytes": hex2bytes(signature["public_key"]["bytes"]),
                        "compressed": signature["public_key"]["compressed"],
                    },
                    "signature": {
                        "Secp256k1": {
                            "der": hex2bytes(signature["signature"]["Secp256k1"]["der"]),
                        }
                    }
                }

            if test:
                return {"result": "the VTT passed the regular expression check"}
            else:
                request = {"jsonrpc": "2.0", "method": "inventory", "params": vtt_transformed, "id": str(WitnetNode.request_id)}
                return self.execute_request(request)
        else:
            if self.logger:
                self.logger.warn(f"VTT {vtt} does not pass the regex test")
            return {"error": "invalid VTT format"}

    def get_priority(self):
        if self.logger:
            self.logger.info("get_priority()")
        request = {"jsonrpc": "2.0", "method": "priority", "id": str(WitnetNode.request_id)}
        return self.execute_request(request)

    def execute_request(self, request):
        WitnetNode.request_id += 1
        if self.request_timeout:
            request["timeout"] = self.request_timeout
        response = self.socket_mngr.query(request)
        if self.logger:
            # Always log errors as warning
            if type(response) is dict and "error" in response:
                self.logger.warning(f"Result for {request}: {response}")
            # Otherwise log as debug
            else:
                log_response = response
                if len(str(response)) > 1000:
                    log_response = str(response)[:500] + "..." + str(response)[-500:]
                self.logger.debug(f"Result for {request}: {log_response}")
        return response
