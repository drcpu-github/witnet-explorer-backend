import json


class MockWitnetNode(object):
    def init_app(self, app):
        app.extensions = getattr(app, "extensions", {})
        if "witnet_node" not in app.extensions:
            app.extensions["witnet_node"] = self

    def get_block(self, block_hash):
        blocks = json.load(open("mockups/data/blocks.json"))
        return {"result": blocks[block_hash]["rpc"]}

    def get_reputation_all(self):
        reputation = json.load(open("mockups/data/reputation.json"))
        return {"result": reputation["rpc"]}

    def get_transaction(self, txn_hash):
        pass

    def get_sync_status(self):
        sync_status = json.load(open("mockups/data/sync_status.json"))
        return {"result": sync_status}

    def get_mempool(self):
        mempool = json.load(open("mockups/data/mempool.json"))
        return {"result": mempool}

    def get_utxos(self, address):
        address_data = json.load(open("mockups/data/address_data.json"))
        return {"result": {"utxos": address_data[address]["utxos"]}}

    def send_vtt(self, vtt):
        return {"result": 1}

    def get_priority(self):
        priority = json.load(open("mockups/data/priority.json"))
        return {"result": priority}

    def get_current_epoch(self):
        blockchain = self.get_blockchain(-1, -1)
        if "error" not in blockchain:
            return blockchain["result"][0][0]
        return 0
