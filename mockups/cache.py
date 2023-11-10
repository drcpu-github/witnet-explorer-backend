import copy
import json


class MockCache(object):
    def __init__(self):
        self.cache = {}

        self.cache["home"] = json.load(open("mockups/data/home.json"))

        self.cache["status"] = json.load(open("mockups/data/status.json"))

        self.cache["transaction_mempool"] = json.load(open("mockups/data/mempool.json"))

        self.cache["priority"] = json.load(open("mockups/data/priority.json"))

        reputation = json.load(open("mockups/data/reputation.json"))
        self.cache["reputation"] = reputation["cache"]

        blocks = json.load(open("mockups/data/blocks.json"))
        for block_hash, block in blocks.items():
            self.cache[str(block["cache"]["block"]["details"]["epoch"])] = block_hash
            self.cache[block_hash] = block["cache"]

        for fh in (
            "balances",
            "blockchain",
            "commits",
            "data_requests",
            "data_request_reports",
            "mints",
            "network_mempool",
            "network_statistics",
            "reveals",
            "tallies",
            "tapi",
            "value_transfers",
        ):
            data = json.load(open(f"mockups/data/{fh}.json"))
            for key, value in data.items():
                self.cache[key] = value

        address_data = json.load(open("mockups/data/address_data.json"))
        for address, data in address_data.items():
            for key, value in data.items():
                self.cache[f"{address}_{key}"] = value

    def init_app(self, app):
        app.extensions = getattr(app, "extensions", {})
        if "cache" not in app.extensions:
            app.extensions["cache"] = self

    def get(self, key):
        if key in self.cache:
            return copy.deepcopy(self.cache[key])
        return None

    def set(self, key, value, timeout=0):
        self.cache[key] = value
        return True

    def delete(self, key):
        del self.cache[key]
        return True
