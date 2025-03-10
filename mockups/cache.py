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
            epoch = str(block["processed"]["api"]["block"]["details"]["epoch"])
            self.cache[epoch] = block_hash
            self.cache[block_hash] = block["processed"]["api"]

        for fh in (
            "balances",
            "blockchain",
            "network_mempool",
            "network_statistics",
            "tapi",
        ):
            data = json.load(open(f"mockups/data/{fh}.json"))
            for key, value in data.items():
                self.cache[key] = value

        address_data = json.load(open("mockups/data/address_data.json"))
        for address, data in address_data.items():
            for key, value in data.items():
                self.cache[f"{address}_{key}"] = value

        network_versions = json.load(open("mockups/data/network_versions.json"))["api"]
        self.cache["network_version_all"] = network_versions
        self.cache["network_version_current"] = {
            "current_version": network_versions["current_version"],
            "current_epoch": network_versions["current_epoch"],
        }

    def init_app(self, app):
        app.extensions = getattr(app, "extensions", {})
        if "cache" not in app.extensions:
            app.extensions["cache"] = self

    def load_json_into_cache(self, file_handle, response_type):
        data = json.load(open(f"mockups/data/{file_handle}"))
        for key, value in data.items():
            if "processed" in value and "database" in value["processed"]:
                value = value["processed"]["database"]
            self.cache[key] = {
                "response_type": response_type,
                response_type: value,
            }

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
