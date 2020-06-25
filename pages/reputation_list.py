import time

from node.witnet_node import WitnetNode

class ReputationList(object):
    def __init__(self, node_config, logging_queue):
        # Connect to node pool
        socket_host = node_config["host"]
        socket_port = node_config["port"]
        self.witnet_node = WitnetNode(socket_host, socket_port, 15, logging_queue, "node-reputation")

    def get_reputation_list(self):
        result = self.witnet_node.get_reputation_all()
        if type(result) is dict and "error" in result:
            return {}
        else:
            result = result["result"]
        stats = result["stats"]
        reputation = [(key, stats[key]["reputation"], stats[key]["eligibility"], stats[key]["is_active"]) for key in stats.keys()]
        reputation = sorted(reputation, key=lambda l: l[1], reverse=True)
        return {"reputation": reputation, "total_reputation": result["total_reputation"], "last_updated": int(time.time())}
