import time

from blockchain.witnet_database import WitnetDatabase

from node.witnet_node import WitnetNode

class Blockchain(object):
    def __init__(self, database_config, node_config, consensus_constants, log_queue):
        db_user = database_config["user"]
        db_name = database_config["name"]
        db_pass = database_config["password"]
        self.witnet_database = WitnetDatabase(db_user, db_name, db_pass, log_queue=log_queue, log_label="db-blockchain")

        # Connect to node pool
        socket_host = node_config["host"]
        socket_port = node_config["port"]
        self.witnet_node = WitnetNode(socket_host, socket_port, 15, log_queue=log_queue, log_label="node-blockchain")

        self.start_time = consensus_constants.checkpoint_zero_timestamp
        self.epoch_period = consensus_constants.checkpoints_period

    def calculate_block_reward(self, epoch):
        initial_reward = 250000000000
        halvings = int(epoch / 1750000)
        if halvings < 64:
            return initial_reward >> halvings
        else:
            return 0

    # num = -10, start = 0, stop = 0 will return the last 10 blocks
    # start != 0, stop != 0 will return the blocks in the start to stop range
    # start != 0, return num blocks starting at start until the newest block
    # num > 0, stop != 0, return num blocks starting from stop and going backwards
    def get_blockchain(self, num, start, stop):
        if num < 0:
            start_at = 0
            num_blocks = num
        elif start != -1:
            start_at = start
            if stop != -1:
                num_blocks = stop - start
            else:
                num_blocks = 1000
        else:
            start_at = num - stop
            num_blocks = num

        info = ""
        if num_blocks > 1000:
            info = "Cannot fetch more than 1000 blocks in one call"

        result = self.witnet_node.get_blockchain(epoch=start_at, num_blocks=num_blocks)
        if type(result) is dict and "error" in result:
            return {"blockchain": [], "last_updated": int(time.time()), "info": result["error"]}
        else:
            result = result["result"]

        blockchain = [[block[1], block[0], self.start_time + (block[0] + 1) * self.epoch_period] for block in result]
        blockchain = sorted(blockchain, key=lambda l: l[1], reverse=True)

        return {"blockchain": blockchain, "last_updated": int(time.time()), "info": info}

    def get_blockchain_details(self, action, num, start, stop):
        blockchain_data = self.get_blockchain(num, start, stop)

        # create dict for easier statistics collection
        blockchain_dict = {}
        for block in blockchain_data["blockchain"]:
            # if there was a rollback, there will be too many blocks, ignore the ones bigger than stop
            if stop != -1 and block[1] + 1 > stop:
                continue
            blockchain_dict[block[1]] = [
                block[0],   # 0: block hash
                block[1],   # 1: block epoch
                block[2],   # 2: block timestamp
                "",         # 3: block minting address
                0,          # 4: number of value transfers
                0,          # 5: number of data requests
                0,          # 6: number of commit transactions
                0,          # 7: number of reveal transactions
                0,          # 8: number of tally transactions
                0,          # 9: block transaction fees
                False       # 10: confirmed
            ]
        epochs = list(blockchain_dict.keys())
        if len(epochs) == 0:
            return {"blockchain": [], "reverted": [], "last_updated": int(time.time()), "info": "no new blocks to fetch"}
        else:
            start_epoch = min(epochs)
            stop_epoch = max(epochs)

        sql = """
            SELECT
                blocks.value_transfer,
                blocks.data_request,
                blocks.commit,
                blocks.reveal,
                blocks.tally,
                blocks.epoch,
                blocks.confirmed,
                mint_txns.miner,
                mint_txns.output_values
            FROM blocks
            LEFT JOIN mint_txns ON 
                blocks.epoch=mint_txns.epoch
            WHERE
                blocks.epoch
        """
        if stop == -1:
            sql += " >= '%s'" % start_epoch
        else:
            sql += " BETWEEN '%s' AND '%s'" % (start_epoch, stop_epoch)
        blocks = self.witnet_database.sql_return_all(sql)

        # not all blocks were added to the database yet, which would result in incorrect data, wait for the next call to this function
        if len(blocks) < len(epochs):
            return {"blockchain": [], "reverted": [], "last_updated": int(time.time())}

        for block in blocks:
            value_transfer, data_request, commit, reveal, tally, epoch, confirmed, miner, output_values = block
            if epoch in blockchain_dict:
                blockchain_dict[epoch][3] = miner
                blockchain_dict[epoch][4] = value_transfer
                blockchain_dict[epoch][5] = data_request
                blockchain_dict[epoch][6] = commit
                blockchain_dict[epoch][7] = reveal
                blockchain_dict[epoch][8] = tally
                blockchain_dict[epoch][9] = sum(output_values) - self.calculate_block_reward(epoch)
                blockchain_dict[epoch][10] = confirmed

        # flatten dict
        blockchain, reverted_blocks = [], []
        if action == "init" or action == "append" or num < 0:
            sorted_epochs = sorted(list(blockchain_dict.keys()), reverse=True)
        else:
            sorted_epochs = sorted(list(blockchain_dict.keys()))
        previous_epoch = sorted_epochs[0]
        for epoch in sorted_epochs:
            blockchain.append(blockchain_dict[epoch])
            if epoch < previous_epoch - 1:
                reverted_blocks.extend(range(epoch, previous_epoch))
            previous_epoch = epoch

        return {"blockchain": blockchain, "reverted": reverted_blocks, "last_updated": int(time.time()), "info": blockchain_data["info"]}
