import time

from util.common_sql import sql_epoch_times

def calculate_block_reward(epoch, halving_period, initial_block_reward):
    halvings = int(epoch / halving_period)
    if halvings < 64:
        return initial_block_reward >> halvings
    else:
        return 0

# Round the priority to the nearest integer value but handle the actual zero-fee transactions and round-to-zero fee transactions differently
def calculate_priority(fee, weight, round_priority=False):
    if round_priority:
        if fee == 0:
            return 0
        elif round(fee / weight) == 0:
            return 1
        else:
            return round(fee / weight)
    else:
        return fee / weight

def calculate_current_epoch(start_time, epoch_period):
    return int((time.time() - start_time) / epoch_period)

def calculate_timestamp_from_epoch(start_time, epoch_period, epoch):
    return start_time + epoch_period * (epoch + 1)

def calculate_epoch_from_timestamp(start_time, epoch_period, timestamp):
    return int((timestamp - start_time) / epoch_period)

def send_address_caching_request(logger, caching_server, request):
    try:
        caching_server.send_request(request)
    except ConnectionRefusedError:
        logger.warning(f"Could not send {request['method']} request to address caching server")
        try:
            caching_server.recreate_socket()
            caching_server.send_request(request)
        except ConnectionRefusedError:
            logger.warning(f"Could not recreate socket, will try again next {request['method']} request")

def get_network_times(database):
    times = database.sql_return_all(sql_epoch_times)
    start_time, epoch_period = 0, 0
    for t in times:
        if t[0] == "checkpoint_zero_timestamp":
            start_time = t[1]
        if t[0] == "checkpoints_period":
            epoch_period = t[1]
    assert start_time != 0 and epoch_period != 0
    return start_time, epoch_period
