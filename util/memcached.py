import time

def calculate_timeout(timeout):
    # Memcached timeouts bigger than 30 days needs to be specified as a unix timestamp
    if timeout > 60*60*24*30:
        timeout = int(time.time()) + timeout
    return timeout
