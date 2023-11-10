import time
import pylibmc

class MemcachedPool(object):
    def __init__(self, servers, username, password, threads, blocking):
        self.memcached_client = pylibmc.Client(
            servers,
            username=username,
            password=password,
            binary=True,
            behaviors={
                "tcp_nodelay": True,  # Faster IO
                "tcp_keepalive": True,  # Keep connection alive
                "connect_timeout": 2000,  # Connection timeout in ms
                "send_timeout": 750 * 1000,  # Send timeout in us
                "receive_timeout": 750 * 1000,  # Receive timeout in us
                "_poll_timeout": 2000,  # Polling timeout in ms
                "ketama": True,  # Set better failover
                "remove_failed": 1,
                "retry_timeout": 2,
                "dead_timeout": 30,
            }
        )
        self.cache = pylibmc.ClientPool()
        self.cache.fill(self.memcached_client, threads)
        self.blocking = blocking

    def get(self, key):
        with self.cache.reserve(block=self.blocking) as client:
            return client.get(key)

    def set(self, key, value, timeout=0):
        timeout = calculate_timeout(timeout)
        with self.cache.reserve(block=self.blocking) as client:
            return bool(client.set(key, value, timeout))

    def delete(self, key):
        with self.cache.reserve(block=self.blocking) as client:
            return bool(client.delete(key))

def calculate_timeout(timeout):
    # Memcached timeouts bigger than 30 days needs to be specified as a unix timestamp
    if timeout > 60*60*24*30:
        timeout = int(time.time()) + timeout
    return timeout
