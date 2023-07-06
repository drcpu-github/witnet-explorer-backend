import toml

from flask_caching import Cache

from .gunicorn_config import TOML_CONFIG
from .gunicorn_config import threads

config = toml.load(TOML_CONFIG)

if config["api"]["cache_server"] == "memcached":
    config = {
        "CACHE_TYPE": "SASLMemcachedCache",
        "CACHE_MEMCACHED_SERVERS": config["api"]["caching"]["server"].split(","),
        "CACHE_DEFAULT_TIMEOUT": 0,
        "CACHE_KEY_PREFIX": None,
        "CACHE_MEMCACHED_USERNAME": config["api"]["caching"]["user"],
        "CACHE_MEMCACHED_PASSWORD": config["api"]["caching"]["password"],
        "CACHE_NUM_THREADS": threads * 2, # Threads used by the SASLMemcachedCache thread pool instance to serve concurrent requests
        "CACHE_BLOCKING_POOL": True, # Set thread pool to blocking such that it always waits for a free thread and returns a value
        "CACHE_OPTIONS": {
            "behaviors": {
                "tcp_nodelay": True, # Faster IO
                "tcp_keepalive": True, # Keep connection alive
                "connect_timeout": 2000, # Connection timeout in ms
                "send_timeout": 750 * 1000, # Send timeout in us
                "receive_timeout": 750 * 1000, # Receive timeout in us
                "_poll_timeout": 2000, # Polling timeout in ms
                "ketama": True, # Set better failover
                "remove_failed": 1,
                "retry_timeout": 2,
                "dead_timeout": 30,
            }
        }
    }

    cache = Cache(config=config)
else:
    cache = None