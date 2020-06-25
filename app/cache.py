from flask_caching import Cache

CacheConfig = {
    "DEBUG": True,
    "CACHE_TYPE": "simple",
    "CACHE_DEFAULT_TIMEOUT": 300
}

cache = Cache(config=CacheConfig)
