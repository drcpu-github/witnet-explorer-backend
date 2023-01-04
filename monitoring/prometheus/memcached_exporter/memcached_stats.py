import logging
import subprocess

from prometheus_client import Counter
from prometheus_client import Gauge

class MemcachedStats():
    def __init__(self, config, debug=False):
        self.config = config

        self.build_memcstat_cmd_line()

        if "stats" not in self.config["prometheus"]:
            logging.debug("No subset of stats found to save, defaulting to all stats")
            self.save_all_stats = True
        else:
            self.save_all_stats = False
            self.stats_to_save = config["prometheus"]["stats"]

        # Info gauges
        self.memcached_info = {
            "uptime": "Number of seconds since the server started",
            "rusage_user": "Accumulated user time for this process",
            "rusage_system": "Accumulated system time for this process",
            "pointer_size": "Size of memory pointers (32 or 64)",
            "max_connections": "Max number of simultaneous connections",
            "limit_maxbytes": "Number of bytes this server is allowed to use for storage",
            "threads": "Number of worker threads requested",
            "connection_structures": "Number of connection structures allocated by the server",
            "reserved_fds": "Number of misc fds used internally",
        }

        self.collect_stats(_all=True)

        # Create info gauges
        if not debug:
            self.memcached_gauges = {}
            for key, info in self.memcached_info.items():
                self.memcached_gauges[key] = Gauge(f"memcached_info_{key}", info)
                self.memcached_gauges[key].set(self.stats[key])

        # Counters increase monotonically, and reset when the process (re)starts
        self.memcached_counters_info = {
            "total_items": "Total number of items stored since the server started",
            "total_connections": "Total number of connections opened since the server started running",
            "rejected_connections": "Connections rejected in maxconns_fast mode",
            "cmd_get": "Cumulative number of retrieval reqs",
            "cmd_set": "Cumulative number of storage reqs",
            "cmd_flush": "Cumulative number of flush reqs",
            "cmd_touch": "Cumulative number of touch reqs",
            "get_hits": "Number of keys that have been requested and found present",
            "get_misses": "Number of items that have been requested and not found",
            "get_expired": "Number of items that have been requested but had already expired",
            "get_flushed": "Number of items that have been requested but have been flushed via flush_all",
            "delete_misses": "Number of deletions reqs for missing keys",
            "delete_hits": "Number of deletion reqs resulting in an item being removed",
            "incr_misses": "Number of incr reqs against missing keys",
            "incr_hits": "Number of successful incr reqs",
            "decr_misses": "Number of decr reqs against missing keys",
            "decr_hits": "Number of successful decr reqs",
            "cas_misses": "Number of CAS reqs against missing keys",
            "cas_hits": "Number of successful CAS reqs",
            "cas_badval": "Number of CAS reqs for which a key was found, but the CAS value did not match",
            "touch_hits": "Number of keys that have been touched with a new expiration time",
            "touch_misses": "Number of items that have been touched and not found",
            "auth_cmds": "Number of authentication commands handled, success or failure",
            "auth_errors": "Number of failed authentications",
            "evictions": "Number of valid items removed from cache to free memory for new items",
            "reclaimed": "Number of times an entry was stored using memory from an expired entry",
            "bytes_read": "Total number of bytes read by this server from network",
            "bytes_written": "Total number of bytes sent by this server to network",
            "listen_disabled_num": "Number of times server has stopped accepting new connections (maxconns)",
            "time_in_listen_disabled_us": "Number of microseconds in maxconns",
            "conn_yields": "Number of times any connection yielded to another due to hitting the -R limit",
            "expired_unfetched": "Items pulled from LRU that were never touched by get/incr/append/etc before expiring",
            "evicted_unfetched": "Items evicted from LRU that were never touched by get/incr/append/etc",
            "evicted_active": "Items evicted from LRU that had been hit recently but did not jump to top of LRU",
            "slabs_moved": "Total slab pages moved",
            "crawler_reclaimed": "Total items freed by LRU Crawler",
            "crawler_items_checked": "Total items examined by LRU Crawler",
            "lrutail_reflocked": "Times LRU tail was found with active ref, items can be evicted to avoid OOM errors",
            "moves_to_cold": "Items moved from HOT/WARM to COLD LRU's",
            "moves_to_warm": "Items moved from COLD to WARM LRU",
            "moves_within_lru": "Items reshuffled within HOT or WARM LRU's",
            "direct_reclaims": "Times worker threads had to directly reclaim or evict items",
            "lru_crawler_starts": "Times an LRU crawler was started",
            "lru_maintainer_juggles": "Number of times the LRU bg thread woke up",
            "slab_global_page_pool": "Slab pages returned to global pool for reassignment to other slab classes",
            "slab_reassign_rescues": "Items rescued from eviction in page move",
            "slab_reassign_evictions_nomem": "Valid items evicted during a page move (due to no free memory in slab)",
            "slab_reassign_chunk_rescues": "Individual sections of an item rescued during a page move",
            "slab_reassign_inline_reclaim": "Internal stat counter for when the page mover clears memory from the chunk freelist when it wasn't expecting to",
            "slab_reassign_busy_items": "Items busy during page move, requiring a retry before page can be moved",
            "slab_reassign_busy_deletes": "Items busy during page move, requiring deletion before page can be moved",
            "log_worker_dropped": "Logs a worker never wrote due to full buf",
            "log_worker_written": "Logs written by a worker, to be picked up",
            "log_watcher_skipped": "Logs not sent to slow watchers",
            "log_watcher_sent": "Logs written to watchers",
        }

        # Only create counters for requested statistics
        if not debug:
            self.memcached_counters = {}
            for key, info in self.memcached_counters_info.items():
                if self.save_all_stats or key in self.stats_to_save:
                    # We track the past value so we can increase the Counter value correctly
                    self.memcached_counters[key] = [0, Counter(f"memcached_counter_{key}", info)]

        # Gauges can increase or decrease
        self.memcached_gauges_info = {
            "accepting_conns": "Whether or not server is accepting conns",
            "hash_is_expanding": "Indicates if the hash table is being grown to a new size",
            "slab_reassign_running": "If a slab page is being moved",
            "curr_items": "Current number of items stored",
            "bytes": "Current number of bytes used to store items",
            "curr_connections": "Number of open connections",
            "hash_power_level": "Current size multiplier for hash table",
            "hash_bytes": "Bytes currently used by hash tables",
        }

        # Only create gauges for requested statistics
        if not debug:
            self.memcached_gauges = {}
            for key, info in self.memcached_gauges_info.items():
                if self.save_all_stats or key in self.stats_to_save:
                    self.memcached_gauges[key] = Gauge(f"memcached_gauge_{key}", info)

    def build_memcstat_cmd_line(self):
        self.command = ["memcstat"]

        if "server" in self.config["memcached"]:
            self.command.extend(["--servers", self.config["memcached"]["server"]])
        else:
            logging.debug("No server configured, defaulting to localhost:11211")
            self.command.extend(["--servers", "127.0.0.1:11211"])

        if "user" in self.config["memcached"] and "password" in self.config["memcached"]:
            self.command.extend(["--username", self.config["memcached"]["user"]])
            self.command.extend(["--password", self.config["memcached"]["password"]])
        else:
            logging.debug("No user and password found, assuming SASL authentication is not enabled")

    def collect_stats(self, _all=False):
        # Reset stats dictionary
        self.stats = None

        # Execute command
        p = subprocess.Popen(self.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()

        if len(stdout) == 0:
            logging.error(f"Could not contact memcached server")

        if len(stderr) > 0:
            logging.error(f"Received error: {stderr.decode('utf-8')}")
        else:
            self.parse_memcstat_output(stdout.decode("utf-8"), _all=_all)

    def parse_memcstat_output(self, output, _all=False):
        self.stats = {}
        lines = output.splitlines()
        for line in lines:
            stat, value = line.strip().split(":")
            # Skip stat conversion if we won't save it
            if not (self.save_all_stats or _all) and stat not in self.stats_to_save:
                continue
            try:
                # Try to convert to an integer
                value = int(value)
                self.stats[stat] = value
            except ValueError:
                try:
                    # Try to convert to a float
                    value = float(value)
                    self.stats[stat] = value
                except ValueError:
                    # Save as a string
                    self.stats[stat] = value.strip()

    def save_stats(self, debug=False):
        if self.stats == {}:
            return

        for stat, value in self.stats.items():
            # Update counter value
            if stat in self.memcached_counters:
                # Check if the current value is bigger or equal and if so increment with the difference
                if value >= self.memcached_counters[stat][0]:
                    if debug:
                        logging.debug(f"Incrementing {stat} counter with {value - self.memcached_counters[stat][0]} to {value}")
                    else:
                        self.memcached_counters[stat][1].inc(value - self.memcached_counters[stat][0])
                # If not, the Memcached process likely was restarted, so increment with the current value
                else:
                    if debug:
                        logging.debug(f"Incrementing {stat} counter with {value} to {value + self.memcached_counters[stat][0]}")
                    else:
                        self.memcached_counters[stat][1].inc(value)
                self.memcached_counters[stat][0] = value

            # Update gauge value
            if stat in self.memcached_gauges:
                if debug:
                    logging.debug(f"Setting {stat} gauge to {value}")
                else:
                    self.memcached_gauges[stat].set(value)
