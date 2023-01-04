import logging
import optparse
import prometheus_client
import sys
import time
import toml

from memcached_stats import MemcachedStats

def setup_logging():
    # Configure logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Add header formatting of the log message
    logging.Formatter.converter = time.gmtime
    formatter = logging.Formatter("[%(levelname)-8s] [%(asctime)s] %(message)s", datefmt="%Y/%m/%d %H:%M:%S")

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", dest="config_file")
    parser.add_option("--no-save", action="store_true", default=False, dest="no_save")
    options, args = parser.parse_args()

    setup_logging()

    if options.config_file:
        config = toml.load(options.config_file)
    else:
        logging.error("No configuration file found")
        sys.exit(1)

    if "memcached" not in config:
        logging.error("No memcached configuration found")
        sys.exit(2)

    if "prometheus" not in config:
        logging.error("No prometheus configuration found")
        sys.exit(3)

    memcached_stats = MemcachedStats(config, debug=options.no_save)

    if not options.no_save:
        logging.info("Starting server")
        prometheus_client.start_http_server(config["prometheus"]["port"])

    while True:
        memcached_stats.collect_stats()
        memcached_stats.save_stats(debug=options.no_save)

        if not options.no_save:
            logging.info("Metrics updated")
        time.sleep(config["prometheus"]["sleep"])

if __name__ == "__main__":
    main()
