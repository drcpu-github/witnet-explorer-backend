import datetime
import logging
import logging.handlers
import os
import re
import shutil
import sys
import time
import toml

from multiprocessing import Process
from multiprocessing import Queue

from flask import Blueprint
from flask import Response
from flask import request

from app.cache import cache

from app.node_manager import NodeManager

def select_logging_level(level):
    if level.lower() == "debug":
        return logging.DEBUG
    elif level.lower() == "info":
        return logging.INFO
    elif level.lower() == "warning":
        return logging.WARNING
    elif level.lower() == "error":
        return logging.ERROR
    elif level.lower() == "critical":
        return logging.CRITICAL

def configure_logging_listener(config):
    root = logging.getLogger()

    logging.Formatter.converter = time.gmtime

    # Add header formatting of the log message
    formatter = logging.Formatter("[%(levelname)-8s] [%(asctime)s] [%(name)-16s] %(message)s", datefmt="%Y/%m/%d %H:%M:%S")

    log_file_name = config["api"]["log"]["log_file"]
    level_file = select_logging_level(config["api"]["log"]["level_file"])
    level_stdout = select_logging_level(config["api"]["log"]["level_stdout"])

    # Get log file parts
    dirname = os.path.dirname(log_file_name)
    basename = os.path.basename(log_file_name)
    filename, extension = os.path.splitext(basename)
    # Move the existing log
    if os.path.exists(log_file_name):
        today = datetime.date.today()
        shutil.move(log_file_name, os.path.join(dirname, f"{filename}.{today.strftime('%Y%m%d')}{extension}"))

    # Add file handler
    file_handler = logging.handlers.TimedRotatingFileHandler(log_file_name, when="D", utc=True)
    # Date suffix should not contain dashes
    file_handler.suffix = "%Y%m%d"
    file_handler.extMatch = re.compile(r"^\d{8}$")
    # Put the date timestamp between the filename and the extension
    file_handler.namer = lambda name: os.path.join(os.path.dirname(name), os.path.basename(name).replace(extension, "") + extension)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(level_file)
    root.addHandler(file_handler)

    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level_stdout)
    root.addHandler(console_handler)

def logging_listener(config, queue):
    configure_logging_listener(config)

    while True:
        try:
            record = queue.get()
            if record == None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except EOFError:
            break
        except KeyboardInterrupt:
            continue
        except ValueError: # ValueError: semaphore or lock released too many times
            continue

api = Blueprint("api", __name__)

# Start logging process
config = toml.load("./config/api.toml")
logging_queue = Queue()
listener = Process(target=logging_listener, args=(config, logging_queue))
listener.daemon = True
listener.start()

node = NodeManager(config, logging_queue)

@cache.cached(timeout=15)
@api.route("/hash")
def hash():
    value = request.args.get("value", default="", type=str)
    simple = request.args.get("simple", default=False, type=bool)
    if value == "":
        return {}
    return node.get_hash(value, simple)

@api.route("/address")
def address():
    value = request.args.get("value", default="", type=str)
    tab = request.args.get("tab", default="transactions", type=str)
    limit = request.args.get("limit", default=1000, type=int)
    epoch = request.args.get("epoch", default=0, type=int)
    if value == "":
        return {}
    return node.get_address(value, tab, limit, epoch)

# Cache the latest data requests for 15 seconds
@api.route("/home")
@cache.cached(timeout=15)
def home():
    return node.get_home()

# Cache the reputation overview for 5 minutes
@api.route("/reputation")
@cache.cached(timeout=300)
def reputation():
    return node.get_reputation_list()

# Cache the richlist for 5 minutes
@api.route("/richlist")
@cache.cached(timeout=300, query_string=True)
def richlist():
    start = request.args.get("start", default=0, type=int)
    stop = request.args.get("stop", default=1000, type=int)
    if stop - start == 1000:
        return node.get_rich_list(start, stop)
    else:
        return {"error": "cannot fetch more than 1000 enties from the richlist"}

# Cache the network stats for 5 minutes
@api.route("/network")
@cache.cached(timeout=300)
def network():
    return node.get_network()

# Cache the transaction pool for 30 seconds
@api.route("/pending")
@cache.cached(timeout=30)
def get_transaction_pool():
    return node.get_pending_transactions()

@api.route('/blockchain')
def blockchain():
    action = request.args.get("action", default="init", type=str)
    block = request.args.get("block", default=-100, type=int)
    return node.get_blockchain(action, block)

# Cache the utxos for 30 seconds
@api.route("/utxos")
@cache.cached(timeout=30, query_string=True)
def utxos():
    address = request.args.get("address", default=None)
    if address == None:
        return {"error": "address argument is required to query utxos"}
    else:
        addresses = list(set(address.split(",")))
        if len(addresses) > 10:
            return {"error": "cannot query utxos for more than 10 unique addresses in one call"}
        utxos = {}
        for address in addresses:
            utxos[address] = node.get_utxos(address)
        return utxos

@api.route("/tapi")
@cache.cached(timeout=180, query_string=True)
def get_tapi():
    action = request.args.get("action", default="init", type=str)
    if action == "init":
        return node.init_tapi()
    elif action == "update":
        return node.update_tapi()
    else:
        return {"error": "invalid TAPI action"}

@api.route("/status")
@cache.cached(timeout=10)
def status():
    return node.get_status()

@api.route("/send", methods=["POST"])
def send():
    test = request.args.get("test", default=False, type=bool)
    return node.send_vtt(request.data.decode("utf-8"), test)
