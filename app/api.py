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

from app.node_manager import NodeManager

from .gunicorn_config import TOML_CONFIG

from util.logger import select_logging_level

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
config = toml.load(TOML_CONFIG)
logging_queue = Queue()
listener = Process(target=logging_listener, args=(config, logging_queue))
listener.daemon = True
listener.start()

node = NodeManager(config, logging_queue)

@api.route("/hash")
def hash():
    # hash value
    value = request.args.get("value", default="", type=str)
    # do not build a full data request report when fetching a data request / commit / reveal / tally transaction
    simple = request.args.get("simple", default=False, type=bool)
    # control fetching RAD or data request history
    start = request.args.get("start", default=0, type=int)
    stop = request.args.get("stop", default=0, type=int)
    amount = request.args.get("amount", default=100, type=int)
    if value == "":
        return {}
    return node.get_hash(value, simple, start, stop, amount)

@api.route("/address")
def address():
    value = request.args.get("value", default="", type=str)
    tab = request.args.get("tab", default="transactions", type=str)
    limit = request.args.get("limit", default=1000, type=int)
    epoch = request.args.get("epoch", default=0, type=int)
    if value == "":
        return {}
    return node.get_address(value, tab, limit, epoch)

@api.route("/epoch")
def epoch():
    epoch = request.args.get("value", default=1, type=int)
    return node.get_epoch(epoch)

@api.route("/home")
def home():
    return node.get_home("full")

@api.route("/supply_info")
def supply_info():
    key = request.args.get("key", default="", type=str)
    supply_info = node.get_home(key)
    if key in ("blocks_minted", "blocks_missing", "current_time", "epoch", "in_flight_requests"):
        return str(supply_info)
    else:
        return str(int(supply_info / 1E9))

@api.route("/reputation")
def reputation():
    epoch = request.args.get("epoch", default="", type=str)
    return node.get_reputation_list(epoch)

@api.route("/balances")
def balance_list():
    start = request.args.get("start", default=0, type=int)
    stop = request.args.get("stop", default=1000, type=int)
    return node.get_balance_list(start, stop)

@api.route("/network")
def network():
    return node.get_network()

@api.route("/pending")
def get_transaction_pool():
    return node.get_mempool_transactions()

@api.route('/blockchain')
def blockchain():
    action = request.args.get("action", default="init", type=str)
    block = request.args.get("block", default=-100, type=int)
    return node.get_blockchain(action, block)

@api.route("/utxos")
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
def get_tapi():
    action = request.args.get("action", default="init", type=str)
    if action == "init":
        return node.init_tapi()
    elif action == "update":
        return node.update_tapi()
    else:
        return {"error": "invalid TAPI action"}

@api.route("/status")
def status():
    return node.get_status()

@api.route("/send", methods=["POST"])
def send():
    test = request.args.get("test", default=False, type=bool)
    return node.send_vtt(request.data.decode("utf-8"), test)
