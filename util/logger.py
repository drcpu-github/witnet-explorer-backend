import datetime
import logging
import os
import re
import shutil
import sys
import time

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

def configure_logger(log_tag, log_filename, log_level):
    logger = logging.getLogger(log_tag)

    # Read filename details
    dirname = os.path.dirname(log_filename)
    filename, extension = os.path.splitext(os.path.basename(log_filename))
    # Add date timestamp in log filename
    today = datetime.date.today()
    log_filename = os.path.join(dirname, f"{filename}.{today.strftime('%Y%m%d')}{extension}")
    # Setup file handler logging
    file_handler = logging.FileHandler(log_filename)

    # Set log level
    log_level = select_logging_level(log_level)
    logger.setLevel(log_level)

    # Add header formatting of the log message
    logging.Formatter.converter = time.gmtime
    formatter = logging.Formatter("[%(levelname)-8s] [%(asctime)s] [%(name)s] %(message)s", datefmt="%Y/%m/%d %H:%M:%S")
    file_handler.setFormatter(formatter)

    # Add file handler
    logger.addHandler(file_handler)

    return logger

def configure_logging_listener(config):
    root = logging.getLogger()

    logging.Formatter.converter = time.gmtime

    # Add header formatting of the log message
    formatter = logging.Formatter("[%(levelname)-8s] [%(asctime)s] [%(name)-8s] %(message)s", datefmt="%Y/%m/%d %H:%M:%S")

    log_file_name = config["address-jit"]["log"]["log_file"]
    level_file = select_logging_level(config["address-jit"]["log"]["level_file"])
    level_stdout = select_logging_level(config["address-jit"]["log"]["level_stdout"])

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

def create_logging_listener(config, queue):
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
