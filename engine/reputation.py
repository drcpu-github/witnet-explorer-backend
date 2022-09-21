import optparse
import time
import toml

from collections import Counter

from util.database_manager import DatabaseManager

from engine.trs import TRS

from util.logger import configure_logger

def get_last_epoch_processed(db_mngr):
    sql = """
        SELECT
            MAX(epoch)
        FROM reputation
    """
    last_epoch = db_mngr.sql_return_one(sql)[0]
    if last_epoch:
        return last_epoch
    else:
        return 0

def get_last_confirmed_epoch(db_mngr):
    sql = """
        SELECT
            MAX(epoch)
        FROM blocks
        WHERE
            confirmed=true
    """
    last_confirmed_epoch = db_mngr.sql_return_one(sql)[0]
    if last_confirmed_epoch:
        return last_confirmed_epoch
    else:
        return 0

def get_unique_addresses(db_mngr, start_epoch, stop_epoch):
    sql = """
        SELECT
            tally_txns.output_addresses,
            tally_txns.error_addresses,
            tally_txns.liar_addresses
        FROM
            tally_txns
        WHERE
            tally_txns.epoch BETWEEN %s AND %s
        ORDER BY
            epoch ASC
    """ % (start_epoch, stop_epoch)
    tallies = db_mngr.sql_return_all(sql)

    addresses = set()
    if tallies:
        for tally in tallies:
            output_addresses, error_addresses, liar_addresses = tally
            for address in output_addresses:
                addresses.add(address)
            for address in error_addresses:
                addresses.add(address)
            for address in liar_addresses:
                addresses.add(address)
    return addresses

def get_solved_data_requests(db_mngr, start_epoch, stop_epoch):
    sql = """
        SELECT
            data_request_txns.txn_hash,
            data_request_txns.input_addresses,
            reveal_txns.txn_address,
            tally_txns.output_addresses,
            tally_txns.error_addresses,
            tally_txns.liar_addresses,
            tally_txns.epoch
        FROM data_request_txns
        LEFT JOIN
            reveal_txns
        ON
            data_request_txns.txn_hash=reveal_txns.data_request_txn_hash
        LEFT JOIN
            tally_txns
        ON
            data_request_txns.txn_hash=tally_txns.data_request_txn_hash
        WHERE
            tally_txns.epoch BETWEEN %s AND %s
        ORDER BY
            epoch ASC
    """ % (max(0, start_epoch - 10), stop_epoch)
    reveals = db_mngr.sql_return_all(sql)

    data_request_reveals = {}
    data_request_tallies = {}
    for reveal in reveals:
        txn_hash, input_addresses, txn_address, output_addresses, error_addresses, liar_addresses, epoch = reveal
        txn_hash = txn_hash.hex()

        if txn_hash not in data_request_reveals:
            data_request_reveals[txn_hash] = set()
        if txn_address != None:
            data_request_reveals[txn_hash].add(txn_address)

        # We subtract 10 epochs from the start epoch to make sure we get all reveals for the tally of a data request
        # We ignore tallies older than the start epoch because we already indexed them in the previous iteration
        if epoch < start_epoch:
            continue

        if txn_hash not in data_request_tallies:
            # Error addresses revealed an error (such as a HTTP timeout)
            error_identities = set(error_addresses)
            # Liar addresses either revealed an non-consensus value or did not reveal anything
            liar_identities = set(liar_addresses)
            # Honest identities revealed an in-consensus value and have an output in the tally output addresses array
            # We have to subtract the identities which revealed an error (they receive their collateral back, but no reward)
            # The output identities can also contain an output to the data requesting identity if there were errors or lies
            # This output pays back the reward for those identities to the data requesting identity and is always the last address
            if len(error_identities) + len(liar_identities) > 0:
                honest_identities = set(output_addresses[:-1]) - set(error_addresses)
            # If the input and output addresses are the same, the data request failed (e.g., due to InsufficientCommits)
            # We can simply ignore this tally transaction and don't need to simulate a TRS update for it
            elif set(output_addresses) == set(input_addresses):
                continue
            # The base case is to subtract the identities which revealed an error (they receive their collateral back, but no reward)
            else:
                honest_identities = set(output_addresses) - set(error_addresses)

            data_request_tallies[txn_hash] = [epoch, honest_identities, error_identities, liar_identities]

    reputation_identities = {}
    for data_request_txn_hash, identities in data_request_tallies.items():
        epoch, honest_identities, error_identities, liar_identities = identities

        if epoch not in reputation_identities:
            reputation_identities[epoch] = []
        reputation_identities[epoch].append([data_request_reveals[data_request_txn_hash], honest_identities, error_identities, liar_identities])

    return [[epoch, data_requests] for epoch, data_requests in reputation_identities.items()]

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file", help="Specify a configuration file")
    parser.add_option("--total-epochs", type="int", dest="total_epochs")
    parser.add_option("--print-trs", action="store_true", default=False, dest="print_trs")
    parser.add_option("--print-statistics", action="store_true", default=False, dest="print_statistics")
    parser.add_option("--load-trs", action="store_true", default=False, dest="load_trs")
    parser.add_option("--persist-trs", action="store_true", default=False, dest="persist_trs")
    options, args = parser.parse_args()

    start_script = time.perf_counter()

    config = toml.load(options.config_file)

    # Create logger
    logger = configure_logger("reputation", config["engine"]["log_file"], config["engine"]["level_file"])

    # Create database manager
    db_mngr = DatabaseManager(config["database"], logger=logger)

    # Get epochs to fetch
    database_epoch = get_last_epoch_processed(db_mngr) + 1

    if options.total_epochs:
        total_epochs = options.total_epochs
    else:
        total_epochs = get_last_confirmed_epoch(db_mngr)

    # Create TRS
    trs = TRS(config["engine"]["json_file"], options.load_trs, db_mngr=db_mngr, logger=logger)

    # We always take the TRS epoch as start epoch because otherwise it's impossible to guarantee it's being built correctly
    start_epoch = trs.epoch + 1
    if abs(database_epoch - start_epoch) > 10:
        logger.warning(f"TRS loaded from JSON file was persisted at epoch {start_epoch}, last database update was at epoch {database_epoch}")

    # To speed up building the TRS, we first fetch all unique addresses that participated in tallies and insert those
    logger.info(f"Collecting unique addresses for epoch {start_epoch} to {total_epochs}")

    for i in range(start_epoch, total_epochs, config["engine"]["fetch_epochs"]):
        fetch_from_epoch = i
        if i + config["engine"]["fetch_epochs"] < total_epochs:
            fetch_to_epoch = i + config["engine"]["fetch_epochs"] - 1
        else:
            fetch_to_epoch = total_epochs
        addresses = get_unique_addresses(db_mngr, fetch_from_epoch, fetch_to_epoch)

        address_ids = trs.get_addresses_to_ids()

        addresses_to_insert = []
        for address in addresses:
            if address not in address_ids:
                addresses_to_insert.append([address])

        if len(addresses_to_insert) > 0:
            trs.insert_addresses(addresses_to_insert)

        logger.info(f"Inserted {len(addresses_to_insert)} new adddresses for epochs {fetch_from_epoch} to {fetch_to_epoch}")

    # Update TRS address ids once more
    trs.get_addresses_to_ids()

    logger.info(f"Collecting data requests for epoch {start_epoch} to {total_epochs} to build TRS")

    previous_epoch, epoch = start_epoch, start_epoch
    for i in range(start_epoch, total_epochs, config["engine"]["fetch_epochs"]):
        # Fetch data requests
        fetch_from_epoch = i
        if i + config["engine"]["fetch_epochs"] < total_epochs:
            fetch_to_epoch = i + config["engine"]["fetch_epochs"] - 1
        else:
            fetch_to_epoch = total_epochs

        start = time.perf_counter()
        solved_data_requests = get_solved_data_requests(db_mngr, fetch_from_epoch, fetch_to_epoch)
        logger.info(f"Fetched data requests for epoch {fetch_from_epoch} to {fetch_to_epoch} in {time.perf_counter() - start:.2f}s")

        start = time.perf_counter()

        for data in solved_data_requests:
            epoch, data_requests = data

            # Create Counters for all identities
            revealing_identities, honest_identities, error_identities, liar_identities = Counter(), Counter(), Counter(), Counter()
            for data_request in data_requests:
                revealing_identities.update(data_request[0])
                honest_identities.update(data_request[1])
                error_identities.update(data_request[2])
                liar_identities.update(data_request[3])

            # Update TRS when necessary
            if sum(revealing_identities.values()) + sum(honest_identities.values()) + sum(liar_identities.values()) > 0:
                trs.update(epoch, revealing_identities, honest_identities, error_identities, liar_identities)

            previous_epoch = epoch

        logger.info(f"Processed data requests for epoch {fetch_from_epoch} to {fetch_to_epoch} in {time.perf_counter() - start:.2f}s")

    # Attempt one last expiry of reputation if necessary
    if epoch < total_epochs:
        trs.expire_reputation_in_next_epoch()
    # Remove all identities with zero reputation
    trs.clean()

    # Print TRS at exit if requested via the command line
    if options.print_trs:
        trs.print_trs()

    # Print statistics at exit if requested via the command line
    if options.print_statistics:
        trs.print_statistics()

    if options.persist_trs:
        trs.persist_trs()

    logger.info(f"Processed all data requests between epochs {start_epoch} and {total_epochs} in {time.perf_counter() - start_script:.2f}s")

if __name__ == "__main__":
    main()
