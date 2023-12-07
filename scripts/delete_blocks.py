import optparse
import sys

import toml

from util.database_manager import DatabaseManager


def delete_block(db_mngr, epoch):
    sql_statement = "DELETE FROM hashes WHERE hashes.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} hash(es) for epoch {epoch}")

    sql_statement = "DELETE FROM blocks WHERE blocks.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} block(s) for epoch {epoch}")

    sql_statement = "DELETE FROM mint_txns WHERE mint_txns.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} mint transaction(s) for epoch {epoch}")

    sql_statement = "DELETE FROM value_transfer_txns WHERE value_transfer_txns.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} value transfer transaction(s) for epoch {epoch}")

    sql_statement = "DELETE FROM data_request_txns WHERE data_request_txns.epoch=%s" % (
        epoch,
    )
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} data request transaction(s) for epoch {epoch}")

    sql_statement = "DELETE FROM commit_txns WHERE commit_txns.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} commit transaction(s) for epoch {epoch}")

    sql_statement = "DELETE FROM reveal_txns WHERE reveal_txns.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} reveal transaction(s) for epoch {epoch}")

    sql_statement = "DELETE FROM tally_txns WHERE tally_txns.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Deleted {result} tally transaction(s) for epoch {epoch}")


def delete_block_range(db_mngr, start_epoch, stop_epoch):
    sql_statement = "DELETE FROM hashes WHERE hashes.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(f"Deleted {result} hashes for epochs {start_epoch} to {stop_epoch}")

    sql_statement = "DELETE FROM blocks WHERE blocks.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(f"Deleted {result} block for epochs {start_epoch} to {stop_epoch}")

    sql_statement = "DELETE FROM mint_txns WHERE mint_txns.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(f"Deleted {result} mint transaction for epochs {start_epoch} to {stop_epoch}")

    sql_statement = "DELETE FROM value_transfer_txns WHERE value_transfer_txns.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(
        f"Deleted {result} value transfer transaction(s) for epochs {start_epoch} to {stop_epoch}"
    )

    sql_statement = (
        "DELETE FROM data_request_txns WHERE data_request_txns.epoch BETWEEN %s AND %s"
    )
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(
        f"Deleted {result} data request transaction(s) for epochs {start_epoch} to {stop_epoch}"
    )

    sql_statement = "DELETE FROM commit_txns WHERE commit_txns.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(
        f"Deleted {result} commit transaction(s) for epochs {start_epoch} to {stop_epoch}"
    )

    sql_statement = "DELETE FROM reveal_txns WHERE reveal_txns.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(
        f"Deleted {result} reveal transaction(s) for epochs {start_epoch} to {stop_epoch}"
    )

    sql_statement = "DELETE FROM tally_txns WHERE tally_txns.epoch BETWEEN %s AND %s"
    result = db_mngr.sql_update_table(
        sql_statement, parameters=[start_epoch, stop_epoch]
    )
    print(
        f"Deleted {result} tally transaction(s) for epochs {start_epoch} to {stop_epoch}"
    )


def get_reverted_blocks(db_mngr):
    sql = "SELECT epoch FROM blocks WHERE confirmed=false AND reverted=true"
    return db_mngr.sql_return_all(sql)


def main():
    parser = optparse.OptionParser()
    parser.add_option("--epochs", type="string", dest="epochs")
    parser.add_option("--start-epoch", type="int", dest="start_epoch")
    parser.add_option("--stop-epoch", type="int", dest="stop_epoch")
    parser.add_option("--reverted", action="store_true", dest="reverted")
    parser.add_option(
        "--config-file",
        type="string",
        default="explorer.toml",
        dest="config_file",
        help="Specify a configuration file",
    )
    options, args = parser.parse_args()

    config = toml.load(options.config_file)
    db_mngr = DatabaseManager(config["database"])

    if options.epochs is not None:
        epochs_to_confirm = [int(epoch) for epoch in options.epochs.split(",")]
        for epoch in epochs_to_confirm:
            delete_block(db_mngr, epoch)
    elif options.start_epoch is not None and options.stop_epoch is not None:
        delete_block_range(db_mngr, options.start_epoch, options.stop_epoch)
    elif options.reverted is not None:
        epochs = get_reverted_blocks(db_mngr)
        if epochs:
            for epoch in epochs:
                delete_block(db_mngr, epoch[0])
    else:
        sys.stderr.write("Usage of this script:\n")
        sys.stderr.write("./delete_blocks --reverted\n")
        sys.stderr.write("./delete_blocks --epochs <comma-separated string>\n")
        sys.stderr.write("./delete_blocks --start-epoch <x> --stop-epoch <y>\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
