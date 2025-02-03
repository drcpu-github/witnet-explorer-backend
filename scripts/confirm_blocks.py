import optparse
import sys

import toml

from util.database_manager import DatabaseManager


def confirm_block(db_mngr, epoch):
    sql_statement = "UPDATE blocks SET confirmed=true WHERE blocks.epoch=%s"
    result = db_mngr.sql_update_table(sql_statement, parameters=[epoch])
    print(f"Confirmed {result} block for epoch {epoch}")


def main():
    parser = optparse.OptionParser()
    parser.add_option("--epochs", type="string", dest="epochs")
    parser.add_option("--start-epoch", type="int", dest="start_epoch")
    parser.add_option("--stop-epoch", type="int", dest="stop_epoch")
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
    elif options.start_epoch is not None and options.stop_epoch is not None:
        epochs_to_confirm = range(options.start_epoch, options.stop_epoch + 1)
    else:
        sys.stderr.write("Usage of this script:\n")
        sys.stderr.write("./confirm_blocks --epochs <comma-separated string>\n")
        sys.stderr.write("./confirm_blocks --start-epoch <x> --stop-epoch <y>\n")
        sys.exit(1)

    for epoch in epochs_to_confirm:
        confirm_block(db_mngr, epoch)


if __name__ == "__main__":
    main()
