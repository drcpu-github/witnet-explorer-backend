import optparse

import toml
from objects.wip import WIP


def main():
    parser = optparse.OptionParser()

    parser.add_option(
        "--print",
        action="store_true",
        dest="print",
        default=False,
        help="Print all WIP entries",
    )

    parser.add_option(
        "--add",
        action="store_true",
        dest="add",
        default=False,
        help="Add a new WIP entry",
    )

    parser.add_option(
        "--process",
        action="store_true",
        dest="process",
        default=False,
        help="Check all TAPI epochs and add signals if needed",
    )

    parser.add_option(
        "--config-file",
        type="string",
        default="explorer.toml",
        dest="config_file",
    )

    options, args = parser.parse_args()

    config = toml.load(options.config_file)

    wip = WIP(config["database"], config["node-pool"])

    if options.print:
        wip.print_wips()
    elif options.add:
        wip.add_wip()
    elif options.process:
        wip.process_tapi()


if __name__ == "__main__":
    main()
