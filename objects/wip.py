import optparse
import sys
import toml

from node.witnet_node import WitnetNode

from util.database_manager import DatabaseManager

class WIP(object):
    def __init__(self, database_config, node_config):
        self.db_mngr = DatabaseManager(database_config)
        self.fetch_wips()

        self.node_config = node_config

    def fetch_wips(self):
        sql = """
            SELECT
                id,
                title,
                description,
                urls,
                activation_epoch,
                tapi_start_epoch,
                tapi_stop_epoch,
                tapi_bit
            FROM
                wips
            ORDER BY
                id
            ASC
        """
        self.wips = self.db_mngr.sql_return_all(sql)

    def print_wips(self):
        for wip in self.wips:
            wip_id, title, description, urls, activation_epoch, tapi_start_epoch, tapi_stop_epoch, tapi_bit = wip
            print(f"Entry {wip_id}")
            print(f"\tTitle: {title}")
            print(f"\tDescription: {description}")
            for counter, url in enumerate(urls):
                print(f"\tURL of WIP {counter + 1}: {url}")
            print(f"\tActivation epoch: {activation_epoch if activation_epoch else 'not activated'}")
            if tapi_start_epoch:
                print(f"\tStarted at epoch: {tapi_start_epoch}")
            if tapi_stop_epoch:
                print(f"\tStopped at epoch: {tapi_stop_epoch}")
            if tapi_bit:
                print(f"\tUsing signaling bit: {tapi_bit}")

    def add_wip(self):
        # Read the WIP title
        wip_title = input("Specify the title of the WIP? ")

        # Read the WIP description
        wip_description = input("Enter a short description of the WIP? ")

        # Read all URLs of all WIP's for this TAPI
        urls = []
        while True:
            try:
                url = input("Specify the url of (one of) the WIP(s) (or press enter)? ")
                if len(url) == 0:
                    break
                urls.append(url)
            except ValueError:
                continue

        # Read the epoch when this WIP was activated
        while True:
            try:
                activation_epoch = input("Specify the activation epoch for this WIP (or press enter)? ")
                if len(activation_epoch) == 0:
                    activation_epoch = None
                else:
                    activation_epoch = int(activation_epoch)
                break
            except ValueError:
                continue

        # Read the TAPI start epoch
        while True:
            try:
                tapi_start_epoch = input("Set the TAPI start epoch for this WIP (or press enter)? ")
                if len(tapi_start_epoch) == 0:
                    tapi_start_epoch = None
                else:
                    tapi_start_epoch = int(tapi_start_epoch)
                break
            except ValueError:
                continue

        # Read the TAPI stop epoch
        while True:
            try:
                tapi_stop_epoch = input("Specify the TAPI stop epoch for this WIP (or press enter)? ")
                if len(tapi_stop_epoch) == 0:
                    tapi_stop_epoch = None
                else:
                    tapi_stop_epoch = int(tapi_stop_epoch)
                break
            except ValueError:
                continue

        # Read the bit used for the TAPI associated with this WIP
        while True:
            try:
                tapi_bit = input("Specify the TAPI signaling bit used for this WIP (or press enter)? ")
                if len(tapi_bit) == 0:
                    tapi_bit = None
                else:
                    tapi_bit = int(tapi_bit)
                break
            except ValueError:
                continue

        sql = """
            INSERT INTO
                wips (
                    title,
                    description,
                    urls,
                    activation_epoch,
                    tapi_start_epoch,
                    tapi_stop_epoch,
                    tapi_bit
                )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self.db_mngr.sql_insert_one(sql, (wip_title, wip_description, urls, activation_epoch, tapi_start_epoch, tapi_stop_epoch, tapi_bit))

    def process_tapi(self):
        witnet_node = WitnetNode(self.node_config)

        for wip in self.wips:
            wip_id, title, description, urls, activation_epoch, tapi_start_epoch, tapi_stop_epoch, tapi_bit = wip

            if tapi_bit == None:
                continue

            sql = """
                SELECT
                    epoch,
                    block_hash,
                    tapi_accept,
                    confirmed
                FROM
                    blocks
                WHERE
                    epoch BETWEEN %s AND %s
                ORDER BY
                    epoch
                ASC
            """ % (tapi_start_epoch, tapi_stop_epoch)

            result = self.db_mngr.sql_return_all(sql)
            for db_block in result:
                epoch, block_hash, tapi_accept, confirmed = db_block
                if confirmed and tapi_accept == None:
                    print(f"Updating TAPI signal for epoch {epoch}")

                    block = witnet_node.get_block(bytes(block_hash).hex())
                    if type(block) is dict and "error" in block:
                        sys.stderr.write(f"Could not fetch block: {block}\n")
                        continue

                    tapi_signal = block["result"]["block_header"]["signals"]
                    tapi_accept = (tapi_signal & (1 << tapi_bit)) != 0

                    sql = "UPDATE blocks SET tapi_accept=%s WHERE epoch=%s" % (tapi_accept, epoch)
                    self.db_mngr.sql_update_table(sql)

    def is_wip_active(self, epoch, wip_title):
        wip_to_check = None

        # Find TAPI of interest based on its id or title
        for wip in self.wips:
            wip_id, title, description, urls, activation_epoch, tapi_start_epoch, tapi_stop_epoch, tapi_bit = wip
            if wip_title == title:
                wip_to_check = wip
                break

        # Check if it is active based on the activation epoch
        if wip_to_check:
            if activation_epoch and epoch >= activation_epoch:
                return True

        return False

    def is_wip0008_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0008")

    def is_wip0009_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0009-0011-0012")

    def is_wip0011_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0009-0011-0012")

    def is_wip0012_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0009-0011-0012")

    def is_third_hard_fork_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="THIRD_HARD_FORK")

    def is_wip0014_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0014-0016")

    def is_wip0016_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0014-0016")

    def is_wip0017_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0017-0018-0019")

    def is_wip0018_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0017-0018-0019")

    def is_wip0019_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0017-0018-0019")

    def is_wip0020_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0020-0021")

    def is_wip0021_active(self, epoch):
        return self.is_wip_active(epoch, wip_title="WIP0020-0021")

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    config = toml.load(options.config_file)

    wip = WIP(config["database"], config["node-pool"])

    # Run some tests
    assert wip.is_wip0008_active(191999) == False
    assert wip.is_wip0008_active(192000) == True

    assert wip.is_wip0020_active(1059840) == False
    assert wip.is_wip0008_active(1059861) == True

if __name__ == "__main__":
    main()
