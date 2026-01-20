from blockchain.transactions.tally import Tally


def test_tally_processing_explorer(
    database,
    witnet_node,
    tallies,
):
    txn_hashes = (
        (
            "d3cda3a26568992a9866f9119930855586aefab8493358fdf26b283fe1b6d74e",
            104,
        ),  # Before wit/2
        (
            "e40def582d9c20c6df815d63c7010572e05df1725f6e22a75d755d209d8cd4b0",
            583,
        ),  # After wit/2, success
        (
            "15ef58fcb4b8d0ebc6175e3f671fd04a8ca8819dc90262dc8eb5709e83b95ff0",
            2070,
        ),  # After wit/2, tooManyWitnesses
    )

    for txn_hash, epoch in txn_hashes:
        tally = Tally(
            database=database,
            witnet_node=witnet_node,
        )
        tally.set_transaction(txn_hash, epoch)
        txn = tally.process_transaction("explorer")

        # Need to modify some of the JSON data since bytearray is not a valid JSON type
        tallies[txn_hash]["processed"]["explorer"]["tally"] = bytearray(
            tallies[txn_hash]["processed"]["explorer"]["tally"]
        )

        assert txn == tallies[txn_hash]["processed"]["explorer"]


def test_tally_processing_api(
    database,
    witnet_node,
    tallies,
):
    txn_hashes = (
        (
            "d3cda3a26568992a9866f9119930855586aefab8493358fdf26b283fe1b6d74e",
            104,
        ),  # Before wit/2
        (
            "e40def582d9c20c6df815d63c7010572e05df1725f6e22a75d755d209d8cd4b0",
            583,
        ),  # After wit/2, success
        (
            "15ef58fcb4b8d0ebc6175e3f671fd04a8ca8819dc90262dc8eb5709e83b95ff0",
            2070,
        ),  # After wit/2, tooManyWitnesses
    )

    for txn_hash, epoch in txn_hashes:
        tally = Tally(
            database=database,
            witnet_node=witnet_node,
        )
        tally.set_transaction(txn_hash, epoch)
        txn = tally.process_transaction("api")

        assert txn == tallies[txn_hash]["processed"]["api"]
