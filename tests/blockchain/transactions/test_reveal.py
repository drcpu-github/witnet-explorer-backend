from blockchain.transactions.reveal import Reveal


def test_reveal_processing_explorer(
    database,
    witnet_node,
    reveals,
):
    txn_hash = "6f25aab90ce784f61c2093fd98b7939036b87f056af2702fcf82b1cf09d30bae"

    reveal = Reveal(
        database=database,
        witnet_node=witnet_node,
    )
    reveal.set_transaction(txn_hash, 103)
    txn = reveal.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    reveals[txn_hash]["processed"]["explorer"]["reveal"] = bytearray(
        reveals[txn_hash]["processed"]["explorer"]["reveal"]
    )

    assert txn == reveals[txn_hash]["processed"]["explorer"]


def test_reveal_processing_api(
    database,
    witnet_node,
    reveals,
):
    txn_hash = "6f25aab90ce784f61c2093fd98b7939036b87f056af2702fcf82b1cf09d30bae"

    reveal = Reveal(
        database=database,
        witnet_node=witnet_node,
    )
    reveal.set_transaction(txn_hash, 103)
    txn = reveal.process_transaction("api")

    assert txn == reveals[txn_hash]["processed"]["api"]
