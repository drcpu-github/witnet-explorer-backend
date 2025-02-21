from blockchain.transactions.commit import Commit


def test_commit_processing_explorer(
    database,
    witnet_node,
    commits,
):
    # Before wit/2
    txn_hash = "1255f0fcb6d8320ce4c7bf65c30e344bef6c1ccc594265260d0169e2841509bf"

    commit = Commit(
        database=database,
        witnet_node=witnet_node,
    )
    commit.set_transaction(txn_hash, 102)
    txn = commit.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = commits[txn_hash]["processed"]["explorer"]["input_utxos"]
    commits[txn_hash]["processed"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]

    assert txn == commits[txn_hash]["processed"]["explorer"]

    # After wit/2
    txn_hash = "a0bc14a003f83eedb5fead4461e4324576dcbc9c66038fd0f8fdbe1a6a1a8cc0"

    commit = Commit(
        database=database,
        witnet_node=witnet_node,
    )
    commit.set_transaction(txn_hash, 581)
    txn = commit.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = commits[txn_hash]["processed"]["explorer"]["input_utxos"]
    commits[txn_hash]["processed"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]

    assert txn == commits[txn_hash]["processed"]["explorer"]


def test_commit_processing_api(
    database,
    witnet_node,
    commits,
):
    # Before wit/2
    txn_hash = "1255f0fcb6d8320ce4c7bf65c30e344bef6c1ccc594265260d0169e2841509bf"

    commit = Commit(
        database=database,
        witnet_node=witnet_node,
    )
    commit.set_transaction(txn_hash, 102)
    txn = commit.process_transaction("api")

    assert txn == commits[txn_hash]["processed"]["api"]

    # After wit/2
    txn_hash = "a0bc14a003f83eedb5fead4461e4324576dcbc9c66038fd0f8fdbe1a6a1a8cc0"

    commit = Commit(
        database=database,
        witnet_node=witnet_node,
    )
    commit.set_transaction(txn_hash, 581)
    txn = commit.process_transaction("api")

    assert txn == commits[txn_hash]["processed"]["api"]
