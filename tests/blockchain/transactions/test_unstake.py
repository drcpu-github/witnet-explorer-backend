from blockchain.transactions.unstake import Unstake


def test_unstake_processing_explorer(
    database,
    witnet_node,
    unstakes,
):
    txn_hash = "c66e40e48763678b4ada2442e59b5a04b69a620a2bf72799abf0cd652a5a68e7"

    unstake = Unstake(
        database=database,
        witnet_node=witnet_node,
    )
    unstake.set_transaction(txn_hash, 2478, txn_weight=153)
    txn = unstake.process_transaction("explorer")

    assert txn == unstakes[txn_hash]["processed"]["explorer"]


def test_unstake_processing_api(
    database,
    witnet_node,
    unstakes,
):
    txn_hash = "c66e40e48763678b4ada2442e59b5a04b69a620a2bf72799abf0cd652a5a68e7"

    unstake = Unstake(
        database=database,
        witnet_node=witnet_node,
    )
    unstake.set_transaction(txn_hash, 2478, txn_weight=153)
    txn = unstake.process_transaction("api")

    assert txn == unstakes[txn_hash]["processed"]["api"]
