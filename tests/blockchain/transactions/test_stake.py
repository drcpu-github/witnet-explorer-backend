from blockchain.transactions.stake import Stake


def test_stake_processing_explorer(
    database,
    witnet_node,
    stakes,
):
    # With change output
    txn_hash = "7fdbfba5239650557dae61e0ae94bec383dcdc96a2e8b7f77a36b916ad1b0a0b"

    stake = Stake(
        database=database,
        witnet_node=witnet_node,
    )
    stake.set_transaction(txn_hash, 211, txn_weight=673)
    txn = stake.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = stakes[txn_hash]["processed"]["explorer"]["input_utxos"]
    stakes[txn_hash]["processed"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]

    assert txn == stakes[txn_hash]["processed"]["explorer"]

    # No change output
    txn_hash = "0ea59be6f3c154836b26387dcdfbb5372f47a338706b8f108da342b699f0f1b3"

    stake = Stake(
        database=database,
        witnet_node=witnet_node,
    )
    stake.set_transaction(txn_hash, 33985, txn_weight=637)
    txn = stake.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = stakes[txn_hash]["processed"]["explorer"]["input_utxos"]
    stakes[txn_hash]["processed"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]

    assert txn == stakes[txn_hash]["processed"]["explorer"]


def test_stake_processing_api(
    database,
    witnet_node,
    stakes,
):
    # With change output
    txn_hash = "7fdbfba5239650557dae61e0ae94bec383dcdc96a2e8b7f77a36b916ad1b0a0b"

    stake = Stake(
        database=database,
        witnet_node=witnet_node,
    )
    stake.set_transaction(txn_hash, 211, txn_weight=673)
    txn = stake.process_transaction("api")

    assert txn == stakes[txn_hash]["processed"]["api"]

    # No change output
    txn_hash = "0ea59be6f3c154836b26387dcdfbb5372f47a338706b8f108da342b699f0f1b3"

    stake = Stake(
        database=database,
        witnet_node=witnet_node,
    )
    stake.set_transaction(txn_hash, 33985, txn_weight=637)
    txn = stake.process_transaction("api")

    assert txn == stakes[txn_hash]["processed"]["api"]
