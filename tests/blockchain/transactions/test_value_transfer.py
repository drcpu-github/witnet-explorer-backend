from blockchain.transactions.value_transfer import ValueTransfer


def test_value_transfer_processing_explorer(
    database,
    witnet_node,
    value_transfers,
):
    # Change
    txn_hash = "de12b689901bb1a9ddd5e3034d6d20146f8ffdf2176438b7513b64775923853c"

    value_transfer = ValueTransfer(
        database=database,
        witnet_node=witnet_node,
    )
    value_transfer.set_transaction(txn_hash, 190, txn_weight=1252)
    txn = value_transfer.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = value_transfers[txn_hash]["processed"]["explorer"]["input_utxos"]
    value_transfers[txn_hash]["processed"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]

    assert txn == value_transfers[txn_hash]["processed"]["explorer"]

    # No change
    txn_hash = "e22cad472149be98e113cfdd6b8da1a91195452ffcdf279d0e2b7fa581e8880d"

    value_transfer = ValueTransfer(
        database=database,
        witnet_node=witnet_node,
    )
    value_transfer.set_transaction(txn_hash, 34055, txn_weight=626)
    txn = value_transfer.process_transaction("explorer")

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = value_transfers[txn_hash]["processed"]["explorer"]["input_utxos"]
    value_transfers[txn_hash]["processed"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]

    assert txn == value_transfers[txn_hash]["processed"]["explorer"]


def test_value_transfer_processing_api(
    database,
    witnet_node,
    value_transfers,
):
    # Change
    txn_hash = "de12b689901bb1a9ddd5e3034d6d20146f8ffdf2176438b7513b64775923853c"

    value_transfer = ValueTransfer(
        database=database,
        witnet_node=witnet_node,
    )
    value_transfer.set_transaction(txn_hash, 190, txn_weight=1252)
    txn = value_transfer.process_transaction("api")

    assert txn == value_transfers[txn_hash]["processed"]["api"]

    # No change
    txn_hash = "e22cad472149be98e113cfdd6b8da1a91195452ffcdf279d0e2b7fa581e8880d"

    value_transfer = ValueTransfer(
        database=database,
        witnet_node=witnet_node,
    )
    value_transfer.set_transaction(txn_hash, 34055, txn_weight=626)
    txn = value_transfer.process_transaction("api")

    assert txn == value_transfers[txn_hash]["processed"]["api"]
