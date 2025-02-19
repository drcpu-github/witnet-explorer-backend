from blockchain.transactions.mint import Mint


def test_mint_processing_explorer(
    database,
    witnet_node,
    mints,
):
    # Before wit/2
    txn_hash = "55b1e998e00e62a71664b52191450788fe0c4a62bab32cc995cf89281845998f"

    mint = Mint(
        database=database,
        witnet_node=witnet_node,
    )
    mint.set_transaction(txn_hash, 4)
    txn = mint.process_transaction(mints[txn_hash]["block_signature"], "explorer")

    assert txn == mints[txn_hash]["processed"]["explorer"]

    # After wit/2
    txn_hash = "ebda819d3e69db289ed43e66c58c8dfd155c3de1ece316f1bccceca47446e556"

    mint = Mint(
        database=database,
        witnet_node=witnet_node,
    )
    mint.set_transaction(txn_hash, 580)
    txn = mint.process_transaction(mints[txn_hash]["block_signature"], "explorer")

    assert txn == mints[txn_hash]["processed"]["explorer"]


def test_mint_processing_api(
    database,
    witnet_node,
    mints,
):
    # Before wit/2
    txn_hash = "55b1e998e00e62a71664b52191450788fe0c4a62bab32cc995cf89281845998f"

    mint = Mint(
        database=database,
        witnet_node=witnet_node,
    )
    mint.set_transaction(txn_hash, 4)
    txn = mint.process_transaction(mints[txn_hash]["block_signature"], "api")

    assert txn == mints[txn_hash]["processed"]["api"]

    # After wit/2
    txn_hash = "ebda819d3e69db289ed43e66c58c8dfd155c3de1ece316f1bccceca47446e556"

    mint = Mint(
        database=database,
        witnet_node=witnet_node,
    )
    mint.set_transaction(txn_hash, 580)
    txn = mint.process_transaction(mints[txn_hash]["block_signature"], "api")

    assert txn == mints[txn_hash]["processed"]["api"]
