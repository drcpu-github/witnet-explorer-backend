from blockchain.transactions.data_request import DataRequest


def test_data_request_explorer(
    database,
    witnet_node,
    data_requests,
):
    txn_hashes = (
        (
            "d05b3d4622dd7f5bbb54fb597f944343d6d1367286fcef53e071c48531a2caac",
            101,
            1080,
        ),  # HTTP-GET, pre-wit/2, success
        (
            "3bf3a7d038ecc3ca3da27083919653453ddce9f1e06a8be10fc360893cd87919",
            580,
            2988,
        ),  # HTTP-GET, post-wit/2, success
        (
            "81bd191c77e5ce8a795911846c6adf1e0337ba5828e945c7d38531b511478f41",
            2070,
            6677,
        ),  # RNG, post-wit/2, TooManyWitnesses
    )

    for txn_hash, epoch, weight in txn_hashes:
        data_request = DataRequest(
            database=database,
            witnet_node=witnet_node,
        )
        data_request.set_transaction(txn_hash, epoch, txn_weight=weight)
        txn = data_request.process_transaction("explorer")

        # Need to modify some of the JSON data since bytearray is not a valid JSON type
        input_utxos = data_requests[txn_hash]["processed"]["explorer"]["input_utxos"]
        data_requests[txn_hash]["processed"]["explorer"]["input_utxos"] = [
            (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
        ]
        bodies = data_requests[txn_hash]["processed"]["explorer"]["bodies"]
        data_requests[txn_hash]["processed"]["explorer"]["bodies"] = [
            bytearray(body, "utf-8") for body in bodies
        ]
        scripts = data_requests[txn_hash]["processed"]["explorer"]["scripts"]
        data_requests[txn_hash]["processed"]["explorer"]["scripts"] = [
            bytearray(script) for script in scripts
        ]

        assert txn == data_requests[txn_hash]["processed"]["explorer"]


def test_data_request_api(
    database,
    witnet_node,
    data_requests,
):
    txn_hashes = (
        (
            "d05b3d4622dd7f5bbb54fb597f944343d6d1367286fcef53e071c48531a2caac",
            101,
            1080,
        ),  # HTTP-GET, pre-wit/2, success
        (
            "3bf3a7d038ecc3ca3da27083919653453ddce9f1e06a8be10fc360893cd87919",
            580,
            2988,
        ),  # HTTP-GET, post-wit/2, success
        (
            "81bd191c77e5ce8a795911846c6adf1e0337ba5828e945c7d38531b511478f41",
            2070,
            6677,
        ),  # RNG, post-wit/2, TooManyWitnesses
    )

    for txn_hash, epoch, weight in txn_hashes:
        data_request = DataRequest(
            database=database,
            witnet_node=witnet_node,
        )
        data_request.set_transaction(txn_hash, epoch, txn_weight=weight)
        txn = data_request.process_transaction("api")

        assert txn == data_requests[txn_hash]["processed"]["api"]
