from blockchain.transactions.data_request import DataRequest


def test_data_request_headers_explorer(
    consensus_constants,
    database,
    witnet_node,
    data_requests,
):
    txn_hash = "c3fb68882075e755aa62777b5ad5986067ec024fc4007fb75a1d855771119c73"

    # Need to modify some of the JSON data since bytearray is not a valid JSON type
    input_utxos = data_requests[txn_hash]["api"]["explorer"]["input_utxos"]
    data_requests[txn_hash]["api"]["explorer"]["input_utxos"] = [
        (bytearray.fromhex(txn), idx) for txn, idx in input_utxos
    ]
    bodies = data_requests[txn_hash]["api"]["explorer"]["bodies"]
    data_requests[txn_hash]["api"]["explorer"]["bodies"] = [
        bytearray(body, "utf-8") for body in bodies
    ]
    scripts = data_requests[txn_hash]["api"]["explorer"]["scripts"]
    data_requests[txn_hash]["api"]["explorer"]["scripts"] = [
        bytearray.fromhex(script) for script in scripts
    ]

    data_request = DataRequest(
        consensus_constants,
        database=database,
        witnet_node=witnet_node,
    )
    data_request.set_transaction(txn_hash, 1059883)
    txn = data_request.process_transaction("explorer")

    assert txn == data_requests[txn_hash]["api"]["explorer"]


def test_data_request_headers_api(
    consensus_constants,
    database,
    witnet_node,
    data_requests,
):
    txn_hash = "c3fb68882075e755aa62777b5ad5986067ec024fc4007fb75a1d855771119c73"

    data_request = DataRequest(
        consensus_constants,
        database=database,
        witnet_node=witnet_node,
    )
    data_request.set_transaction(txn_hash, 1059883)
    txn = data_request.process_transaction("api")

    assert txn == data_requests[txn_hash]["api"]["api"]
