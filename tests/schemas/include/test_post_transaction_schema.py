import pytest
from marshmallow import ValidationError

from schemas.include.post_transaction_schema import (
    PostStake,
    PostTransaction,
    PostUnstake,
    PostValueTransfer,
    StakeTransactionBody,
    TransactionOutput,
    UnstakeTransactionBody,
)
from tests.schemas.include.test_address_schema import generic_address_test


@pytest.fixture
def transaction_output():
    return {
        "pkh": "wit1gjnecg8demjagg6jdg65hmgf5xa9g32zsk65j9",
        "time_lock": 0,
        "value": 12194899013917,
    }


def test_transaction_output_success(transaction_output):
    TransactionOutput().load(transaction_output)


def test_transaction_output_failure_address(transaction_output):
    generic_address_test(
        transaction_output,
        ("pkh",),
        TransactionOutput,
    )


def test_transaction_output_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        TransactionOutput().load(data)
    assert len(err_info.value.messages) == 3
    assert err_info.value.messages["pkh"][0] == "Missing data for required field."
    assert err_info.value.messages["time_lock"][0] == "Missing data for required field."
    assert err_info.value.messages["value"][0] == "Missing data for required field."


@pytest.fixture
def value_transfer():
    return {
        "body": {
            "inputs": [
                {
                    "output_pointer": "a3187803402f5519b4c6997401ca8a6fa354b61f1d31a9239d0004e72432e4e7:0"
                },
                {
                    "output_pointer": "600a2abfe0834fa56f4d370d627385312940910dfa1b2644fc2c9feab60c6239:0"
                },
            ],
            "outputs": [
                {
                    "pkh": "wit1n3yprd6lsuh0wrhfry4cdd26kvxrzrfmeprnvh",
                    "time_lock": 0,
                    "value": 392000000000,
                },
                {
                    "pkh": "wit1lnv34qhxkuvj9ech69k0s07z7ghh4p2kyymvae",
                    "time_lock": 0,
                    "value": 108900209999,
                },
            ],
        },
        "signatures": [
            {
                "public_key": {
                    "bytes": "57189ecff64966144250b30a14482eac0e794f888febae71eb859b015ea1e27f",
                    "compressed": 2,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "3045022100b57d337d58a6a6ef9a552f2cdc06dcae0bc74556c4375410a5c5822c8b04ec4a022010e15446085222246f67ae0146ddb7089a9a94ac515748f602124d9dddbf64d3"
                    }
                },
            },
            {
                "public_key": {
                    "bytes": "57189ecff64966144250b30a14482eac0e794f888febae71eb859b015ea1e27f",
                    "compressed": 2,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "3045022100b57d337d58a6a6ef9a552f2cdc06dcae0bc74556c4375410a5c5822c8b04ec4a022010e15446085222246f67ae0146ddb7089a9a94ac515748f602124d9dddbf64d3"
                    }
                },
            },
        ],
    }


def test_value_transfer_success(value_transfer):
    transformed = PostValueTransfer().load(value_transfer)
    signatures = transformed["signatures"]
    assert signatures[0]["public_key"]["bytes"] == [
        87,
        24,
        158,
        207,
        246,
        73,
        102,
        20,
        66,
        80,
        179,
        10,
        20,
        72,
        46,
        172,
        14,
        121,
        79,
        136,
        143,
        235,
        174,
        113,
        235,
        133,
        155,
        1,
        94,
        161,
        226,
        127,
    ]
    assert signatures[1]["signature"]["Secp256k1"]["der"] == [
        48,
        69,
        2,
        33,
        0,
        181,
        125,
        51,
        125,
        88,
        166,
        166,
        239,
        154,
        85,
        47,
        44,
        220,
        6,
        220,
        174,
        11,
        199,
        69,
        86,
        196,
        55,
        84,
        16,
        165,
        197,
        130,
        44,
        139,
        4,
        236,
        74,
        2,
        32,
        16,
        225,
        84,
        70,
        8,
        82,
        34,
        36,
        111,
        103,
        174,
        1,
        70,
        221,
        183,
        8,
        154,
        154,
        148,
        172,
        81,
        87,
        72,
        246,
        2,
        18,
        77,
        157,
        221,
        191,
        100,
        211,
    ]


def test_value_transfer_failure_not_enough_signatures(value_transfer):
    value_transfer["body"]["inputs"].append(
        {
            "output_pointer": "862cadf2caa1f4126da66e0250e73284fd5126c574e18ede0725de9ccaba24e5:1"
        }
    )
    with pytest.raises(ValidationError) as err_info:
        PostValueTransfer().load(value_transfer)
    assert (
        err_info.value.messages["_schema"][0]
        == "Unexpected amount of signatures: 2 signatures, 3 inputs."
    )


def test_value_transfer_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        PostValueTransfer().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["body"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["signatures"][0] == "Missing data for required field."
    )


@pytest.fixture
def stake_body():
    return {
        "inputs": [
            {
                "output_pointer": "a6fe48d4d00c8843f9fc18e616ece58e81ddf8d61963debb70e8dd1b64edfdb5:9"
            },
            {
                "output_pointer": "a6fe48d4d00c8843f9fc18e616ece58e81ddf8d61963debb70e8dd1b64edfdb5:10"
            },
        ],
        "output": {
            "authorization": {
                "public_key": {
                    "bytes": "9f91aa5fde151aeaaafa6622828b255ba26a345b3b50f0df6749e31bc3a67067",
                    "compressed": 3,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "304502210088887aa088fba909611df500d040fcdae704780719b4b3963d9f980eb2cb145c02204bd412f643c63e6431f899b5c282778238a08f5aef90d34b25bb47f4efd426c9"
                    }
                },
            },
            "key": {
                "validator": "twit1w5n5v0mu8erpdf9uj32ekke07kgggpxcvg8v5r",
                "withdrawer": "twit1w5n5v0mu8erpdf9uj32ekke07kgggpxcvg8v5r",
            },
            "value": 10_000_000_000_000,
        },
        "change": {
            "pkh": "twit1jw07frmeq7m48zmr7yfsq6dkmc939fhnq34gtv",
            "time_lock": 0,
            "value": 10_000_000_000_000,
        },
    }


def test_stake_body_success(stake_body):
    StakeTransactionBody().load(stake_body)


def test_stake_body_no_change_success(stake_body):
    del stake_body["change"]
    StakeTransactionBody().load(stake_body)


def test_stake_body_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        StakeTransactionBody().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["inputs"][0] == "Missing data for required field."
    assert err_info.value.messages["output"][0] == "Missing data for required field."


@pytest.fixture
def stake(stake_body):
    return {
        "body": stake_body,
        "signatures": [
            {
                "public_key": {
                    "bytes": "9f91aa5fde151aeaaafa6622828b255ba26a345b3b50f0df6749e31bc3a67067",
                    "compressed": 3,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "3044022031c600672ee8636f019bac03bccab541a3f499c476ccee2035b22b1f8c6a35bd02203b85a0d7191f5ab3555f5945c72dec963ae817b70d0b8b932f79708cbb5b4040"
                    }
                },
            },
            {
                "public_key": {
                    "bytes": "9f91aa5fde151aeaaafa6622828b255ba26a345b3b50f0df6749e31bc3a67067",
                    "compressed": 3,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "3044022031c600672ee8636f019bac03bccab541a3f499c476ccee2035b22b1f8c6a35bd02203b85a0d7191f5ab3555f5945c72dec963ae817b70d0b8b932f79708cbb5b4040"
                    }
                },
            },
        ],
    }


def test_stake_success(stake):
    PostStake().load(stake)


def test_stake_failure_not_enough_signatures(stake):
    stake["body"]["inputs"].append(
        {
            "output_pointer": "862cadf2caa1f4126da66e0250e73284fd5126c574e18ede0725de9ccaba24e5:1"
        }
    )
    with pytest.raises(ValidationError) as err_info:
        PostStake().load(stake)
    assert (
        err_info.value.messages["_schema"][0]
        == "Unexpected amount of signatures: 2 signatures, 3 inputs."
    )


def test_stake_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        PostStake().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["body"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["signatures"][0] == "Missing data for required field."
    )


@pytest.fixture
def unstake_body():
    return {
        "fee": 1,
        "nonce": 2258,
        "operator": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
        "withdrawal": {
            "pkh": "twit1mseplfttj5vvm8r7d5pn5je9dd02el4hw4w4cp",
            "time_lock": 3600,
            "value": 1000000000000,
        },
    }


def test_unstake_body_success(unstake_body):
    UnstakeTransactionBody().load(unstake_body)


def test_unstake_body_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        UnstakeTransactionBody().load(data)
    assert len(err_info.value.messages) == 4
    assert err_info.value.messages["fee"][0] == "Missing data for required field."
    assert err_info.value.messages["nonce"][0] == "Missing data for required field."
    assert err_info.value.messages["operator"][0] == "Missing data for required field."
    assert (
        err_info.value.messages["withdrawal"][0] == "Missing data for required field."
    )


@pytest.fixture
def unstake(unstake_body):
    return {
        "body": unstake_body,
        "signature": {
            "public_key": {
                "bytes": "97dba59b36593a662de82f6a845f43816d9786a23743c68bfab980ccf3f13870",
                "compressed": 2,
            },
            "signature": {
                "Secp256k1": {
                    "der": "304402206e0024ac8d59558f651ef36705a66f3e78518717a2299aea9f2dc8bf7d773a7f02205591ac9de06eea8cd47dd2abb6fea7990e2e458557f73a2113082f3e57b00aa4"
                }
            },
        },
    }


def test_unstake_success(unstake):
    PostUnstake().load(unstake)


def test_unstake_failure_missing():
    data = {}
    with pytest.raises(ValidationError) as err_info:
        PostUnstake().load(data)
    assert len(err_info.value.messages) == 2
    assert err_info.value.messages["body"][0] == "Missing data for required field."
    assert err_info.value.messages["signature"][0] == "Missing data for required field."


def test_transaction_success(value_transfer, stake, unstake):
    data = {"ValueTransfer": value_transfer}
    PostTransaction().load(data)

    data = {"Stake": stake}
    PostTransaction().load(data)

    data = {"Unstake": unstake}
    PostTransaction().load(data)


def test_transaction_failure_multiple_transactions(value_transfer, stake):
    data = {
        "ValueTransfer": value_transfer,
        "Stake": stake,
    }
    with pytest.raises(ValidationError) as err_info:
        PostTransaction().load(data)
    assert (
        err_info.value.messages["_schema"][0]
        == "Transaction class requires exactly one of ValueTransfer, Stake or Unstake."
    )
