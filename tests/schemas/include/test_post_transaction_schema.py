import pytest
from marshmallow import ValidationError

from schemas.include.post_transaction_schema import PostTransaction, PostValueTransfer


@pytest.fixture
def value_transfer():
    return {
        "body": {
            "inputs": [
                {
                    "output_pointer": "8254b181ef8738376eaa1ee16d7bd2346cd6e98cb2e527f80bf433cd1066475a:0"
                }
            ],
            "outputs": [
                {
                    "pkh": "wit1gjnecg8demjagg6jdg65hmgf5xa9g32zsk65j9",
                    "time_lock": 0,
                    "value": 12194899013917,
                }
            ],
        },
        "signatures": [
            {
                "public_key": {
                    "bytes": "924d7f74c59371aa2bbe4d2105abad942410bd3f8465318b9befa85eaa36bc2e",
                    "compressed": 2,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "304402203b8c9129fc788094ab46100089b97c6e8528d767e34530eab25d2e1ee92b8cf202203f7e5c5bbcd5f2efac71958eb1a0906a3bf74144289f631a9b14c4b54ed5a55f"
                    }
                },
            }
        ],
    }


def test_value_transfer_1_success(value_transfer):
    PostValueTransfer().load(value_transfer)


def test_value_transfer_2_success():
    data = {
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
    transformed = PostValueTransfer().load(data)
    signatures = transformed["signatures"]
    # fmt: off
    assert signatures[0]["public_key"]["bytes"] == [
        87, 24, 158, 207, 246, 73, 102, 20, 66, 80, 179, 10, 20, 72, 46, 172, 14, 121, 79, 136,
        143, 235, 174, 113, 235, 133, 155, 1, 94, 161, 226, 127,
    ]
    assert signatures[1]["signature"]["Secp256k1"]["der"] == [
        48, 69, 2, 33, 0, 181, 125, 51, 125, 88, 166, 166, 239, 154, 85, 47, 44, 220, 6, 220,
        174, 11, 199, 69, 86, 196, 55, 84, 16, 165, 197, 130, 44, 139, 4, 236, 74, 2, 32, 16,
        225, 84, 70, 8, 82, 34, 36, 111, 103, 174, 1, 70, 221, 183, 8, 154, 154, 148, 172, 81,
        87, 72, 246, 2, 18, 77, 157, 221, 191, 100, 211,
    ]
    # fmt: on


def test_value_transfer_failure_not_enough_signatures():
    data = {
        "body": {
            "inputs": [
                {
                    "output_pointer": "862cadf2caa1f4126da66e0250e73284fd5126c574e18ede0725de9ccaba24e5:0"
                },
                {
                    "output_pointer": "862cadf2caa1f4126da66e0250e73284fd5126c574e18ede0725de9ccaba24e5:1"
                },
            ],
            "outputs": [
                {
                    "pkh": "wit1zl7ty0lwr7atp5fu34azkgewhtfx2fl4wv69cw",
                    "time_lock": 0,
                    "value": 999999999,
                }
            ],
        },
        "signatures": [
            {
                "public_key": {
                    "bytes": "ae64651655ac5d9f8650760ca2631efca31626745ca4133ac21f55221e3d3069",
                    "compressed": 3,
                },
                "signature": {
                    "Secp256k1": {
                        "der": "3044022072b465d197382905780fb41e570dfb6ca0932985197d223c16cec27f21f4f10c022032a35c4685cd32be7f5eb47cafc30b34b2d1fae8b4f1bd869cd0fcceab4dbefa"
                    }
                },
            }
        ],
    }
    with pytest.raises(ValidationError) as err_info:
        PostValueTransfer().load(data)
    assert (
        err_info.value.messages["_schema"][0]
        == "Amount of signatures (1) != amount of inputs (2)."
    )


def test_transaction_success():
    data = {
        "ValueTransfer": {
            "body": {
                "inputs": [
                    {
                        "output_pointer": "8254b181ef8738376eaa1ee16d7bd2346cd6e98cb2e527f80bf433cd1066475a:0"
                    }
                ],
                "outputs": [
                    {
                        "pkh": "wit1gjnecg8demjagg6jdg65hmgf5xa9g32zsk65j9",
                        "time_lock": 0,
                        "value": 12194899013917,
                    }
                ],
            },
            "signatures": [
                {
                    "public_key": {
                        "bytes": "924d7f74c59371aa2bbe4d2105abad942410bd3f8465318b9befa85eaa36bc2e",
                        "compressed": 2,
                    },
                    "signature": {
                        "Secp256k1": {
                            "der": "304402203b8c9129fc788094ab46100089b97c6e8528d767e34530eab25d2e1ee92b8cf202203f7e5c5bbcd5f2efac71958eb1a0906a3bf74144289f631a9b14c4b54ed5a55f"
                        }
                    },
                }
            ],
        }
    }
    PostTransaction().load(data)


def test_transaction_failure_multiple_transactions():
    data = {
        "ValueTransfer": {
            "body": {
                "inputs": [],
                "outputs": [],
            },
            "signatures": [],
        },
        "Stake": {},
    }
    with pytest.raises(ValidationError) as err_info:
        PostTransaction().load(data)
    assert (
        err_info.value.messages["_schema"][0]
        == "Transaction class requires exactly one of ValueTransfer, Stake or Unstake."
    )
