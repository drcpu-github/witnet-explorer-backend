import hashlib
from dataclasses import dataclass
from enum import Enum

from typing import Union, List
from blockchain.objects.wip import WIP

sha256 = lambda x: hashlib.sha256(x).digest()
str_or_none = Union[str, None]

TAG_TYPE_BITS = 3
TAG_TYPE_MASK = (1 << TAG_TYPE_BITS) - 1
VARINT = 0
FIXED64 = 1
LENGTH_DELIMITED = 2
START_GROUP = 3
END_GROUP = 4
FIXED32 = 5

def str_to_bytes(s: str) -> bytes:
    return str.encode(s, 'utf-8')

def bytes_to_hex(b: bytes):
    return b.hex()

def concat(values: Union[List[str], List[bytes]]) -> Union[str, bytes]:
    if isinstance(values[0], str):
        return concat_string(values)
    elif isinstance(values[0], bytes):
        return concat_bytes(values)

def concat_string(values: List[str]) -> str:
    return ''.join(values)

def concat_bytes(values: List[bytes]) -> bytes:
    return b''.join(values)

def var_int(value: int):
    """
    Write unsigned `VarInt` to a file-like object.
    """
    if isinstance(value, str):
        value = int(value)
    tmp = []
    while value > 0x7F:
        tmp.append(bytes((value & 0x7F | 0x80,)))
        value >>= 7
    tmp.append(bytes((value,)))
    return concat(tmp)

def var_int_serializer(value: int):
    return var_int(value)

def bytes_serializer(value: bytes):
    if value:
        return concat([var_int(len(value)), value])
    return b''

def get_tag_field_number(tag: int):
    return tag >> TAG_TYPE_BITS

def get_tag_wire_type(tag: int):
    return tag & TAG_TYPE_MASK

def make_tag(field_number: int, tag: int):
    return (field_number << TAG_TYPE_BITS) | tag

def make_tag_bytes(field_number: int, tag: int):
    return var_int_serializer(make_tag(field_number, tag))

def pb_field(field_number: int, tag: int, value):
    _data = []
    if tag == VARINT:
        _data = concat([var_int_serializer(value=value)])
    elif tag == LENGTH_DELIMITED:
        _data = bytes_serializer(value=value)
    else:
        ...
    return concat([make_tag_bytes(field_number=field_number, tag=tag), _data])
class ProtobufEncoderError(Exception):
    def __init__(self, message, errors):
        super().__init__(message)
        self.message = message
        self.errors = errors

@dataclass
class RADType(Enum):
    Unknown = 0
    HttpGet = 1
    Rng = 2
    HttpPost = 3

    @classmethod
    def from_json(cls, data):
        if data == 'Unknown':
            return RADType.Unknown
        elif data == 'HTTP-GET':
            return RADType.HttpGet
        elif data == 'HTTP-POST':
            return RADType.HttpPost
        elif data == 'RNG':
            return RADType.Rng
        else:
            assert False, "Unknown RADType"

    @classmethod
    def from_value(cls, value: int):
        value_map = {
            0: RADType.Unknown,
            1: RADType.HttpGet,
            2: RADType.Rng,
            3: RADType.HttpPost
        }
        return value_map[value]

@dataclass
class StringPair:
    left: str
    right: str

    @classmethod
    def from_json(cls, data):
        return StringPair(left=data['left'], right=data['right'])

    def to_pb_bytes(self):
        left_bytes = pb_field(field_number=1, tag=LENGTH_DELIMITED, value=str_to_bytes(self.left))
        right_bytes = pb_field(field_number=2, tag=LENGTH_DELIMITED, value=str_to_bytes(self.right))
        return concat([left_bytes, right_bytes])

@dataclass
class RADRetrieve:
    kind: RADType
    url: str
    script: bytes
    body: bytes
    headers: List[StringPair]

    @classmethod
    def from_json(cls, data):
        _url: str = ''
        if 'url' in data:
            _url = data['url']

        _body: bytes = b''
        if 'body' in data:
            _body = bytes(data['body'])

        _headers: list = []
        if 'headers' in data:
            for key, value in data['headers']:
                _headers.append(StringPair(left=key, right=value))

        return RADRetrieve(
            kind=RADType.from_json(data['kind']),
            url=_url,
            script=bytes(data['script']),
            body=_body,
            headers=_headers
        )

    def to_json(self) -> dict:
        return {
            "script": self.script,
            "kind": self.kind,
            "url": self.url if self.url else "",
        }

    def to_pb_bytes(self, epoch: int, wip: WIP) -> bytes:
        kind_bytes: bytes
        url_bytes: bytes
        body_bytes: bytes
        header_bytes: bytes
        value_map: dict
        # Before WIP 0019 activation, the only RADType enum 0 position was valid {0: "HTTP-GET"}
        if not wip.is_wip0019_active(epoch):
            value_map = {
                "HttpGet": 0,
            }
        # Before WIP 0020 activation the only valid RADType enum positions were {0: "HTTP-GET",1: "RNG"}
        elif not wip.is_wip0020_active(epoch):
            value_map = {
                "HttpGet": 0,
                "Rng": 1
            }
        # After WIP 0020 activation the valid RADType enum positions are {0: "Unknown", 1: "HTTP-GET", 2: "RNG", 3: "HTTP-POST" }
        else:
            value_map = {
                "Unknown": 0,
                "HttpGet": 1,
                "Rng": 2,
                "HttpPost": 3,
            }

        try:
            kind = value_map[self.kind.name]
        except:
            raise ProtobufEncoderError(f'Invalid kind: {self.kind.name}, valid types for epoch {epoch} are {value_map.keys()}', {})

        kind_bytes, url_bytes, script_bytes, body_bytes, header_bytes = b'', b'', b'', b'', b''
        if kind > 0:
            kind_bytes = pb_field(field_number=1, tag=VARINT, value=kind)
        if self.url != '':
            url_bytes = pb_field(field_number=2, tag=LENGTH_DELIMITED, value=str_to_bytes(self.url))
        script_bytes = pb_field(field_number=3, tag=LENGTH_DELIMITED, value=self.script)
        if self.body is not None:
            body_bytes = pb_field(field_number=4, tag=LENGTH_DELIMITED, value=self.body)
        if len(self.headers) > 0:
            header_bytes = pb_field(field_number=5, tag=LENGTH_DELIMITED, value=concat([x.to_pb_bytes() for x in self.headers]))

        if self.kind.value == RADType.HttpPost.value:
            return concat([kind_bytes, url_bytes, script_bytes, body_bytes, header_bytes])
        else:
            return concat([kind_bytes, url_bytes, script_bytes])

@dataclass
class RADFilter:
    op: int
    args: bytes

    @classmethod
    def from_json(cls, data):
        return RADFilter(op=data['op'], args=data['args'])

    def to_json(self) -> dict:
        return vars(self)

    def to_pb_bytes(self):
        op_bytes = pb_field(field_number=1, tag=VARINT, value=self.op)
        args_bytes = pb_field(field_number=2, tag=LENGTH_DELIMITED, value=bytes(self.args))
        return concat([op_bytes, args_bytes])

@dataclass
class RADAggregate:
    filters: List[RADFilter]
    reducer: int

    @classmethod
    def from_json(cls, data):
        return RADAggregate(
            filters=[RADFilter.from_json(data=x) for x in data["filters"]],
            reducer=data["reducer"]
        )

    def to_json(self) -> dict:
        return {
            "filters": [
                filters.to_json() for filters in self.filters
            ],
            "reducer": self.reducer,
        }

    def to_pb_bytes(self):
        filter_bytes = b''
        if len(self.filters) > 0:
            filter_bytes = pb_field(field_number=1, tag=LENGTH_DELIMITED, value=concat([x.to_pb_bytes() for x in self.filters]))
        reducer_bytes = pb_field(field_number=2, tag=VARINT, value=self.reducer)
        return concat([filter_bytes, reducer_bytes])

@dataclass
class RADTally:
    filters: List[RADFilter]
    reducer: int

    @classmethod
    def from_json(cls, data):
        return RADTally(
            filters=[RADFilter.from_json(data=x) for x in data["filters"]],
            reducer=data["reducer"]
        )

    def to_json(self) -> dict:
        return {
            "filters": [filters.to_json() for filters in self.filters],
            "reducer": self.reducer,
        }

    def to_pb_bytes(self):
        filter_bytes = b''
        if len(self.filters) > 0:
            filter_bytes = pb_field(field_number=1, tag=LENGTH_DELIMITED, value=concat([x.to_pb_bytes() for x in self.filters]))
        reducer_bytes = pb_field(field_number=2, tag=VARINT, value=self.reducer)
        return concat([filter_bytes, reducer_bytes])

@dataclass
class RADRequest:
    time_lock: int
    retrieve: List[RADRetrieve]
    aggregate: RADAggregate
    tally: RADTally

    @classmethod
    def from_json(cls, data):
        return RADRequest(
            time_lock=data["time_lock"],
            retrieve=[RADRetrieve.from_json(x) for x in data['retrieve']],
            aggregate=RADAggregate.from_json(data=data["aggregate"]),
            tally=RADTally.from_json(data=data["tally"])
        )

    def to_json(self) -> dict:
        return {
            "retrieve": [retrieve.to_json() for retrieve in self.retrieve],
            "aggregate": self.aggregate.to_json(),
            "tally": self.tally.to_json(),
            "time_lock": self.time_lock,
        }

    def to_pb_bytes(self, epoch: int, wip: WIP) -> bytes:
        timelock_bytes = b''
        if self.time_lock > 0:
            timelock_bytes = pb_field(field_number=1, tag=VARINT, value=self.time_lock)
        retrieve_bytes = concat([pb_field(field_number=2, tag=LENGTH_DELIMITED, value=x.to_pb_bytes(epoch, wip)) for x in self.retrieve])
        aggregate_bytes = pb_field(field_number=3, tag=LENGTH_DELIMITED, value=self.aggregate.to_pb_bytes())
        tally_bytes = pb_field(field_number=4, tag=LENGTH_DELIMITED, value=self.tally.to_pb_bytes())
        return concat([timelock_bytes, retrieve_bytes, aggregate_bytes, tally_bytes])

    def hash(self, epoch: int, wip: WIP) -> bytes:
        return sha256(self.to_pb_bytes(epoch, wip))

@dataclass
class DataRequestOutput:
    data_request: RADRequest
    witness_reward: int
    witnesses: int
    commit_and_reveal_fee: int
    min_consensus_percentage: int
    collateral: int

    @classmethod
    def from_json(cls, data):
        output = DataRequestOutput(**data)
        output.data_request = RADRequest.from_json(data=data["data_request"])
        return output

    def to_json(self) -> dict:
        return {
            "data_request": self.data_request.to_json(),
            "witness_reward": self.witness_reward,
            "witnesses": self.witnesses,
            "commit_and_reveal_fee": self.commit_and_reveal_fee,
            "min_consensus_percentage": self.min_consensus_percentage,
            "collateral": self.collateral,
        }

    def to_pb_bytes(self, epoch: int, wip: WIP) -> bytes:
        return concat([
            pb_field(field_number=1, tag=LENGTH_DELIMITED, value=self.data_request.to_pb_bytes(epoch, wip)),
            pb_field(field_number=2, tag=VARINT, value=self.witness_reward),
            pb_field(field_number=3, tag=VARINT, value=self.witnesses),
            pb_field(field_number=4, tag=VARINT, value=self.commit_and_reveal_fee),
            pb_field(field_number=5, tag=VARINT, value=self.min_consensus_percentage),
            pb_field(field_number=6, tag=VARINT, value=self.collateral),
        ])

    def hash(self, epoch, wip) -> bytes:
        return sha256(self.to_pb_bytes(epoch, wip))

class ProtobufEncoder(object):
    def __init__(self, wip: WIP = None):
        self.transaction = None
        self.data_request: Union[DataRequestOutput, None] = None
        self.wip = wip

    def set_transaction(self, transaction):
        if transaction is not None:
            self.transaction = transaction
            if "transaction" in self.transaction:
                self.transaction = transaction["transaction"]
            if "DataRequest" in self.transaction:
                self.dr_output = DataRequestOutput.from_json(self.transaction["DataRequest"]["body"]["dr_output"])
            if "body" in self.transaction and "dr_output" in self.transaction["body"]:
                self.dr_output = DataRequestOutput.from_json(self.transaction["body"]["dr_output"])

    def get_RAD_bytecode(self, epoch: int):
        assert self.dr_output, "Transaction not set"
        return bytes_to_hex(self.dr_output.data_request.hash(epoch, self.wip)), bytes_to_hex(self.dr_output.data_request.to_pb_bytes(epoch, self.wip))

    def get_DRO_bytecode(self, epoch: int):
        assert self.dr_output, "Transaction not set"
        return bytes_to_hex(self.dr_output.hash(epoch, self.wip)), bytes_to_hex(self.dr_output.to_pb_bytes(epoch, self.wip))

    def validate_data_request(self, expected_DRO_bytes: str_or_none, expected_DRO_bytes_hash: str_or_none, expected_RAD_bytes_hash: str_or_none, epoch: int) -> bool:
        try:
            DRO_bytes_hash, DRO_bytes = self.get_DRO_bytecode(epoch)
            RAD_bytes_hash, RAD_bytes = self.get_RAD_bytecode(epoch)
            assert expected_DRO_bytes == DRO_bytes
            assert expected_DRO_bytes_hash == DRO_bytes_hash
            assert expected_RAD_bytes_hash == RAD_bytes_hash
        except ProtobufEncoderError:
            return False
        return True

    def test_http_get_bytecode(self):
        transaction = {
            "transaction": {
                "DataRequest": {
                    "body": {
                        "dr_output": {
                            "data_request": {
                                "retrieve": [
                                    {
                                        "script": [0x84, 0x18, 0x77, 0x82, 0x18, 0x64, 0x65, 0x70, 0x72, 0x69, 0x63, 0x65, 0x82, 0x18, 0x57, 0x1a, 0x00, 0x0f, 0x42, 0x40, 0x18, 0x5b],
                                        "kind": "HTTP-GET",
                                        "url": 'https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT'
                                    },
                                    {
                                        "script": [0x84, 0x18, 0x77, 0x82, 0x18, 0x64, 0x6a, 0x6c, 0x61, 0x73, 0x74, 0x5f, 0x70, 0x72, 0x69, 0x63, 0x65, 0x82, 0x18, 0x57, 0x1a, 0x00, 0x0f, 0x42, 0x40, 0x18, 0x5b],
                                        "kind": "HTTP-GET",
                                        "url": 'https://api.bitfinex.com/v1/pubticker/btcusd'
                                    },
                                    {
                                        "script": [0x87, 0x18, 0x77, 0x82, 0x18, 0x66, 0x66, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x82, 0x18, 0x66, 0x68, 0x58, 0x58, 0x42, 0x54, 0x5a, 0x55, 0x53, 0x44, 0x82, 0x18, 0x61, 0x61, 0x61, 0x82, 0x16, 0x00, 0x82, 0x18, 0x57, 0x1a, 0x00, 0x0f, 0x42, 0x40, 0x18, 0x5b],
                                        "kind": "HTTP-GET",
                                        "url": 'https://api.kraken.com/0/public/Ticker?pair=BTCUSD'
                                    },
                                    {
                                        "script": [0x84, 0x18, 0x77, 0x82, 0x18, 0x64, 0x64, 0x6c, 0x61, 0x73, 0x74, 0x82, 0x18, 0x57, 0x1a, 0x00, 0x0f, 0x42, 0x40, 0x18, 0x5b],
                                        "kind": "HTTP-GET",
                                        "url": 'https://www.bitstamp.net/api/v2/ticker/btcusd'
                                    },
                                    {
                                        "script": [0x85, 0x18, 0x77, 0x82, 0x18, 0x66, 0x66, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x82, 0x18, 0x64, 0x64, 0x4c, 0x61, 0x73, 0x74, 0x82, 0x18, 0x57, 0x1a, 0x00, 0x0f, 0x42, 0x40, 0x18, 0x5b],
                                        "kind": "HTTP-GET",
                                        "url": 'https://api.bittrex.com/api/v1.1/public/getticker?market=USD-BTC'
                                    }
                                ],
                                "aggregate": {
                                    "filters": [
                                        {
                                            "op": 5,
                                            "args": [0xfa, 0x40, 0x20, 0x00, 0x00],
                                        }
                                    ],
                                    "reducer": 3,
                                },
                                "tally": {
                                    "filters": [
                                        {
                                            "op": 5,
                                            "args": [0xfa, 0x40, 0x20, 0x00, 0x00],
                                        }
                                    ],
                                    "reducer": 3,
                                },
                                "time_lock": 0,
                            },
                            "witness_reward": 1000000,
                            "witnesses": 10,
                            "commit_and_reveal_fee": 1000000,
                            "min_consensus_percentage": 70,
                            "collateral": 5000000000,
                        },
                        "inputs": [],
                        "outputs": [],
                    },  # end body
                    "signatures": [],
                }
            }
        }

        # Ensure we set the transaction for the test
        self.set_transaction(transaction)

        # Test pre WIP 0019
        # Before WIP 0019 activation, only RADType enum 0 position is valid {0: "HTTP-GET"}
        assert self.validate_data_request(
            expected_DRO_bytes="0ad1031254123a68747470733a2f2f6170692e62696e616e63652e636f6d2f6170692f76332f7469636b65722f70726963653f73796d626f6c3d425443555344541a168418778218646570726963658218571a000f4240185b124b122c68747470733a2f2f6170692e62697466696e65782e636f6d2f76312f7075627469636b65722f6274637573641a1b8418778218646a6c6173745f70726963658218571a000f4240185b1261123268747470733a2f2f6170692e6b72616b656e2e636f6d2f302f7075626c69632f5469636b65723f706169723d4254435553441a2b87187782186666726573756c7482186668585842545a55534482186161618216008218571a000f4240185b1246122d68747470733a2f2f7777772e6269747374616d702e6e65742f6170692f76322f7469636b65722f6274637573641a15841877821864646c6173748218571a000f4240185b1263124068747470733a2f2f6170692e626974747265782e636f6d2f6170692f76312e312f7075626c69632f6765747469636b65723f6d61726b65743d5553442d4254431a1f85187782186666726573756c74821864644c6173748218571a000f4240185b1a0d0a0908051205fa402000001003220d0a0908051205fa40200000100310c0843d180a20c0843d28463080e497d012",
            expected_DRO_bytes_hash="0de5c46c40dd9a97e529374a96ac30dc5d5a85bb3f721ed0c152e18b043d2cf1",
            expected_RAD_bytes_hash="38894f317460d993b00d1c821078daae405ac203583295b9402d04f73d101104",
            epoch=pre_wip0019_epoch
        )

        # Test pre WIP 0020
        # Before WIP 0020 activation the valid types were {0: "HTTP-GET",1: "RNG"}
        assert self.validate_data_request(
            expected_DRO_bytes="0ad1031254123a68747470733a2f2f6170692e62696e616e63652e636f6d2f6170692f76332f7469636b65722f70726963653f73796d626f6c3d425443555344541a168418778218646570726963658218571a000f4240185b124b122c68747470733a2f2f6170692e62697466696e65782e636f6d2f76312f7075627469636b65722f6274637573641a1b8418778218646a6c6173745f70726963658218571a000f4240185b1261123268747470733a2f2f6170692e6b72616b656e2e636f6d2f302f7075626c69632f5469636b65723f706169723d4254435553441a2b87187782186666726573756c7482186668585842545a55534482186161618216008218571a000f4240185b1246122d68747470733a2f2f7777772e6269747374616d702e6e65742f6170692f76322f7469636b65722f6274637573641a15841877821864646c6173748218571a000f4240185b1263124068747470733a2f2f6170692e626974747265782e636f6d2f6170692f76312e312f7075626c69632f6765747469636b65723f6d61726b65743d5553442d4254431a1f85187782186666726573756c74821864644c6173748218571a000f4240185b1a0d0a0908051205fa402000001003220d0a0908051205fa40200000100310c0843d180a20c0843d28463080e497d012",
            expected_DRO_bytes_hash="0de5c46c40dd9a97e529374a96ac30dc5d5a85bb3f721ed0c152e18b043d2cf1",
            expected_RAD_bytes_hash="38894f317460d993b00d1c821078daae405ac203583295b9402d04f73d101104",
            epoch=pre_wip0020_epoch
        )

        # Test post WIP 0020
        # After WIP 0020 activation the valid types are {0: "Unknown", 1: "HTTP-GET", 2: "RNG", 3: "HTTP-POST" }
        assert self.validate_data_request(
            expected_DRO_bytes="0adb0312560801123a68747470733a2f2f6170692e62696e616e63652e636f6d2f6170692f76332f7469636b65722f70726963653f73796d626f6c3d425443555344541a168418778218646570726963658218571a000f4240185b124d0801122c68747470733a2f2f6170692e62697466696e65782e636f6d2f76312f7075627469636b65722f6274637573641a1b8418778218646a6c6173745f70726963658218571a000f4240185b12630801123268747470733a2f2f6170692e6b72616b656e2e636f6d2f302f7075626c69632f5469636b65723f706169723d4254435553441a2b87187782186666726573756c7482186668585842545a55534482186161618216008218571a000f4240185b12480801122d68747470733a2f2f7777772e6269747374616d702e6e65742f6170692f76322f7469636b65722f6274637573641a15841877821864646c6173748218571a000f4240185b12650801124068747470733a2f2f6170692e626974747265782e636f6d2f6170692f76312e312f7075626c69632f6765747469636b65723f6d61726b65743d5553442d4254431a1f85187782186666726573756c74821864644c6173748218571a000f4240185b1a0d0a0908051205fa402000001003220d0a0908051205fa40200000100310c0843d180a20c0843d28463080e497d012",
            expected_DRO_bytes_hash="53be15928e684d456f8b7973a684834385a14d539c92b5e468029e68c54ab32a",
            expected_RAD_bytes_hash="4dedccc82c58ae9daa0e1d79f35e494e38ffaa7a56b0728932850c9209bbb901",
            epoch=post_wip0020_epoch
        )

        print("Protobuf data request encoding test was successful!")
        # clear the transaction
        self.set_transaction(None)

    def test_rng_bytecode(self):
        transaction = {
            "transaction": {
                "DataRequest": {
                    "body": {
                        "dr_output": {
                            "data_request": {
                                "retrieve": [
                                    {
                                        "script": [0x80],
                                        "kind": "RNG",
                                    },
                                ],
                                "aggregate": {
                                    "filters": [],
                                    "reducer": 2,
                                },
                                "tally": {
                                    "filters": [],
                                    "reducer": 11,
                                },
                                "time_lock": 0,
                            },
                            "witness_reward": 500000,
                            "witnesses": 2,
                            "commit_and_reveal_fee": 250000,
                            "min_consensus_percentage": 51,
                            "collateral": 1000000000,
                        },
                        "inputs": [],
                        "outputs": [],
                    },  # end body
                    "signatures": [],
                }
            }
        }

        # Ensure we set the transaction for the test
        self.set_transaction(transaction)

        # Test pre WIP 0019
        # Before WIP 0019 activation, only RADType enum 0 position is valid {0: "HTTP-GET"} so this should return False
        assert self.validate_data_request(
            expected_DRO_bytes='0a0f120508011a01801a0210022202100b10a0c21e18022090a10f2833308094ebdc03',
            expected_DRO_bytes_hash='8ffefe7c5104479e73fdc8efba0bf137f73a3807c3b83515a9305df9d52537df',
            expected_RAD_bytes_hash='d2abe8431d6220b0d313a65fad8aa92403e4eb436d86fb5964a7ae9c18e0c37c',
            epoch=pre_wip0019_epoch,
        ) is False

        # Test post WIP 0019
        # Before WIP 0020 activation the valid types were {0: "HTTP-GET",1: "RNG"}
        assert self.validate_data_request(
            expected_DRO_bytes='0a0f120508011a01801a0210022202100b10a0c21e18022090a10f2833308094ebdc03',
            expected_DRO_bytes_hash='8ffefe7c5104479e73fdc8efba0bf137f73a3807c3b83515a9305df9d52537df',
            expected_RAD_bytes_hash='d2abe8431d6220b0d313a65fad8aa92403e4eb436d86fb5964a7ae9c18e0c37c',
            epoch=pre_wip0020_epoch,
        )

        # Test post WIP 0020
        # After WIP 0020 activation the valid types are {0: "Unknown", 1: "HTTP-GET", 2: "RNG", 3: "HTTP-POST" }
        assert self.validate_data_request(
            expected_DRO_bytes='0a0f120508021a01801a0210022202100b10a0c21e18022090a10f2833308094ebdc03',
            expected_DRO_bytes_hash='0dd4be45fe46949658d276b2a9f8550f72c3352692cdcd718d16b87924fbc113',
            expected_RAD_bytes_hash='65d6d4ee499b0bd13f0a96355ae30ede913555094a265b75305e904de1afbf3c',
            epoch=post_wip0020_epoch,
        )

        print("Protobuf RNG data request encoding test was successful!")
        # clear the transaction
        self.set_transaction(None)

    def test_http_post_bytecode(self):
        transaction = {
            "transaction": {
                "DataRequest": {
                    "body": {
                        "dr_output": {
                            "collateral": 5000000000,
                            "commit_and_reveal_fee": 1000000,
                            "data_request": {
                                "aggregate": {
                                    "filters": [
                                        {
                                            "op": 5,
                                            "args": [0xfa, 0x3f, 0xc0, 0x00, 0x00]
                                        }
                                    ],
                                    "reducer": 3
                                },
                                "retrieve": [
                                    {
                                        "kind": 'HTTP-POST',
                                        "script": [0x86, 0x18, 0x77, 0x82, 0x18, 0x66, 0x64, 0x64, 0x61, 0x74, 0x61, 0x82, 0x18, 0x66, 0x64, 0x70, 0x61, 0x69, 0x72, 0x82, 0x18, 0x64, 0x6b, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x30, 0x50, 0x72, 0x69, 0x63, 0x65, 0x82, 0x18, 0x57, 0x1a, 0x00, 0x0f, 0x42, 0x40, 0x18, 0x5b],
                                        "url": "https://api.thegraph.com/subgraphs/name/sushiswap/matic-exchange",
                                        "body": [0x7b, 0x22, 0x71, 0x75, 0x65, 0x72, 0x79, 0x22, 0x3a, 0x22, 0x7b, 0x70, 0x61, 0x69, 0x72, 0x28, 0x69, 0x64, 0x3a, 0x5c, 0x22, 0x30, 0x78, 0x31, 0x30, 0x32, 0x64, 0x33, 0x39, 0x62, 0x63, 0x32, 0x39, 0x33, 0x34, 0x37, 0x32, 0x64, 0x63, 0x39, 0x61, 0x63, 0x33, 0x65, 0x36, 0x61, 0x30, 0x61, 0x39, 0x32, 0x36, 0x31, 0x61, 0x38, 0x33, 0x38, 0x62, 0x33, 0x62, 0x63, 0x36, 0x64, 0x37, 0x5c, 0x22, 0x29, 0x7b, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x30, 0x50, 0x72, 0x69, 0x63, 0x65, 0x7d, 0x7d, 0x22, 0x7d],
                                        "headers": [
                                            ["Content-Type", "application/json"]
                                        ]
                                    }
                                ],
                                "tally": {
                                    "filters": [
                                        {
                                            "op": 5,
                                            "args": [0xfa, 0x40, 0x20, 0x00, 0x00]
                                        }
                                    ],
                                    "reducer": 3
                                },
                                "time_lock": 0
                            },
                            "min_consensus_percentage": 51,
                            "witness_reward": 1000000,
                            "witnesses": 10
                        },
                        "inputs": [],
                        "outputs": [],
                    },
                    "signatures": [],
                }
            }
        }

        # Ensure we set the transaction for the test
        self.set_transaction(transaction)

        # since HTTP-POST was the last to be added we use a single example to validate or invalidate a request.
        expected_DRO_bytes = '0a890212e8010803124068747470733a2f2f6170692e74686567726170682e636f6d2f7375626772617068732f6e616d652f7375736869737761702f6d617469632d65786368616e67651a2c861877821866646461746182186664706169728218646b746f6b656e3050726963658218571a000f4240185b22527b227175657279223a227b706169722869643a5c223078313032643339626332393334373264633961633365366130613932363161383338623362633664375c22297b746f6b656e3050726963657d7d227d2a200a0c436f6e74656e742d5479706512106170706c69636174696f6e2f6a736f6e1a0d0a0908051205fa3fc000001003220d0a0908051205fa40200000100310c0843d180a20c0843d28333080e497d012'
        expected_DRO_bytes_hash = 'caf2d1b4a778aecc324b3fd19cf2dc765bc58c640df4491557771bc813bea44d'
        expected_RAD_bytes_hash = '6c97cf998a1c8bfcf0e3d519fe2f082f45524b0f768478488b6fa16b32353192'

        # Test pre WIP 0019
        # Before WIP 0019 activation, only RADType enum 0 position is valid {0: "HTTP-GET"} so this should return False
        assert self.validate_data_request(
            expected_DRO_bytes=expected_DRO_bytes,
            expected_DRO_bytes_hash=expected_DRO_bytes_hash,
            expected_RAD_bytes_hash=expected_RAD_bytes_hash,
            epoch=pre_wip0019_epoch,
        ) is False

        # Test post WIP 0019
        # Before WIP 0020 activation the valid types were {0: "HTTP-GET", 1: "RNG"} so this should return False
        assert self.validate_data_request(
            expected_DRO_bytes=expected_DRO_bytes,
            expected_DRO_bytes_hash=expected_DRO_bytes_hash,
            expected_RAD_bytes_hash=expected_RAD_bytes_hash,
            epoch=pre_wip0020_epoch,
        ) is False

        # Test post WIP 0020
        # After WIP 0020 activation the valid types are {0: "Unknown", 1: "HTTP-GET", 2: "RNG", 3: "HTTP-POST" }
        assert self.validate_data_request(
            expected_DRO_bytes=expected_DRO_bytes,
            expected_DRO_bytes_hash=expected_DRO_bytes_hash,
            expected_RAD_bytes_hash=expected_RAD_bytes_hash,
            epoch=post_wip0020_epoch,
        )
        print("Protobuf HTTP-POST data request encoding test was successful!")
        # clear the transaction
        self.set_transaction(None)


if __name__ == "__main__":

    # wip activation epochs used for testing
    pre_wip0019_epoch = 683540
    pre_wip0020_epoch = 1059860
    post_wip0020_epoch = 1059861

    pe = ProtobufEncoder(wip=WIP(mockup=True))

    # run tests for each RADType
    pe.test_http_get_bytecode()
    pe.test_rng_bytecode()
    pe.test_http_post_bytecode()
