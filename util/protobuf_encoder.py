#####################################################################################
# Data request protobuf encoding based on https://github.com/parodyBit/witnet_tools #
#####################################################################################

import hashlib
from dataclasses import dataclass
from enum import Enum

from typing import Union, List

sha256 = lambda x: hashlib.sha256(x).digest()

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
    else:
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

@dataclass
class RADType(Enum):
    Unknown = 0
    HttpGet = 1
    Rng = 2

    @classmethod
    def from_json(cls, data):
        if data == 'Unknown':
            return RADType.Unknown
        elif data == 'HTTP-GET':
            return RADType.HttpGet
        elif data == 'RNG':
            return RADType.Rng
        else:
            assert False, "Unknown RADType"

@dataclass
class RADRetrieve:
    kind: RADType
    url: str
    script: bytes

    @classmethod
    def from_json(cls, data):
        return RADRetrieve(kind=RADType.from_json(data['kind']), url=data['url'], script=bytes(data['script']))

    def to_pb_bytes(self):
        kind_bytes = pb_field(field_number=1, tag=VARINT, value=self.kind.value)
        url_bytes = pb_field(field_number=2, tag=LENGTH_DELIMITED, value=str_to_bytes(self.url))
        script_bytes = pb_field(field_number=3, tag=LENGTH_DELIMITED, value=self.script)
        if self.kind.value == 0:
            return concat([url_bytes, script_bytes])
        else:
            return concat([kind_bytes, url_bytes, script_bytes])

@dataclass
class RADFilter:
    op: int
    args: bytes

    @classmethod
    def from_json(cls, data):
        return RADFilter(op=data['op'], args=data['args'])

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

    def to_pb_bytes(self):
        if len(self.filters) > 0:
            filter_bytes = pb_field(field_number=1, tag=LENGTH_DELIMITED, value=concat([x.to_pb_bytes() for x in self.filters]))
        else:
            filter_bytes = []
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

    def to_pb_bytes(self):
        if len(self.filters) > 0:
            filter_bytes = pb_field(field_number=1, tag=LENGTH_DELIMITED, value=concat([x.to_pb_bytes() for x in self.filters]))
        else:
            filter_bytes = []
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

    def to_pb_bytes(self):
        if self.time_lock > 0:
            timelock_bytes = pb_field(field_number=1, tag=VARINT, value=self.time_lock)
        else:
            timelock_bytes = None
        retrieve_bytes = concat([pb_field(field_number=2, tag=LENGTH_DELIMITED, value=x.to_pb_bytes()) for x in self.retrieve])
        aggregate_bytes = pb_field(field_number=3, tag=LENGTH_DELIMITED, value=self.aggregate.to_pb_bytes())
        tally_bytes = pb_field(field_number=4, tag=LENGTH_DELIMITED, value=self.tally.to_pb_bytes())
        if timelock_bytes:
            return concat([timelock_bytes, retrieve_bytes, aggregate_bytes, tally_bytes])
        else:
            return concat([retrieve_bytes, aggregate_bytes, tally_bytes])

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
        return DataRequestOutput(
            collateral=data["collateral"],
            commit_and_reveal_fee=data["commit_and_reveal_fee"],
            data_request=RADRequest.from_json(data=data["data_request"]),
            min_consensus_percentage=data["min_consensus_percentage"],
            witness_reward=data["witness_reward"],
            witnesses=data["witnesses"],
        )

    def to_pb_bytes(self):
        return concat([
            pb_field(field_number=1, tag=LENGTH_DELIMITED, value=self.data_request.to_pb_bytes()),
            pb_field(field_number=2, tag=VARINT, value=self.witness_reward),
            pb_field(field_number=3, tag=VARINT, value=self.witnesses),
            pb_field(field_number=4, tag=VARINT, value=self.commit_and_reveal_fee),
            pb_field(field_number=5, tag=VARINT, value=self.min_consensus_percentage),
            pb_field(field_number=6, tag=VARINT, value=self.collateral),
        ])

class ProtobufEncoder(object):
    def __init__(self):
        self.transaction = None

    def set_transaction(self, transaction):
        self.transaction = transaction
        if "transaction" in self.transaction:
            self.transaction = transaction["transaction"]

    def build_RAD_request_json(self, RAD_request):
        return {
            "retrieve": [
                {
                    "script": retrieve["script"],
                    "kind": retrieve["kind"],
                    "url": retrieve["url"] if "url" in retrieve else "",
                } for retrieve in RAD_request["retrieve"]
            ],
            "aggregate": {
                "filters": [
                    {
                        "op": filters["op"],
                        "args": filters["args"],
                    } for filters in RAD_request["aggregate"]["filters"]
                ],
                "reducer": RAD_request["aggregate"]["reducer"],
            },
            "tally": {
                "filters": [
                    {
                        "op": filters["op"],
                        "args": filters["args"],
                    } for filters in RAD_request["tally"]["filters"]
                ],
                "reducer": RAD_request["tally"]["reducer"],
            },
            "time_lock": RAD_request["time_lock"],
        }

    def get_bytecode_RAD_request(self):
        assert self.transaction, "Transaction not set"
        if "DataRequest" in self.transaction:
            self.transaction = self.transaction["DataRequest"]
        RAD_request_json = self.build_RAD_request_json(self.transaction["body"]["dr_output"]["data_request"])
        RAD_request_bytes = RADRequest.from_json(data=RAD_request_json).to_pb_bytes()
        return self.hash_protobuf_bytes(RAD_request_bytes), bytes_to_hex(RAD_request_bytes)

    def build_data_request_json(self, data_request):
        return {
            "data_request": self.build_RAD_request_json(data_request["data_request"]),
            "witness_reward": data_request["witness_reward"],
            "witnesses": data_request["witnesses"],
            "commit_and_reveal_fee": data_request["commit_and_reveal_fee"],
            "min_consensus_percentage": data_request["min_consensus_percentage"],
            "collateral": data_request["collateral"],
        }

    def get_bytecode_data_request(self):
        assert self.transaction, "Transaction not set"
        if "DataRequest" in self.transaction:
            self.transaction = self.transaction["DataRequest"]
        data_request_json = self.build_data_request_json(self.transaction["body"]["dr_output"])
        data_request_bytes = DataRequestOutput.from_json(data=data_request_json).to_pb_bytes()
        return self.hash_protobuf_bytes(data_request_bytes), bytes_to_hex(data_request_bytes)

    def hash_protobuf_bytes(self, pb_bytes):
        return bytes_to_hex(sha256(pb_bytes))

    def test_data_request_bytecode(self):
        data_request = {
            "dr_output": {
                "data_request":{
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
            }
        }

        data_request_output = DataRequestOutput.from_json(data=data_request["dr_output"])
        data_request_output_bytes = data_request_output.to_pb_bytes()

        assert bytes_to_hex(data_request_output_bytes) == "0adb0312560801123a68747470733a2f2f6170692e62696e616e63652e636f6d2f6170692f76332f7469636b65722f70726963653f73796d626f6c3d425443555344541a168418778218646570726963658218571a000f4240185b124d0801122c68747470733a2f2f6170692e62697466696e65782e636f6d2f76312f7075627469636b65722f6274637573641a1b8418778218646a6c6173745f70726963658218571a000f4240185b12630801123268747470733a2f2f6170692e6b72616b656e2e636f6d2f302f7075626c69632f5469636b65723f706169723d4254435553441a2b87187782186666726573756c7482186668585842545a55534482186161618216008218571a000f4240185b12480801122d68747470733a2f2f7777772e6269747374616d702e6e65742f6170692f76322f7469636b65722f6274637573641a15841877821864646c6173748218571a000f4240185b12650801124068747470733a2f2f6170692e626974747265782e636f6d2f6170692f76312e312f7075626c69632f6765747469636b65723f6d61726b65743d5553442d4254431a1f85187782186666726573756c74821864644c6173748218571a000f4240185b1a0d0a0908051205fa402000001003220d0a0908051205fa40200000100310c0843d180a20c0843d28463080e497d012", "Data request output bytes do not match"
        assert self.hash_protobuf_bytes(data_request_output_bytes) == "53be15928e684d456f8b7973a684834385a14d539c92b5e468029e68c54ab32a", "Data request output bytes hash does not match"

        print("Protobuf data request encoding test was successful!")

if __name__ == "__main__":
    pe = ProtobufEncoder()
    pe.test_data_request_bytecode()
