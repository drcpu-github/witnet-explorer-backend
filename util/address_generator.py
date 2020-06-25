import hashlib

from util.data_transformer import bytes2hex

class AddressGenerator(object):
    def __init__(self, hrp):
        self.hrp = hrp

    def bech32_polymod(self, values):
        GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3]
        chk = 1
        for v in values:
            b = (chk >> 25)
            chk = (chk & 0x1ffffff) << 5 ^ v
            for i in range(5):
                chk ^= GEN[i] if ((b >> i) & 1) else 0
        return chk

    def bech32_hrp_expand(self):
        return [ord(h) >> 5 for h in self.hrp] + [0] + [ord(h) & 31 for h in self.hrp]

    def bech32_create_checksum(self, data):
        values = self.bech32_hrp_expand() + data
        polymod = self.bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ 1
        return [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]

    def public_key_to_address(self, public_key):
        h1 = hashlib.sha256(bytearray.fromhex(public_key)).digest()[:20]

        h2 = "".join([bin(nibble)[2:].zfill(8) for nibble in h1])
        h3 = [int(h2[i: i + 5], 2) for i in range(0, len(h2), 5)]

        checksum = self.bech32_create_checksum(h3)

        h4 = h3 + checksum

        BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
        address = self.hrp + "1" + "".join([BECH32_CHARSET[i] for i in h4])

        return address

    def signature_to_address(self, compressed, public_key_bytes):
        h1 = hashlib.sha256(bytearray.fromhex(bytes2hex([compressed]) + bytes2hex(public_key_bytes))).digest()[:20]

        h2 = "".join([bin(nibble)[2:].zfill(8) for nibble in h1])
        h3 = [int(h2[i: i + 5], 2) for i in range(0, len(h2), 5)]

        checksum = self.bech32_create_checksum(h3)

        h4 = h3 + checksum

        BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"
        address = self.hrp + "1" + "".join([BECH32_CHARSET[i] for i in h4])

        return address
