def bytes2bit(bytes_lst):
    return "".join([bin(byte)[2:].zfill(8) for byte in bytes_lst])

def bytes2hex(bytes_lst):
    return "".join([hex(byte)[2:].zfill(2) for byte in bytes_lst])

def hex2bytes(hex_str):
    return [int(str(hex_str[i : i + 2]), 16) for i in range(0, len(hex_str), 2)]
