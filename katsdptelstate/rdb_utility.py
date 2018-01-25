import struct

def encode_len(length):
    """Encodes the specified length as 1,2 or 5 bytes of
       RDB specific length encoded byte.
       For values less than 64 (i.e two MSBs zero - encode directly in the byte)
       For values less than 16384 use two bytes, leading MSBs are 01 followed by 14 bits encoding the value
       For values less than (2^32 -1) use 5 bytes, leading MSBs are 10. Length encoded only in the lowest 32 bits.
    """
    if length > (2**32 -1): raise ValueError("Cannot encode item of length greater than 2^32 -1")
    if length < 64: return struct.pack('B', length)
    if length < 16384: return struct.pack(">h",0x4000 + length)
    return struct.pack('>q',0x8000000000 + length)[3:]

def encode_prev_length(length):
    """Special helper for zset previous entry lengths.
       If length < 253 then use 1 byte directly, otherwise
       set first byte to 254 and add 4 trailing bytes as an
       unsigned integer.
    """
    if length < 254: return struct.pack('B', length)
    return b'\xfe' + struct.pack(">q", length)

