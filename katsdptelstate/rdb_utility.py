################################################################################
# Copyright (c) 2018-2019, National Research Foundation (Square Kilometre Array)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import struct


# Version 6 (first two bytes), and a zero checksum
DUMP_POSTFIX = b"\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00"


def encode_len(length):
    """Encodes the specified length as 1,2 or 5 bytes of
       RDB specific length encoded byte.
       For values less than 64 (i.e two MSBs zero - encode directly in the byte)
       For values less than 16384 use two bytes, leading MSBs are 01 followed by 14 bits encoding the value
       For values less than (2^32 -1) use 5 bytes, leading MSBs are 10. Length encoded only in the lowest 32 bits.
    """
    if length > (2**32 - 1):
        raise ValueError("Cannot encode item of length {} as it is greater than 2^32 -1".format(length))
    if length < 64:
        return struct.pack('B', length)
    if length < 16384:
        return struct.pack(">h", 0x4000 + length)
    return struct.pack('>q', 0x8000000000 + length)[3:]


def encode_prev_length(length):
    """Special helper for zset previous entry lengths.
       If length < 253 then use 1 byte directly, otherwise
       set first byte to 254 and add 4 trailing bytes as an
       unsigned integer.
    """
    if length < 254:
        return struct.pack('B', length)
    return b'\xfe' + struct.pack(">q", length)


def dump_string(data):
    """Encode a binary string as per redis DUMP command"""
    type_specifier = b'\x00'
    encoded_length = encode_len(len(data))
    return type_specifier + encoded_length + data + DUMP_POSTFIX


def dump_zset(data):
    """Encode a set of values as a redis zset, as per redis DUMP command
       All scores are assumed to be zero.

       Redis uses both LZF and Ziplist format depending on various arcane heuristics.
       For maximum compatibility we use LZF and Ziplist as this supports really large values well, at the expense of
       additional overhead for small values.

       A zset dump thus takes the following form:

            (length encoding)(ziplist string envelope)

       The ziplist string envelope itself is an LZF compressed string with the following form:
       (format descriptions are provided in the description for each component)

            (bytes in list '<i')(offset to tail '<i')(number of entries '<h')(entry 1)(entry 2)(entry 2N)(terminator 0xFF)

       The entries themselves are encoded as follows:

            (previous entry length 1byte or 5bytes)(entry length - up to 5 bytes as per length encoding)(value)

       The entries alternate between zset values and scores, so there should always be 2N entries in the decode.
    """
    entry_count = 2 * len(data)

    # The entries counter for ziplists is encoded as 2 bytes, if we exceed this limit
    # we fall back to making a simple set. Redis of course makes the decision point using
    # only 7 out of the 16 bits available and switches at 127 entries...
    if entry_count > 127:
        enc = [b'\x03' + encode_len(len(data))]
         # for inscrutable reasons the length here is half the number of actual entries (i.e. scores are ignored)
        for entry in data:
            enc.append(encode_len(len(entry)) + entry + b'\x010')
             # interleave entries and scores directly
             # for now, scores are kept fixed at integer zero since float
             # encoding is breaking for some reason and is not needed for telstate
        enc.append(DUMP_POSTFIX)
        return b"".join(enc)

    type_specifier = b'\x0c'
    raw_entries = []
    previous_length = b'\x00'
    # loop through each entry in the data interleaving encoded values and scores (all set to zero)
    for entry in data:
        enc = previous_length + encode_len(len(entry)) + entry
        raw_entries.append(enc)
        previous_length = encode_prev_length(len(enc))
        raw_entries.append(previous_length + b'\xf1')
         # scores are encoded using a special length schema which supports direct integer addressing
         # 4 MSB set implies direct unsigned integer in 4 LSB (minus 1). Hence \xf1 is integer 0
        previous_length = b'\x02'
    encoded_entries = b"".join(raw_entries) + b'\xff'
    zl_length = 10 + len(encoded_entries)
     # account for the known 10 bytes worth of length descriptors when calculating envelope length
    zl_envelope = struct.pack('<i', zl_length) + struct.pack('<i', zl_length - 3) + struct.pack('<h', entry_count) + encoded_entries
    return b'\x0c' + encode_len(len(zl_envelope)) + zl_envelope + DUMP_POSTFIX
