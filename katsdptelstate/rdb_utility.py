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

"""Implements encoding to the RDB file format.

Some documentation is included here, but for a more complete reference, see
https://github.com/sripathikrishnan/redis-rdb-tools/wiki/Redis-RDB-Dump-File-Format
"""

import struct
import itertools
from typing import Mapping, Sequence, Iterable, Union


# Version 6 (first two bytes), and a zero checksum
DUMP_POSTFIX = b"\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00"


def encode_len(length: int) -> bytes:
    """Encodes the specified length as 1,2 or 5 bytes of
    RDB specific length encoded byte.

    - For values less than 64 (i.e two MSBs zero - encode directly in the byte)

    - For values less than 16384 use two bytes, leading MSBs are 01 followed by
      14 bits encoding the value

    - For values less than (2^32 - 1) use 5 bytes, leading MSBs are 10. Length
      encoded only in the lowest 32 bits.
    """
    if length > (2**32 - 1):
        raise ValueError("Cannot encode item of length {} as it is greater than 2^32 -1"
                         .format(length))
    if length < 64:
        return struct.pack('B', length)
    if length < 16384:
        return struct.pack(">H", 0x4000 + length)
    return struct.pack('>Q', 0x8000000000 + length)[3:]


def encode_prev_length(length: int) -> bytes:
    """Special helper for zset previous entry lengths.

    If length < 253 then use 1 byte directly, otherwise
    set first byte to 254 and add 4 trailing bytes as an
    unsigned integer.
    """
    if length < 254:
        return struct.pack('B', length)
    return b'\xfe' + struct.pack("<I", length)


def dump_string(data: bytes) -> bytes:
    """Encode a binary string as per redis DUMP command"""
    type_specifier = b'\x00'
    encoded_length = encode_len(len(data))
    return type_specifier + encoded_length + data + DUMP_POSTFIX


def encode_ziplist(entries: Iterable[Union[bytes, int]]) -> bytes:
    """Create an RDB ziplist, including the envelope.

    The entries can either be byte strings or integers, and any iterable can
    be used.

    The ziplist string envelope itself is an RDB-encoded string with the
    following form:
    (format descriptions are provided in the description for each component)

        (bytes in list '<i')(offset to tail '<i')
        (number of entries '<h')(entry 1)(entry 2)(entry N)(terminator 0xFF)

    The entries themselves are encoded as follows:

        (previous entry length 1byte or 5bytes)
        (entry length - up to 5 bytes as per length encoding)(value)
    """
    def append(raw):
        nonlocal zl_len
        raw_entries.append(raw)
        zl_len += len(raw)

    raw_entries = [b'']   # Later replaced by a header
    zl_len = 10           # Header is 10 bytes
    zl_entries = 0
    zl_last = zl_len      # Offset to previous entry
    for entry in entries:
        previous_length = zl_len - zl_last
        zl_last = zl_len
        zl_entries += 1
        append(encode_prev_length(previous_length))
        if isinstance(entry, int):
            # Special encoding for integers. At present only the value 0 is
            # used, so we only implement the 4-bit encoding, and otherwise
            # fall back to string form.
            if 0 <= entry <= 12:
                append(struct.pack('<B', 0xf1 + entry))
                continue
            else:
                entry = b'%d' % entry
        assert isinstance(entry, bytes)
        append(encode_len(len(entry)))
        append(entry)
    append(b'\xff')
    # Fill in the header
    raw_entries[0] = (struct.pack('<I', zl_len) + struct.pack('<I', zl_last)
                      + struct.pack('<H', zl_entries))
    return b''.join(raw_entries)


def dump_iterable(prefix: bytes, suffix: bytes, entries: Iterable[bytes]) -> bytes:
    """Output a sequence of strings, bracketed by a prefix and suffix.

    This is an internal function used to implement :func:`dump_zset` and
    :func:`dump_hash`.
    """
    enc = [prefix]
    for entry in entries:
        enc.extend([encode_len(len(entry)), entry])
    enc.append(suffix)
    return b''.join(enc)


def dump_zset(data: Sequence[bytes]) -> bytes:
    """Encode a set of values as a redis zset, as per redis DUMP command.

    All scores are assumed to be zero and encoded in the most efficient way
    possible.

    Redis uses both LZF and Ziplist format depending on various arcane heuristics.
    For maximum compatibility we use Ziplist for small lists.

    There are two possible ways to encode the zset:

      1. Using a ziplist: (0c)(ziplist) where (ziplist) is string-encoded.
      2. Using a simple list (03)(num entries)(entry)(entry)...
    """
    entry_count = 2 * len(data)

    # The entries counter for ziplists is encoded as 2 bytes, if we exceed this limit
    # we fall back to making a simple set. Redis of course makes the decision point using
    # only 7 out of the 16 bits available and switches at 127 entries...
    # Additionally, ziplists can't be used if the total size is too large to encode. For
    # safety, we switch at 2GB rather than risking off-by-one errors.
    if entry_count > 127 or sum(len(entry) for entry in data) >= 2**31:
        # for inscrutable reasons the length here is half the number of actual
        # entries (i.e. scores are ignored)
        return dump_iterable(
            b'\x03' + encode_len(len(data)),
            DUMP_POSTFIX,
            # interleave entries and scores directly
            # for now, scores are kept fixed at integer zero since float
            # encoding is breaking for some reason and is not needed for telstate
            itertools.chain.from_iterable((entry, b'0') for entry in data)
        )
    else:
        zl_envelope = encode_ziplist(itertools.chain.from_iterable((x, 0) for x in data))
        return b'\x0c' + encode_len(len(zl_envelope)) + zl_envelope + DUMP_POSTFIX


def dump_hash(data: Mapping[bytes, bytes]) -> bytes:
    """Encode a dictionary as a redis hash, as per redis DUMP command.

    The encoding is similar to zsets, but using the value instead of the score.
    """
    entry_count = 2 * len(data)

    if entry_count > 512 or sum(len(entry[0]) + len(entry[1]) for entry in data.items()) >= 2**31:
        return dump_iterable(
            b'\x04' + encode_len(len(data)),
            DUMP_POSTFIX,
            itertools.chain.from_iterable(data.items())
        )
    else:
        zl_envelope = encode_ziplist(itertools.chain.from_iterable(data.items()))
        return b'\x0d' + encode_len(len(zl_envelope)) + zl_envelope + DUMP_POSTFIX
