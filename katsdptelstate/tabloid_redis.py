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

import logging
import struct

from fakeredis import FakeStrictRedis

from .rdb_utility import encode_len, encode_prev_length


DUMP_POSTFIX = b"\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00"


class TabloidRedis(FakeStrictRedis):
    """A Redis-like class that provides a very superficial
    simulacrum of a real Redis server. Designed specifically to
    support the read cases in use by katsdptelstate.

    The Redis-like functionality is almost entirely derived from FakeStrictRedis,
    we only add a dump function.
    """
    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        super(TabloidRedis, self).__init__(**kwargs)

    def dump(self, key):
        """Encode Redis key value in an RDB compatible format.
           Note: This follows the DUMP command in Redis itself which produces output
           that is similarly encoded to an RDB, but not exactly the same.

           String types are encoded simply with a length specified (as documented in encode_len) followed directly by the
           value bytestring.

           Zset types are more complex and Redis uses both LZF and Ziplist format depending on various arcane heuristics.
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

           Returns None is key not found
        """

        type_specifier = b'\x00'
        key_type = self.type(key)
        if key_type == b'none':
            return None
        if key_type == b'zset':
            type_specifier = b'\x0c'
            data = self.zrange(key, 0, -1, withscores=True)
            entry_count = 2 * len(data)

            # The entries counter for ziplists is encoded as 2 bytes, if we exceed this limit
            # we fall back to making a simple set. Redis of course makes the decision point using
            # only 7 out of the 16 bits available and switches at 127 entries...
            if entry_count > 127:
                enc = [b'\x03' + encode_len(len(data))]
                 # for inscrutable reasons the length here is half the number of actual entries (i.e. scores are ignored)
                for entry in data:
                    enc.append(encode_len(len(entry[0])) + entry[0] + '\x010')
                     # interleave entries and scores directly
                     # for now, scores are kept fixed at integer zero since float
                     # encoding is breaking for some reason and is not needed for telstate
                enc.append(DUMP_POSTFIX)
                return b"".join(enc)

            raw_entries = []
            previous_length = b'\x00'
            # loop through each entry in the data interleaving encoded values and scores (all set to zero)
            for entry in data:
                enc = previous_length + encode_len(len(entry[0])) + entry[0]
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
        else:
            type_specifier = b'\x00'
            val = self.get(key)
            encoded_length = encode_len(len(val))
            return type_specifier + encoded_length + val + DUMP_POSTFIX
        raise NotImplementedError("Unsupported key type {}. Must be either string or zset".format(key_type))
